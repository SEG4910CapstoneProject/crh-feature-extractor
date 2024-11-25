import logging

import reactivex as rx
from multiprocess.synchronize import Lock
from reactivex import operators as ops
from multiprocess import Process, Queue, Manager
import dill
from reactivex.scheduler import ThreadPoolScheduler

from src.base_extractor import BaseExtractor
from src.category_assigner import CategoryAssigner
from src.collections import ArticleContent
from src.config import THREADS_PER_CORE, LOGGER_FORMAT
from src.exceptions import DisposedException
from src.ioc_extractor import IocExtractor
from src.postgres_service import PostgresService


class ProcessPoolTaskScheduler:
    """
    Allows to submit tasks for being processed in another process for parallel processing
    *Database calls must never be called from this task scheduler* as they are not process safe
    """

    def __init__(self, max_workers=1):
        dill.settings['recurse'] = True
        self.max_workers = max_workers
        self._disposed = False
        self._processes = []
        self._manager = Manager()
        self._taskQueue = self._manager.Queue()
        self._disposedValue = self._manager.Value(bool, False)

        startLocks = []
        for pid in range(max_workers):
            startLock: Lock = self._manager.Lock()
            startLock.acquire()
            p = Process(target=self._processRun, args=[self._taskQueue, startLock, self._disposedValue])
            p.daemon = True
            self._processes.append(p)
            startLocks.append(startLock)
            p.start()

        # Wait for all processes to start
        for lock in startLocks:
            lock.acquire()

    def dispose(self):
        """
        Releases resources for processes
        """
        self._disposedValue.value = True
        # unblock all processes
        for i in range(self.max_workers):
            self._taskQueue.put(i)

        self._taskQueue._close()
        for p in self._processes:
            # Wait 10 seconds max for shutdown
            p.join(10)

    def submitArticle(self, articleContent: ArticleContent):
        if self._disposedValue.value:
            raise DisposedException()

        completeLock: Lock = self._manager.Lock()
        # Lock is initially unlocked, lock it before submitting
        completeLock.acquire()
        self._taskQueue.put([articleContent, completeLock])
        # Wait for completion
        completeLock.acquire()

    def _processRun(self, queue, startLock, disposedValue):
        """
        code that runs when process starts
        """
        try:
            # Create required dependencies
            scheduler = ThreadPoolScheduler(THREADS_PER_CORE)
            logging.basicConfig(level=logging.INFO, format=LOGGER_FORMAT)
            logger = logging.getLogger('Process Extractor')

            try:
                postgresService = PostgresService(logging.getLogger('PostgresService'), scheduler)
            except Exception as e:
                logging.error('Failed to Initialize Databases', exc_info=e)
                return

            # Instantiate extractor services
            extractorServices: list[BaseExtractor] = [
                IocExtractor(logging.getLogger('IocExtractor'), postgresService),
                CategoryAssigner(logging.getLogger('CategoryAssigner'), postgresService)
            ]

            logger.info("Process extractor started")
            startLock.release()
            # Execute loop
            while not disposedValue.value:

                request = queue.get(block=True)
                if disposedValue.value:
                    return

                articleContent: ArticleContent = request[0]
                outLock: Lock = request[1]

                try:
                    extractFeatures(articleContent, extractorServices, postgresService, scheduler, logger)
                except Exception as err:
                    pass

                outLock.release()
        except EOFError:
            # Queue is closed, exit process
            pass
        except Exception as err:
            logging.error('Something went wrong', exc_info=err)

        postgresService.close()


def extractFeatures(articleContent: ArticleContent, extractorServices, postgresService, scheduler, logger):
    """
    Extracts features for a given article.
    :param articleContent: article to extract features
    """
    rx.from_iterable(extractorServices).pipe(
        # Extracts features
        ops.flat_map(lambda extService: extService.extract_features(articleContent)),
        ops.filter(lambda v: False),
        # Complete with emitting original article
        ops.concat(rx.of(articleContent)),
        ops.flat_map(lambda article: postgresService.markArticleAsExtractedAsStream(article.articleId)),
        # Error handling
        ops.do_action(on_error=lambda err: logger.error("Error occurred.", exc_info=err)),
        ops.catch(rx.empty()),
        # Scheduler setup
        ops.subscribe_on(scheduler=scheduler),
        # Ensures something is always returned
        ops.to_list()
    ).run()
