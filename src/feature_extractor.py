import threading
from logging import Logger
import reactivex as rx
from reactivex import operators as ops
from reactivex.subject import Subject

from src.collections import ArticleContent
from src.config import *


class FeatureExtractor:
    """
    Class for Extracting Features from articles
    """

    def __init__(self, logger: Logger, postgresService, mongoService, scheduler, processPool):
        self.completeSubject = Subject()
        self.countingLock = threading.Lock()
        self.articleCount = 0

        self.postgresService = postgresService
        self.mongoService = mongoService
        self.scheduler = scheduler
        self.logger = logger
        self.processPool = processPool

    def complete(self):
        """
        Called when extraction is complete. Logs completed articles and unblocks main thread
        """
        self.logger.info("Completed extraction for %s articles", self.articleCount)
        self.completeSubject.on_next(1)
        self.completeSubject.on_completed()

    def countAndLog(self):
        """
        Counts articles that goes through the stream. Logs based off the frequency defined in {LOG_FREQUENCY}.
        Is a synchronous function that only allows 1 thread to execute.
        """
        # Avoid race conditions by adding a lock
        with self.countingLock:
            self.articleCount += 1
            if self.articleCount % LOG_FREQUENCY == 0:
                self.logger.info("Completed extraction for %s articles", self.articleCount)

    def run(self):
        """
        Starts Feature extraction. Will block main thread until complete or program timeout.
        """
        self.buildExtractPipeline().subscribe(on_completed=lambda: self.complete())

        # Stream that blocks main thread until main stream completes
        # If main stream takes longer than {PROGRAM_TIMEOUT} this stream completes and unblocks main for shutdown
        self.completeSubject.pipe(
            ops.timeout(PROGRAM_TIMEOUT),
            ops.do_action(on_error=lambda err: self.logger.error("Error occurred during execution", exc_info=err))
        ).run()

    def buildExtractPipeline(self):
        """
        Builds observable stream for ioc extractor
        :return: Observable containing IOC Extracting pipeline
        """
        # Call Postgres to get non-extracted ids
        return self.postgresService.getNonExtractedIdsAsStream().pipe(
            # Get Article content from mongo as a stream
            ops.flat_map(lambda articleId: self.mongoService.getByIdAsStream(articleId)),
            # Extracts content
            ops.flat_map(lambda article: self.getExtractedFeatures(article)),
            # Counts article
            ops.do_action(on_next=lambda article: self.countAndLog()),
            # Error handling
            ops.do_action(on_error=lambda err: self.logger.error("Error occurred.", exc_info=err)),
            ops.catch(rx.empty()),
            # Scheduler setup for entire stream
            ops.subscribe_on(scheduler=self.scheduler),
        )

    def getExtractedFeatures(self, articleContent: ArticleContent):
        return rx.just(articleContent).pipe(
            ops.do_action(lambda article: self.processPool.submitArticle(article)),
            ops.do_action(on_error=lambda err: self.logger.error("Error occurred.", exc_info=err)),
            ops.catch(rx.empty()),
            # Scheduler setup
            ops.subscribe_on(scheduler=self.scheduler)
        )
