import logging
import multiprocessing
from reactivex.scheduler import ThreadPoolScheduler

from src.config import *
from src.postgres_service import PostgresService
from src.mongo_service import MongoService
from src.feature_extractor import FeatureExtractor
from src.process_pool_task_scheduler import ProcessPoolTaskScheduler


def main():
    # Setup logger
    logging.basicConfig(level=logging.INFO, format=LOGGER_FORMAT)
    logging.info('Starting feature extractor')

    # Create schedulers
    processesToMake = multiprocessing.cpu_count()
    threadsToMake = THREADS_PER_CORE * multiprocessing.cpu_count()
    logging.info('Starting Main Threadpool with %s threads', str(threadsToMake))
    logging.info('Starting Processpool with %s processes each with %s threads', str(processesToMake), str(THREADS_PER_CORE))
    scheduler = ThreadPoolScheduler(threadsToMake)
    taskScheduler = ProcessPoolTaskScheduler(processesToMake)

    # Instantiate Database services for feature extractor
    try:
        postgresService = PostgresService(logging.getLogger('PostgresService'), scheduler)
        mongoService = MongoService(logging.getLogger('MongoService'), scheduler)
    except Exception as e:
        logging.error('Failed to Initialize Databases', exc_info=e)
        return

    featureExtractor = FeatureExtractor(logging.getLogger('FeatureExtractor'), postgresService, mongoService, scheduler, taskScheduler)

    logging.info('Startup Completed')
    # Start Extraction
    featureExtractor.run()

    logging.info('Shutting down feature extractor')

    # Cleanup
    postgresService.close()
    taskScheduler.dispose()

    logging.info('Done')


if __name__ == "__main__":
    main()
