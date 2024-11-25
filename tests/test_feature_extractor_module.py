
import unittest
from unittest.mock import *

from logging import Logger
from uuid import UUID

from reactivex.scheduler import CurrentThreadScheduler

from src.feature_extractor import FeatureExtractor
from src.postgres_service import PostgresService
from src.mongo_service import *
from src.process_pool_task_scheduler import ProcessPoolTaskScheduler

UUID_1 = UUID("5d8a48c7-8799-49ba-8a61-b96c6f0d08e8")
UUID_2 = UUID("2c398d08-22e0-4f69-955b-69fb39666a9c")
UUID_3 = UUID("8c819db1-3dfa-4343-b6e7-9b73495fcdec")


def getMockObjects():
    loggerMock = Mock(spec_set=Logger)
    postgresServiceMock = Mock(spec_set=PostgresService)
    mongoServiceMock = Mock(spec_set=MongoService)
    processPool = Mock(spec_set=ProcessPoolTaskScheduler)

    return loggerMock, postgresServiceMock, mongoServiceMock, processPool


class FeatureExtractorTests(unittest.TestCase):

    def test_extractor_success(self):
        # assembly
        loggerMock, postgresServiceMock, mongoServiceMock, processPoolMock = getMockObjects()

        article1 = ArticleContent(UUID_1, "content 1", 1)
        article2 = ArticleContent(UUID_2, "content 2", 1)
        article3 = ArticleContent(UUID_3, "content 3", 1)

        scheduler = CurrentThreadScheduler()

        postgresServiceMock.getNonExtractedIdsAsStream.return_value = rx.of(1,2,3)
        mongoServiceMock.getByIdAsStream.side_effect = [
            rx.of(article1),
            rx.of(article2),
            rx.of(article3),
        ]
        postgresServiceMock.markArticleAsExtractedAsStream.side_effect = [
            rx.of("Ignored"),
            rx.of("Ignored"),
            rx.of("Ignored"),
        ]

        extractor = FeatureExtractor(
            loggerMock,
            postgresServiceMock,
            mongoServiceMock,
            scheduler,
            processPoolMock
        )

        # Actual
        extractor.buildExtractPipeline().subscribe(scheduler=scheduler)

        # Assert
        postgresServiceMock.getNonExtractedIdsAsStream.assert_called_once()
        self.assertEqual(3, mongoServiceMock.getByIdAsStream.call_count)
        processPoolMock.submitArticle.assert_has_calls([
            call(article1), call(article2), call(article3)
        ], any_order=True)
        loggerMock.error.assert_not_called()

    def test_extractor_error_db_read_handled(self):
        # assembly
        loggerMock, postgresServiceMock, mongoServiceMock, processPoolMock = getMockObjects()

        scheduler = CurrentThreadScheduler()

        postgresServiceMock.getNonExtractedIdsAsStream.return_value = rx.throw(Exception("Test Exception"))

        extractor = FeatureExtractor(
            loggerMock,
            postgresServiceMock,
            mongoServiceMock,
            scheduler,
            processPoolMock
        )

        # Actual
        extractor.buildExtractPipeline().subscribe(scheduler=scheduler)

        # Assert
        postgresServiceMock.getNonExtractedIdsAsStream.assert_called_once()
        loggerMock.error.assert_called()

if __name__ == '__main__':
    unittest.main()
