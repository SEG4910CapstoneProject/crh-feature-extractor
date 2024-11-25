import unittest
from logging import Logger
from unittest.mock import *
from uuid import UUID

import reactivex as rx
from reactivex import operators as ops
from reactivex.scheduler import CurrentThreadScheduler

from src.mongo_service import ArticleContent
from src.postgres_service import PostgresService
from src.collections import CategoryAssignerRule
from src.category_assigner import CategoryAssigner

UUID_1 = UUID("5d8a48c7-8799-49ba-8a61-b96c6f0d08e8")
UUID_2 = UUID("385650f8-4f1d-450e-80a5-5eb82d50ab42")
UUID_3 = UUID("74f504a2-f20c-456c-bf85-7065b6b44b2f")


def getMockObjects():
    loggerMock = Mock(spec_set=Logger)
    postgresServiceMock = Mock(spec_set=PostgresService)

    return loggerMock, postgresServiceMock

def getMockRules():
    return [
        CategoryAssignerRule(1, 'test1'),
        CategoryAssignerRule(2, 'test2')
    ]


class CategoryAssignerTests(unittest.TestCase):
    def test_extract_features_success(self):
        loggerMock, postgresServiceMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        article1 = ArticleContent(UUID_1, "content test2 1", 1)
        postgresServiceMock.getCategoryRulesAsStream.return_value = rx.of(getMockRules())
        postgresServiceMock.insertCategoryArticleAsStream.return_value = rx.of(0)

        # Actual
        categoryAssigner = CategoryAssigner(loggerMock, postgresServiceMock)
        categoryAssigner.extract_features(article1).subscribe(scheduler=scheduler)

        # Assert
        postgresServiceMock.insertCategoryArticleAsStream.assert_called_once_with('2', UUID_1)
        loggerMock.error.assert_not_called()

    def test_extract_features_call_insertCategoryArticleAsStream_exactly_once(self):
        loggerMock, postgresServiceMock = getMockObjects()
        scheduler = CurrentThreadScheduler()
        countingMock = Mock()

        article1 = ArticleContent(UUID_1, "content test2 1", 1)
        article2 = ArticleContent(UUID_2, "content test2 2", 1)
        article3 = ArticleContent(UUID_3, "content test2 3", 1)

        postgresServiceMock.getCategoryRulesAsStream.return_value = rx.of(getMockRules()).pipe(
            ops.do_action(on_next=lambda a: countingMock()) # Counts all values that comes through
        )
        postgresServiceMock.insertCategoryArticleAsStream.return_value = rx.of(0)

        # Actual
        categoryAssigner = CategoryAssigner(loggerMock, postgresServiceMock)
        categoryAssigner.extract_features(article1).subscribe(scheduler=scheduler)
        categoryAssigner.extract_features(article2).subscribe(scheduler=scheduler)
        categoryAssigner.extract_features(article3).subscribe(scheduler=scheduler)

        # Assert
        self.assertEqual(1, countingMock.call_count)
        loggerMock.error.assert_not_called()

    def test_extract_features_no_matches_success(self):
        loggerMock, postgresServiceMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        article1 = ArticleContent(UUID_1, "content 1", 1)
        postgresServiceMock.getCategoryRulesAsStream.return_value = rx.of(getMockRules())
        postgresServiceMock.insertCategoryArticleAsStream.return_value = rx.of(0)

        # Actual
        categoryAssigner = CategoryAssigner(loggerMock, postgresServiceMock)
        categoryAssigner.extract_features(article1).subscribe(scheduler=scheduler)

        # Assert
        postgresServiceMock.insertCategoryArticleAsStream.assert_not_called()
        loggerMock.error.assert_not_called()
