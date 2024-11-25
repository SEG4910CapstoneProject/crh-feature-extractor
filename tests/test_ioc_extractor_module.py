import unittest
from logging import Logger
from unittest.mock import *
from uuid import UUID

import reactivex as rx
from reactivex.scheduler import CurrentThreadScheduler
from iocsearcher.searcher import Searcher
from src.config import iocIdToIdMapping

from src.mongo_service import ArticleContent
from src.postgres_service import PostgresService
from src.ioc_extractor import IocExtractor

UUID_1 = UUID("5d8a48c7-8799-49ba-8a61-b96c6f0d08e8")

def getMockObjects():
    loggerMock = Mock(spec_set=Logger)
    postgresServiceMock = Mock(spec_set=PostgresService)

    return loggerMock, postgresServiceMock


def getPatches(config):
    searcherPatch = patch("src.ioc_extractor.IocSearcher", spec=Searcher, **config)

    return searcherPatch


class IocExtractorTests(unittest.TestCase):
    def test_extract_features_success(self):
        loggerMock, postgresServiceMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        article1 = ArticleContent(UUID_1, "content 1", 1)
        iocType = "ip4"
        iocValue = "1.1.1.1"
        iocId = 1

        searcherPatch = getPatches({"search_raw.return_value": [(iocType, iocValue, 0, iocValue)]})
        postgresServiceMock.addIOCIfNotExistAsStream.return_value = rx.of(iocId)
        postgresServiceMock.addArticleIocAsStream.return_value = rx.of((iocId, UUID_1))
        postgresServiceMock.getGlobalFiltersAsDictAsStream.return_value = rx.of(dict())
        postgresServiceMock.getSourceFiltersAsDictAsStream.return_value = rx.of(dict())

        searcherPatch.start()

        # Actual
        iocExtractor = IocExtractor(loggerMock, postgresServiceMock)
        iocExtractor.extract_features(article1).subscribe(scheduler=scheduler)

        # Assert
        postgresServiceMock.addIOCIfNotExistAsStream.assert_called_once_with(iocValue, iocIdToIdMapping[iocType])
        postgresServiceMock.addArticleIocAsStream.assert_called_once_with(iocId, UUID_1)
        loggerMock.error.assert_not_called()

        searcherPatch.stop()

    def test_extract_features_remove_duplicates_success(self):
        loggerMock, postgresServiceMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        article1 = ArticleContent(UUID_1, "content 1", 1)
        iocType = "ip4"
        iocValue = "1.1.1.1"
        iocId = 1

        searcherPatch = getPatches({"search_raw.return_value": [(iocType, iocValue, 0, iocValue),
                                                                (iocType, iocValue, 2, iocValue),
                                                                (iocType, iocValue, 3, iocValue)]})
        postgresServiceMock.addIOCIfNotExistAsStream.return_value = rx.of(iocId)
        postgresServiceMock.addArticleIocAsStream.return_value = rx.of((iocId, UUID_1))
        postgresServiceMock.getGlobalFiltersAsDictAsStream.return_value = rx.of(dict())
        postgresServiceMock.getSourceFiltersAsDictAsStream.return_value = rx.of(dict())

        searcherPatch.start()

        # Actual
        iocExtractor = IocExtractor(loggerMock, postgresServiceMock)
        iocExtractor.extract_features(article1).subscribe(scheduler=scheduler)

        # Assert
        postgresServiceMock.addIOCIfNotExistAsStream.assert_called_once_with(iocValue, iocIdToIdMapping[iocType])
        postgresServiceMock.addArticleIocAsStream.assert_called_once_with(iocId, UUID_1)
        loggerMock.error.assert_not_called()

        searcherPatch.stop()

    def test_extract_features_error_db_complete(self):
        loggerMock, postgresServiceMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        article1 = ArticleContent(UUID_1, "content 1", 1)
        iocType = "ip4"
        iocValue = "1.1.1.1"

        searcherPatch = getPatches({"search_raw.return_value": [(iocType, iocValue, 0, iocValue)]})
        postgresServiceMock.addIOCIfNotExistAsStream.return_value = rx.throw(Exception("Test Exception"))
        postgresServiceMock.getGlobalFiltersAsDictAsStream.return_value = rx.of(dict())
        postgresServiceMock.getSourceFiltersAsDictAsStream.return_value = rx.of(dict())

        searcherPatch.start()

        # Actual
        iocExtractor = IocExtractor(loggerMock, postgresServiceMock)
        iocExtractor.extract_features(article1).subscribe(scheduler=scheduler)

        # Assert
        loggerMock.error.assert_called()

        searcherPatch.stop()

    def test_extract_features_extract_error_complete(self):
        loggerMock, postgresServiceMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        article1 = ArticleContent(UUID_1, "content 1", 1)
        postgresServiceMock.getGlobalFiltersAsDictAsStream.return_value = rx.of(dict())
        postgresServiceMock.getSourceFiltersAsDictAsStream.return_value = rx.of(dict())

        searcherPatch = getPatches({"search_raw.side_effect": Exception("Test exception")})

        searcherPatch.start()

        # Actual
        iocExtractor = IocExtractor(loggerMock, postgresServiceMock)
        iocExtractor.extract_features(article1).subscribe(scheduler=scheduler)

        # Assert
        loggerMock.error.assert_called()

        searcherPatch.stop()

    def test_extract_features_globalFilter_shouldFilter(self):
        loggerMock, postgresServiceMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        article1 = ArticleContent(UUID_1, "content 1", 1)
        searcherReturn1 = ("ip4", "1.1.1.1", 0, "1.1.1.1")
        searcherReturn2 = ("ip4", "2.1.1.1", 0, "2.1.1.1")  # Should not be filtered
        searcherReturn3 = ("ip6", "1.1.1.1", 0, "1.1.1.1")  # Should not be filtered
        iocId1 = 1
        iocId2 = 2
        iocId3 = 3

        searcherPatch = getPatches({"search_raw.return_value": [searcherReturn1, searcherReturn2, searcherReturn3]})
        postgresServiceMock.addIOCIfNotExistAsStream.side_effect = [rx.of(iocId1), rx.of(iocId2), rx.of(iocId3),]
        postgresServiceMock.addArticleIocAsStream.side_effect = [rx.of((iocId1, UUID_1)), rx.of((iocId2, UUID_1)), rx.of((iocId3, UUID_1))]
        postgresServiceMock.getGlobalFiltersAsDictAsStream.return_value = rx.of(dict({
            3: ['1\\.1\\.1\\.1']
        }))
        postgresServiceMock.getSourceFiltersAsDictAsStream.return_value = rx.of(dict())

        searcherPatch.start()

        # Actual
        iocExtractor = IocExtractor(loggerMock, postgresServiceMock)
        iocExtractor.extract_features(article1).subscribe(scheduler=scheduler)

        # Assert
        postgresServiceMock.addIOCIfNotExistAsStream.assert_has_calls([
            call(searcherReturn2[1], iocIdToIdMapping[searcherReturn2[0]]),
            call(searcherReturn3[1], iocIdToIdMapping[searcherReturn3[0]])
        ])
        postgresServiceMock.addArticleIocAsStream.assert_has_calls([
            call(iocId1, UUID_1),
            call(iocId2, UUID_1),
        ])
        loggerMock.error.assert_not_called()

        searcherPatch.stop()

    def test_extract_features_sourceFilter_sameSource_shouldFilter(self):
        loggerMock, postgresServiceMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        article1 = ArticleContent(UUID_1, "content 1", 1)
        searcherReturn1 = ("ip4", "1.1.1.1", 0, "1.1.1.1")
        searcherReturn2 = ("ip4", "2.1.1.1", 0, "2.1.1.1")  # Should not be filtered
        searcherReturn3 = ("ip6", "1.1.1.1", 0, "1.1.1.1")  # Should not be filtered
        iocId1 = 1
        iocId2 = 2
        iocId3 = 3

        searcherPatch = getPatches({"search_raw.return_value": [searcherReturn1, searcherReturn2, searcherReturn3]})
        postgresServiceMock.addIOCIfNotExistAsStream.side_effect = [rx.of(iocId1), rx.of(iocId2), rx.of(iocId3),]
        postgresServiceMock.addArticleIocAsStream.side_effect = [rx.of((iocId1, UUID_1)), rx.of((iocId2, UUID_1)), rx.of((iocId3, UUID_1))]
        postgresServiceMock.getGlobalFiltersAsDictAsStream.return_value = rx.of(dict())
        postgresServiceMock.getSourceFiltersAsDictAsStream.return_value = rx.of(dict({
            3: ['1\\.1\\.1\\.1']
        }))

        searcherPatch.start()

        # Actual
        iocExtractor = IocExtractor(loggerMock, postgresServiceMock)
        iocExtractor.extract_features(article1).subscribe(scheduler=scheduler)

        # Assert
        postgresServiceMock.addIOCIfNotExistAsStream.assert_has_calls([
            call(searcherReturn2[1], iocIdToIdMapping[searcherReturn2[0]]),
            call(searcherReturn3[1], iocIdToIdMapping[searcherReturn3[0]])
        ])
        postgresServiceMock.addArticleIocAsStream.assert_has_calls([
            call(iocId1, UUID_1),
            call(iocId2, UUID_1),
        ])
        loggerMock.error.assert_not_called()

        searcherPatch.stop()
    
    def test_removeHTML(self):
        article_content = "&lt;p&gt;This is a &lt;b&gt;bold&lt;/b&gt; paragraph&lt;/p&gt;"
        loggerMock, postgresServiceMock = getMockObjects()
        iocExtractor = IocExtractor(loggerMock, postgresServiceMock)
        result = iocExtractor.removeHTML(article_content)
        self.assertEqual("This is a bold paragraph", result)


if __name__ == '__main__':
    unittest.main()
