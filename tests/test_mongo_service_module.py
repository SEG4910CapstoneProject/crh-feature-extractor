import unittest
from logging import Logger
from unittest.mock import *
from uuid import UUID
from pymongo import MongoClient
from pymongo.collection import Collection

from reactivex.scheduler import CurrentThreadScheduler
import reactivex.operators as ops

from src.mongo_service import MongoService
from src.collections import ArticleInfo, ArticleContent

UUID_1 = UUID("5d8a48c7-8799-49ba-8a61-b96c6f0d08e8")

def getMockObjects():
    loggerMock = Mock(spec_set=Logger)
    collectionMock = Mock(spec_set=Collection)
    documentMock = MagicMock()

    return loggerMock, collectionMock, documentMock

def getPatches(config):
    mongoPatch = patch("src.mongo_service.MongoClient", spec=MongoClient, **config)

    return mongoPatch

class MongoServiceTests(unittest.TestCase):
    def test_getByIdAsStream_success(self):
        loggerMock, collectionMock, documentMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        articleInfo = ArticleInfo(UUID_1, 1)
        expectedArticle1 = ArticleContent(UUID_1, "content 1", 1)

        mongoPatch = getPatches({"__getitem__.return_value.__getitem__.return_value": collectionMock})

        collectionMock.find_one.return_value = documentMock
        documentMock.__getitem__.return_value = expectedArticle1.articleContent

        mongoMock = mongoPatch.start()
        mongoService = MongoService(loggerMock, scheduler)

        # Actual
        actual = mongoService.getByIdAsStream(articleInfo).run()

        # Assert
        self.assertEqual(expectedArticle1, actual)
        loggerMock.error.assert_not_called()
        collectionMock.find_one.assert_called_once()
        documentMock.__getitem__.assert_called_once()

        mongoPatch.stop()

    def test_getByIdAsStream_error_retry_success(self):
        loggerMock, collectionMock, documentMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        articleInfo = ArticleInfo(UUID_1, 1)
        expectedArticle1 = ArticleContent(UUID_1, "content 1", 1)

        mongoPatch = getPatches({"__getitem__.return_value.__getitem__.return_value": collectionMock})

        collectionMock.find_one.side_effect = [Exception("Test Exception"), documentMock]
        documentMock.__getitem__.return_value = expectedArticle1.articleContent

        mongoMock = mongoPatch.start()
        mongoService = MongoService(loggerMock, scheduler)

        # Actual
        actual = mongoService.getByIdAsStream(articleInfo).run()

        # Assert
        self.assertEqual(expectedArticle1, actual)
        loggerMock.error.assert_called()
        self.assertEqual(2, collectionMock.find_one.call_count)
        documentMock.__getitem__.assert_called_once()

        mongoPatch.stop()

    def test_getByIdAsStream_error_retry_fail_complete(self):
        loggerMock, collectionMock, documentMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        articleInfo = ArticleInfo(UUID_1, 1)

        mongoPatch = getPatches({"__getitem__.return_value.__getitem__.return_value": collectionMock})

        collectionMock.find_one.side_effect = Exception("Test Exception")

        mongoMock = mongoPatch.start()
        mongoService = MongoService(loggerMock, scheduler)

        # Actual
        actual = mongoService.getByIdAsStream(articleInfo).pipe(
            ops.to_list()
        ).run()

        # Assert
        self.assertEqual(0, len(actual))
        loggerMock.error.assert_called()
        self.assertEqual(3, collectionMock.find_one.call_count)
        documentMock.__getitem__.assert_not_called()

        mongoPatch.stop()
    
    def test_getById_success(self):
        loggerMock, collectionMock, documentMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        articleInfo = ArticleInfo(UUID_1, 1)
        expectedArticle1 = ArticleContent(UUID_1, "content 1", 1)
        
        documentMock1 = {"_id": expectedArticle1.articleId, "web_scrap" : expectedArticle1.articleContent}

        collectionMock.find_one.return_value = documentMock1

        mongoPatch = getPatches({"__getitem__.return_value.__getitem__.return_value": collectionMock})

        mongoMock = mongoPatch.start()
        mongoService = MongoService(loggerMock, scheduler)

        # Actual
        actual = mongoService.getById(articleInfo)

        # Assert
        self.assertEqual(actual.articleId, expectedArticle1.articleId)
        self.assertEqual(actual.articleContent, expectedArticle1.articleContent)
        self.assertEqual(expectedArticle1, actual)
        loggerMock.error.assert_not_called()
        collectionMock.find_one.assert_called_once()

        mongoPatch.stop()
   
    def test_getById_resultNone(self):
        loggerMock, collectionMock, documentMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        collectionMock.find_one.return_value = None
        articleInfo = ArticleInfo(UUID_1, 1)
        
        mongoPatch = getPatches({"__getitem__.return_value.__getitem__.return_value": collectionMock})        

        mongoMock = mongoPatch.start()
        mongoService = MongoService(loggerMock, scheduler)

        # Actual
        actual = mongoService.getById(articleInfo)

        # Assert
        self.assertEqual(None, actual)
        loggerMock.warning.assert_called()
        collectionMock.find_one.assert_called_once()

        mongoPatch.stop()
    
    def test_getById_webScrapNone(self):
        loggerMock, collectionMock, documentMock = getMockObjects()
        scheduler = CurrentThreadScheduler()

        articleInfo = ArticleInfo(UUID_1, 1)
        expectedArticle1 = ArticleContent(UUID_1, None, 1)

        documentMock1 = {"_id": expectedArticle1.articleId, "web_scrap" : expectedArticle1.articleContent}

        collectionMock.find_one.return_value = documentMock1

        mongoPatch = getPatches({"__getitem__.return_value.__getitem__.return_value": collectionMock})

        mongoMock = mongoPatch.start()
        mongoService = MongoService(loggerMock, scheduler)

        # Actual
        actual = mongoService.getById(articleInfo)

        # Assert
        self.assertEqual(actual.articleId, expectedArticle1.articleId)
        self.assertEqual(actual.articleContent, "")
        loggerMock.error.assert_not_called()
        loggerMock.info.assert_called()
        collectionMock.find_one.assert_called_once()

        mongoPatch.stop()
    
    



if __name__ == '__main__':
    unittest.main()
