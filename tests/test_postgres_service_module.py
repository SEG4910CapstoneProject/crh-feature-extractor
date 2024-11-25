import re
import unittest
from unittest.mock import *
from psycopg import Connection
from psycopg import Cursor
from reactivex.scheduler import CurrentThreadScheduler
from src.postgres_service import *

UUID_1 = UUID("5d8a48c7-8799-49ba-8a61-b96c6f0d08e8")
UUID_2 = UUID("2c398d08-22e0-4f69-955b-69fb39666a9c")
UUID_3 = UUID("8c819db1-3dfa-4343-b6e7-9b73495fcdec")

def getMockObjects():
    loggerMock = Mock(spec_set=Logger)
    connectionMock = Mock(spec_set=Connection)
    cursorMock = MagicMock(spec_set=Cursor)

    connectionMock.cursor.return_value = cursorMock
    cursorMock.__enter__.return_value = cursorMock

    return loggerMock, connectionMock, cursorMock

def getPatches(connectionMock):
    config = {"connect.return_value": connectionMock}
    postgresPatch = patch("src.postgres_service.psycopg", spec=psycopg, **config)

    return postgresPatch

class PostgresServiceTests(unittest.TestCase):
    def test_getNonExtractedIdsAsStream_success(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        expectedArticleInfo1 = ArticleInfo(UUID_1, 1)
        expectedArticleInfo2 = ArticleInfo(UUID_2, 1)
        expectedArticleInfo3 = ArticleInfo(UUID_3, 1)

        cursorMock.fetchall.return_value = [
            [expectedArticleInfo1.articleId, expectedArticleInfo1.sourceId],
            [expectedArticleInfo2.articleId, expectedArticleInfo2.sourceId],
            [expectedArticleInfo3.articleId, expectedArticleInfo3.sourceId],
        ]

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        actual = postgresService.getNonExtractedIdsAsStream().pipe(
            ops.to_list()
        ).run()

        # Assert
        self.assertEqual(3, len(actual))
        self.assertIn(expectedArticleInfo1, actual)
        self.assertIn(expectedArticleInfo2, actual)
        self.assertIn(expectedArticleInfo3, actual)
        cursorMock.execute.assert_called_once()
        cursorMock.fetchall.assert_called_once()
        loggerMock.error.assert_not_called()

        postgresPatch.stop()

    def test_getNonExtractedIdsAsStream_error_retry(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        expectedArticleInfo1 = ArticleInfo(UUID_1, 1)
        expectedArticleInfo2 = ArticleInfo(UUID_2, 1)
        expectedArticleInfo3 = ArticleInfo(UUID_3, 1)

        cursorMock.execute.side_effect = [Exception("Test Exception"), cursorMock]
        cursorMock.fetchall.return_value = [
            [expectedArticleInfo1.articleId, expectedArticleInfo1.sourceId],
            [expectedArticleInfo2.articleId, expectedArticleInfo2.sourceId],
            [expectedArticleInfo3.articleId, expectedArticleInfo3.sourceId],
        ]

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        actual = postgresService.getNonExtractedIdsAsStream().pipe(
            ops.to_list()
        ).run()

        # Assert
        self.assertEqual(3, len(actual))
        self.assertIn(expectedArticleInfo1, actual)
        self.assertIn(expectedArticleInfo2, actual)
        self.assertIn(expectedArticleInfo3, actual)
        self.assertEqual(2, cursorMock.execute.call_count)
        cursorMock.fetchall.assert_called_once()
        loggerMock.error.assert_called()

        postgresPatch.stop()

    def test_getNonExtractedIdsAsStream_error_complete(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        cursorMock.execute.side_effect = Exception("Test Exception")

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        actual = postgresService.getNonExtractedIdsAsStream().pipe(
            ops.to_list()
        ).run()

        # Assert
        self.assertEqual(0, len(actual))
        self.assertEqual(3, cursorMock.execute.call_count)
        cursorMock.fetchall.assert_not_called()
        loggerMock.error.assert_called()

        postgresPatch.stop()

    def test_markArticleAsExtractedAsStream_success(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        postgresService.markArticleAsExtractedAsStream(UUID_1).subscribe(scheduler=scheduler)

        # Assert
        cursorMock.execute.assert_called_once()
        loggerMock.error.assert_not_called()

        postgresPatch.stop()

    def test_markArticleAsExtractedAsStream_error_retry(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        cursorMock.execute.side_effect = [Exception("Test Exception"), cursorMock]

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        postgresService.markArticleAsExtractedAsStream(UUID_1).subscribe(scheduler=scheduler)

        # Assert
        self.assertEqual(2, cursorMock.execute.call_count)
        loggerMock.error.assert_called()

        postgresPatch.stop()

    def test_markArticleAsExtractedAsStream_error_complete(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        cursorMock.execute.side_effect = Exception("Test Exception")

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        postgresService.markArticleAsExtractedAsStream(UUID_1).subscribe(scheduler=scheduler)

        # Assert
        self.assertEqual(3, cursorMock.execute.call_count)
        loggerMock.error.assert_called()

        postgresPatch.stop()

    def test_addIOCIfNotExistAsStream_success(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        iocValue = "ioc"
        iocTypeId = 1
        iocReturnId = 2

        cursorMock.fetchone.side_effect = [None, [iocReturnId]]

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        actual = postgresService.addIOCIfNotExistAsStream(iocValue, iocTypeId).run()

        # Assert
        self.assertEqual(iocReturnId, actual)
        cursorMock.execute.assert_has_calls([
            call(GET_IOC_ID_QUERY, (iocTypeId, iocValue)),
            call(INSERT_IOC_QUERY, (iocTypeId, iocValue)),
            call(GET_IOC_ID_QUERY, (iocTypeId, iocValue))
        ], any_order=False)
        self.assertEqual(2, cursorMock.nextset.call_count)
        loggerMock.error.assert_not_called()

        postgresPatch.stop()

    def test_addIOCIfNotExistAsStream_already_exists_success(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        iocValue = "ioc"
        iocTypeId = 1
        iocReturnId = 2

        cursorMock.fetchone.return_value = [iocReturnId]

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        actual = postgresService.addIOCIfNotExistAsStream(iocValue, iocTypeId).run()

        # Assert
        self.assertEqual(iocReturnId, actual)
        cursorMock.execute.assert_called_once_with(GET_IOC_ID_QUERY, (iocTypeId, iocValue))
        self.assertEqual(0, cursorMock.nextset.call_count)
        loggerMock.error.assert_not_called()

        postgresPatch.stop()

    def test_addIOCIfNotExistAsStream_failed_twice_complete(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        iocValue = "ioc"
        iocTypeId = 1

        cursorMock.fetchone.return_value = None

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        actual = postgresService.addIOCIfNotExistAsStream(iocValue, iocTypeId).run()

        # Assert
        self.assertEqual(None, actual)
        cursorMock.execute.assert_has_calls([
            call(GET_IOC_ID_QUERY, (iocTypeId, iocValue)),
            call(INSERT_IOC_QUERY, (iocTypeId, iocValue)),
            call(GET_IOC_ID_QUERY, (iocTypeId, iocValue))
        ], any_order=False)
        self.assertEqual(2, cursorMock.nextset.call_count)
        loggerMock.error.assert_called_once()

        postgresPatch.stop()

    def test_addIOCIfNotExistAsStream_error_retry(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        iocValue = "ioc"
        iocTypeId = 1
        iocReturnId = 2

        cursorMock.execute.side_effect = [Exception("Test Exception"), cursorMock, cursorMock, cursorMock]
        cursorMock.fetchone.side_effect = [None, [iocReturnId]]

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        actual = postgresService.addIOCIfNotExistAsStream(iocValue, iocTypeId).run()

        # Assert
        self.assertEqual(iocReturnId, actual)
        cursorMock.execute.assert_has_calls([
            call(GET_IOC_ID_QUERY, (iocTypeId, iocValue)),
            call(GET_IOC_ID_QUERY, (iocTypeId, iocValue)),
            call(INSERT_IOC_QUERY, (iocTypeId, iocValue)),
            call(GET_IOC_ID_QUERY, (iocTypeId, iocValue))
        ], any_order=False)
        self.assertEqual(2, cursorMock.nextset.call_count)
        loggerMock.error.assert_called()

        postgresPatch.stop()

    def test_addIOCIfNotExistAsStream_retry_exhausted_complete(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        iocValue = "ioc"
        iocTypeId = 1

        cursorMock.execute.side_effect = Exception("Test Exception")

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        actual = postgresService.addIOCIfNotExistAsStream(iocValue, iocTypeId).pipe(
            ops.to_list()
        ).run()

        # Assert
        self.assertEqual(0, len(actual))
        cursorMock.execute.assert_has_calls([
            call(GET_IOC_ID_QUERY, (iocTypeId, iocValue)),
            call(GET_IOC_ID_QUERY, (iocTypeId, iocValue)),
            call(GET_IOC_ID_QUERY, (iocTypeId, iocValue))
        ], any_order=False)
        loggerMock.error.assert_called()

        postgresPatch.stop()

    def test_addArticleIocAsStream_success(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        iocTypeId = 1

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        postgresService.addArticleIocAsStream(iocTypeId, UUID_1).subscribe(scheduler=scheduler)

        # Assert
        cursorMock.execute.assert_called_once()
        loggerMock.error.assert_not_called()

        postgresPatch.stop()

    def test_addArticleIocAsStream_error_retry(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        iocTypeId = 1

        cursorMock.execute.side_effect = [Exception("Test Exception"), cursorMock]

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        postgresService.addArticleIocAsStream(iocTypeId, UUID_1).subscribe(scheduler=scheduler)

        # Assert
        self.assertEqual(2, cursorMock.execute.call_count)
        loggerMock.error.assert_called()

        postgresPatch.stop()

    def test_addArticleIocAsStream_error_complete(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        iocTypeId = 1

        cursorMock.execute.side_effect = Exception("Test Exception")

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        postgresService.addArticleIocAsStream(iocTypeId, UUID_1).subscribe(scheduler=scheduler)

        # Assert
        self.assertEqual(3, cursorMock.execute.call_count)
        loggerMock.error.assert_called()

        postgresPatch.stop()

    def test_getGlobalFiltersAsDictAsStream_success(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        cursorMock.fetchall.return_value = [[0, 'value0'], [1, 'value1'], [2, 'value2']]

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        actual: dict[int, str] = postgresService.getGlobalFiltersAsDictAsStream().run()

        # Assert
        cursorMock.execute.assert_called_once()
        loggerMock.error.assert_not_called()
        self.assertIsNotNone(actual)
        self.assertEqual(re.compile('value0'), actual[0][0])
        self.assertEqual(1, len(actual[0]))
        self.assertEqual(re.compile('value1'), actual[1][0])
        self.assertEqual(1, len(actual[1]))
        self.assertEqual(re.compile('value2'), actual[2][0])
        self.assertEqual(1, len(actual[2]))

        postgresPatch.stop()

    def test_getGlobalFiltersAsDictAsStream_error_retry(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()

        cursorMock.fetchall.return_value = [[0, 'value0'], [1, 'value1'], [2, 'value2']]
        cursorMock.execute.side_effect = [Exception("Test Exception"), cursorMock]

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        actual: dict[int, str] = postgresService.getGlobalFiltersAsDictAsStream().run()

        # Assert
        self.assertEqual(2, cursorMock.execute.call_count)
        loggerMock.error.assert_called()
        self.assertIsNotNone(actual)
        self.assertEqual(re.compile('value0'), actual[0][0])
        self.assertEqual(1, len(actual[0]))
        self.assertEqual(re.compile('value1'), actual[1][0])
        self.assertEqual(1, len(actual[1]))
        self.assertEqual(re.compile('value2'), actual[2][0])
        self.assertEqual(1, len(actual[2]))

        postgresPatch.stop()

    def test_getGlobalFiltersAsDictAsStream_error_complete(self):
        loggerMock, connectionMock, cursorMock = getMockObjects()
        postgresPatch = getPatches(connectionMock)
        scheduler = CurrentThreadScheduler()
        cursorMock.execute.side_effect = Exception("Test Exception")

        postgresPatch.start()
        postgresService = PostgresService(loggerMock, scheduler)

        # Actual
        actual: dict[int, str] = postgresService.getGlobalFiltersAsDictAsStream().pipe(
            ops.to_list()
        ).run()

        # Assert
        self.assertEqual(3, cursorMock.execute.call_count)
        self.assertEqual(0, len(actual))
        loggerMock.error.assert_called()

        postgresPatch.stop()

if __name__ == '__main__':
    unittest.main()
