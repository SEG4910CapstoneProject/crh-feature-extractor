import psycopg
from uuid import UUID
from logging import Logger
import reactivex as rx
from reactivex import Observable, operators as ops
from collections import defaultdict

from src.collections import ArticleInfo, IOCFilterPattern, CategoryAssignerRule
from src.config import *

GET_NON_EXTRACTED_IDS_QUERY = """
    SELECT article_ID, source_ID FROM articles
    WHERE is_feature_ext = FALSE
"""

MARK_EXTRACTED_QUERY = """
    UPDATE articles
    SET is_feature_ext = TRUE
    WHERE article_ID = %s
"""

INSERT_IOC_QUERY = """
    INSERT INTO iocs (ioc_type, ioc_value)
    VALUES (%s, %s) 
    ON CONFLICT DO NOTHING 
"""

GET_IOC_ID_QUERY = """
    SELECT ioc_ID FROM iocs
    WHERE ioc_type = %s
    AND ioc_value = %s
"""

ADD_ARTICLE_IOC_QUERY = """
    INSERT INTO ioc_articles (article_ID, ioc_ID)
    VALUES (%s, %s)
"""

GET_GLOBAL_FILTERS_QUERY = """
    SELECT ioc_type_ID, ioc_pattern FROM ioc_filter_pattern
"""

GET_SOURCE_FILTERS_QUERY = """
    SELECT ioc_type_ID, ioc_pattern FROM ioc_source_filter_pattern
    WHERE source_ID = %s
"""

GET_CATEGORY_RULES_QUERY = """
    SELECT category_id, category_regex FROM category_rule
    ORDER BY category_rank ASC
"""

INSERT_CATEGORY_QUERY = """
    INSERT INTO article_category (category_id, article_id)
    VALUES (%s, %s)
"""


class PostgresService:
    """
    Service that handles all Postgres Db Operations
    """

    def __init__(self, logger: Logger, scheduler):
        self.logger = logger
        self.scheduler = scheduler

        self.connection = psycopg.connect("postgresql://{}:{}/{}?user={}&password={}"
                                          .format(POSTGRES_HOST,
                                                  POSTGRES_PORT,
                                                  POSTGRES_DB_NAME,
                                                  POSTGRES_USERNAME,
                                                  POSTGRES_PASSWORD),
                                          autocommit=True)

    def getNonExtractedIds(self):
        """
        Gets article ids for articles that has not been feature extracted
        :return: list of article ids that requires extraction as UUID
        """
        with self.connection.cursor() as cursor:
            cursor.execute(GET_NON_EXTRACTED_IDS_QUERY)

            result = cursor.fetchall()
            return [ArticleInfo(row[0], row[1]) for row in result]

    def getNonExtractedIdsAsStream(self):
        """
        Gets article ids for articles that has not been feature extracted as a stream
        :return: Observable that emits all article ids that requires extraction
        """
        # Start stream with value so when database calls are called, errors are transmitted into stream
        return rx.of(0).pipe(
            ops.do_action(on_next=lambda v: self.logger.info("Reading Article ids to extract")),
            # Extract id
            ops.map(lambda b: self.getNonExtractedIds()),
            # Retry
            ops.do_action(on_error=lambda err: self.logger.error("Failed to read from db", exc_info=err)),
            ops.retry(DB_MAX_RETRIES),
            ops.do_action(on_error=lambda err: self.logger.error("Retries Exhausted", exc_info=err)),
            ops.catch(rx.empty()),
            # Split array into individual elements
            ops.do_action(on_next=lambda idList: self.logger.info("Found %s articles to process", str(len(idList)))),
            ops.flat_map(lambda idList: rx.from_iterable(idList)),
            # Scheduler setup
            ops.subscribe_on(self.scheduler)
        )

    def markArticleAsExtracted(self, articleId: UUID):
        """
        Marks Article in db as having been extracted.
        :param articleId: Article id to mark
        """
        with self.connection.cursor() as cursor:
            cursor.execute(MARK_EXTRACTED_QUERY, (articleId,))

    def markArticleAsExtractedAsStream(self, articleId: UUID):
        """
        Marks Article in db as having been extracted as observable stream.
        :param articleId: Article id to mark
        :return: Observable stream that does the action
        """
        return rx.of(articleId).pipe(
            # Mark as extracted
            ops.do_action(self.markArticleAsExtracted),
            # Retry
            ops.do_action(on_error=lambda err: self.logger.error("Failed to write to db", exc_info=err)),
            ops.retry(DB_MAX_RETRIES),
            ops.do_action(on_error=lambda err: self.logger.error("Retries Exhausted", exc_info=err)),
            ops.catch(rx.empty()),
        )

    def addIOCIfNotExist(self, normalizedIocValue: str, iocTypeId: int):
        """
        Adds IOC to db if it does not exist
        :param normalizedIocValue: IOC Value normalized
        :param iocTypeId: The id number for the IOC Type
        :return: The IOC Id
        """
        with self.connection.cursor() as cursor:
            # Check if already exists
            cursor.execute(GET_IOC_ID_QUERY, (iocTypeId, normalizedIocValue))

            result = cursor.fetchone()
            if result is not None:
                # Already exists
                return result[0]

            # It does not exist, insert into table
            cursor.nextset()
            cursor.execute(INSERT_IOC_QUERY, (iocTypeId, normalizedIocValue))

            # Get id back
            cursor.nextset()
            cursor.execute(GET_IOC_ID_QUERY, (iocTypeId, normalizedIocValue))

            result = cursor.fetchone()
            if result is None:
                self.logger.error("Failed to get recently inserted IOC into Db.")
                return None
            return result[0]

    def addIOCIfNotExistAsStream(self, normalizedIocValue: str, iocTypeId: int):
        """
        Adds IOC to db if it does not exist as a stream
        :param normalizedIocValue: IOC Value normalized
        :param iocTypeId: The id number for the IOC Type
        :return: Observable containing the id of the IOC
        """
        return rx.of((normalizedIocValue, iocTypeId)).pipe(
            # Add IOC
            ops.map(lambda args: self.addIOCIfNotExist(args[0], args[1])),
            # Retry
            ops.do_action(on_error=lambda err: self.logger.error("Failed to write to db", exc_info=err)),
            ops.retry(DB_MAX_RETRIES),
            ops.do_action(on_error=lambda err: self.logger.error("Retries Exhausted", exc_info=err)),
            ops.catch(rx.empty()),
        )

    def addArticleIoc(self, iocId, articleId):
        """
        Add Article IOC Relation to db
        :param iocId: IOC id to add
        :param articleId: Article Id to add
        """
        with self.connection.cursor() as cursor:
            cursor.execute(ADD_ARTICLE_IOC_QUERY, (articleId, iocId))

    def addArticleIocAsStream(self, iocId, articleId):
        """
        Add Article IOC Relation to db as a stream
        :param iocId: IOC id to add
        :param articleId: Article Id to add
        :return: Observable that adds article IOC relation
        """
        return rx.of((iocId, articleId)).pipe(
            # Adds relation
            ops.do_action(lambda args: self.addArticleIoc(args[0], args[1])),
            # Retry
            ops.do_action(on_error=lambda err: self.logger.error("Failed to write to db", exc_info=err)),
            ops.retry(DB_MAX_RETRIES),
            ops.do_action(on_error=lambda err: self.logger.error("Retries Exhausted", exc_info=err)),
            ops.catch(rx.empty())
        )

    def _mapFiltersAndRetry(self, sourceObservable: Observable):
        return sourceObservable.pipe(
            # Retry
            ops.do_action(on_error=lambda err: self.logger.error("Failed to write to db", exc_info=err)),
            ops.retry(DB_MAX_RETRIES),
            ops.do_action(on_error=lambda err: self.logger.error("Retries Exhausted", exc_info=err)),
            # Convert to dict
            ops.map(lambda patterns: self._convertIocListToDict(patterns)),
            ops.catch(rx.empty())
        )

    def _convertIocListToDict(self, patterns: list[IOCFilterPattern]):
        """
        Maps a list of ioc filters into a type to ioc value map
        """
        result = defaultdict(list)

        for iocFilter in patterns:
            result[iocFilter.typeId].append(iocFilter.pattern)

        return dict(result)

    def getGlobalFilters(self):
        """
            Get all global filters from db
            :return IOCFilterPattern[]: Map containing id and pattern
        """
        with self.connection.cursor() as cursor:
            cursor.execute(GET_GLOBAL_FILTERS_QUERY)

            result = cursor.fetchall()
            return [IOCFilterPattern(row[0], row[1]) for row in result]

    def getGlobalFiltersAsDictAsStream(self):
        """
            Get all global filters from db as a stream
            :return: Observable that gets global filters as a dict
        """
        return self._mapFiltersAndRetry(rx.of(1).pipe(
            ops.map(lambda args: self.getGlobalFilters())
        ))

    def getSourceFilters(self, sourceId):
        """
            Get all global filters from db
            :return IOCFilterPattern[]: Map containing id and pattern
        """
        with self.connection.cursor() as cursor:
            cursor.execute(GET_SOURCE_FILTERS_QUERY, (sourceId,))

            result = cursor.fetchall()
            return [IOCFilterPattern(row[0], row[1]) for row in result]

    def getSourceFiltersAsDictAsStream(self, sourceId):
        """
            Get all global filters from db as a stream
            :return: Observable that gets global filters as a dict
        """

        return self._mapFiltersAndRetry(rx.of(1).pipe(
            ops.map(lambda args: self.getSourceFilters(sourceId)),
        ))

    def getCategoryRules(self):
        """
            Get category rules from db
            :return CategoryAssignerRule[]: List Containing category and rules
        """
        with self.connection.cursor() as cursor:
            cursor.execute(GET_CATEGORY_RULES_QUERY)

            result = cursor.fetchall()
            return [CategoryAssignerRule(row[0], row[1]) for row in result]

    def getCategoryRulesAsStream(self):
        """
            Get category rules from db
            :return: Observable that gets category rules
        """
        return rx.of(1).pipe(
            ops.map(lambda args: self.getCategoryRules()),
            # Retry
            ops.do_action(on_error=lambda err: self.logger.error("Failed to write to db", exc_info=err)),
            ops.retry(DB_MAX_RETRIES),
            ops.do_action(on_error=lambda err: self.logger.error("Retries Exhausted", exc_info=err)),
            ops.catch(rx.empty())
        )

    def insertCategoryArticle(self, categoryId: str, articleId: UUID):
        with self.connection.cursor() as cursor:
            cursor.execute(INSERT_CATEGORY_QUERY, (categoryId, articleId))

    def insertCategoryArticleAsStream(self, categoryId: str, articleId: UUID):
        return rx.of((categoryId, articleId)).pipe(
            # Add IOC
            ops.map(lambda args: self.insertCategoryArticle(args[0], args[1])),
            # Retry
            ops.do_action(on_error=lambda err: self.logger.error("Failed to write to db", exc_info=err)),
            ops.retry(DB_MAX_RETRIES),
            ops.do_action(on_error=lambda err: self.logger.error("Retries Exhausted", exc_info=err)),
            ops.catch(rx.empty()),
        )

    def close(self):
        """
        Closes the connection
        """
        self.connection.close()
