import reactivex as rx
from reactivex import operators as ops
from pymongo import MongoClient

from src.collections import ArticleInfo, ArticleContent
from src.config import *


class MongoService:
    """
    Service that handles all mongo db operations
    """

    def __init__(self, logger, scheduler):
        self.logger = logger
        self.scheduler = scheduler

        self.client = MongoClient("mongodb://{}:{}/".format(MONGO_HOST, MONGO_PORT),
                                  username=MONGO_USERNAME,
                                  password=MONGO_PASSWORD,
                                  uuidRepresentation='standard')
        self.collection = self.client[MONGO_DB_NAME][MONGO_COLLECTION]

    def getById(self, articleId: ArticleInfo):
        """
        Get document by id
        :param articleId: UUID of desired article.
        :return: Article Content object containing the article.
        """
        result = self.collection.find_one({"_id": articleId.articleId, "web_scrap": {"$exists": True}})

        if result is None:
            self.logger.warning("Failed to find article with id %s. Might not be web scrapped?", str(articleId.articleId))
            return None

        webScrapResult = result["web_scrap"]

        # Not yet web scraped
        if webScrapResult is None:
            self.logger.info("Article id %s has no web scrapped data. Marking empty", str(articleId.articleId))
            # Empty string allows it to go through, so it may be marked as extracted while
            # acting as if there is no content
            webScrapResult = ""

        return ArticleContent(articleId.articleId, webScrapResult, articleId.sourceId)

    def getByIdAsStream(self, articleId: ArticleInfo):
        """
        Get article by id using Observable Stream
        :param articleId: UUID of desired article.
        :return: Observable that emits the article content
        """
        return rx.of(articleId).pipe(
            # Get by id
            ops.map(lambda uid: self.getById(uid)),
            # Retry
            ops.do_action(on_error=lambda err: self.logger.error("Failed to read from db", exc_info=err)),
            ops.retry(DB_MAX_RETRIES),
            ops.do_action(on_error=lambda err: self.logger.error("Retries Exhausted", exc_info=err)),
            ops.catch(rx.empty()),
            # Remove if content not available
            ops.filter(lambda doc: doc is not None),
            ops.subscribe_on(scheduler=self.scheduler),
        )
