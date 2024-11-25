from logging import Logger
import reactivex as rx
from reactivex import operators as ops
import re

from src.base_extractor import BaseExtractor
from src.collections import ArticleContent, CategoryAssignerRule
from src.postgres_service import PostgresService


class CategoryAssigner(BaseExtractor):

    def __init__(self, logger: Logger, postgresService: PostgresService):
        self.logger = logger
        self.postgresService = postgresService

        self.categoryRulesStream = self.postgresService.getCategoryRulesAsStream().pipe(
            ops.replay(buffer_size=1),
        )

    def extract_features(self, article: ArticleContent):
        return rx.of(article).pipe(
            # Find Category
            ops.flat_map(lambda a: self.get_category(a)),
            # If None, writing is not required
            ops.filter(lambda item: item is not None),
            # Write Category
            ops.flat_map(lambda category_rule: self.insert_category(article, category_rule)),
            # Error Handling
            ops.do_action(on_error=lambda err: self.logger.error("Error occurred in Category Assigner", exc_info=err)),
            ops.catch(rx.empty()),
        )

    def get_category_rules(self):
        self.categoryRulesStream.connect()
        return self.categoryRulesStream.pipe(
            ops.flat_map(lambda categoryList: rx.from_iterable(categoryList)),
        )

    def doesArticleMatchRule(self, article: ArticleContent, categoryRule: CategoryAssignerRule):
        return categoryRule.pattern.search(article.articleContent) is not None

    def get_category(self, article: ArticleContent):
        return self.get_category_rules().pipe(
            ops.filter(lambda rule: self.doesArticleMatchRule(article, rule)),
            ops.first_or_default(),
            # Error Handling
            ops.do_action(on_error=lambda err: self.logger.error("Error occurred while finding categories", exc_info=err)),
            ops.catch(rx.empty()),
        )

    def insert_category(self, article: ArticleContent, category: CategoryAssignerRule):
        return self.postgresService.insertCategoryArticleAsStream(str(category.category_id), article.articleId)
