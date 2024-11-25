from abc import abstractmethod, ABC
from src.collections import ArticleContent


class BaseExtractor(ABC):
    """
    Base Extractor for features
    """

    @abstractmethod
    def extract_features(self, article: ArticleContent):
        """
        Extracts Features from provided article and inserts into database as required
        :param article: Article to grab features
        :return: Observable that extracts features and inserts into db
        """
        pass
