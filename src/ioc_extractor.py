from logging import Logger
import reactivex as rx
from reactivex import Observable, operators as ops
import re
import html

from src.collections import LeastRecentlyUsedDict, IOCResult
from src.config import iocIdToIdMapping, ioc_patterns_file, SOURCE_FILTER_CACHE_SIZE
from src.base_extractor import BaseExtractor
from src.postgres_service import PostgresService
from src.ioc_searcher import IocSearcher


class IocExtractor(BaseExtractor):
    """
    An Extractor that extracts IOCs
    """

    def __init__(self, logger: Logger, postgresService: PostgresService):
        self.logger = logger
        self.postgresService = postgresService
        self.searcher = IocSearcher(patterns_ini=ioc_patterns_file)
        self.globalFilterObservable = postgresService.getGlobalFiltersAsDictAsStream().pipe(
            ops.replay(buffer_size=1)
        )
        self.globalFilterObservable.connect()
        self.iocSourceFilterCache = LeastRecentlyUsedDict(SOURCE_FILTER_CACHE_SIZE)

    def extract_features(self, article):
        return rx.of(article).pipe(
            ops.map(lambda singleArt: singleArt.articleContent),
            # Remove HTML (undo escape and remove it)
            ops.map(lambda singleArt: self.removeHTML(singleArt)),
            # Extracts IOCs
            ops.flat_map(lambda singleArt: self.extractIocs(singleArt, article.sourceId)),
            # Push IOC to db
            ops.flat_map(lambda ioc: self.postgresService.addIOCIfNotExistAsStream(ioc.iocValue, ioc.iocType)),
            ops.filter(lambda iocId: iocId is not None),
            # Push IOC Article relation to db
            ops.flat_map(lambda ioc_Id: self.postgresService.addArticleIocAsStream(ioc_Id, article.articleId)),
            # Error Handling
            ops.do_action(on_error=lambda err: self.logger.error("Error occurred in IOC Extractor", exc_info=err)),
            ops.catch(rx.empty()),
        )

    def removeHTML(self, inputString):
        """
        Unespaces HTML tags and then removes all HTML tags
        :param inputString: String with HTML escaped article content
        :return: String with no HTML tags 
        """
        # Unescape HTML entities
        unescapedString = html.unescape(inputString)
        
        # Remove HTML tags using regex
        cleanString = re.sub(r'<[^>]+>', '', unescapedString)
        
        return cleanString 

    def extractIocs(self, articleContent, sourceId):
        """
        Extracts IOCs and emits them into a stream
        :param articleContent: Article from which to get IOCs
        :param sourceId: the source id of the article
        :return: Observable Stream with IOCs
        """
        return rx.of(articleContent).pipe(
            # Use ioc searcher to get IOCs
            ops.map(lambda content: self.searcher.search_raw(content, targets=iocIdToIdMapping.keys())),
            ops.filter(lambda iocs: iocs is not None),
            # Converts array into individual elements
            ops.flat_map(rx.from_iterable),
            ops.map(lambda originalIoc: IOCResult(originalIoc[0], originalIoc[1])),
            self.filterIocsOperator(sourceId),
            # Error handling
            ops.do_action(on_error=lambda err: self.logger.error("Error occurred while extracting", exc_info=err)),
            ops.catch(rx.empty()),
        )

    def getSourceFilterStream(self, sourceId):
        if sourceId not in self.iocSourceFilterCache:
            self.iocSourceFilterCache[sourceId] = self.postgresService.getSourceFiltersAsDictAsStream(sourceId).pipe(
                ops.replay(buffer_size=1)
            )
            self.iocSourceFilterCache[sourceId].connect()
        return self.iocSourceFilterCache[sourceId]

    def getFilterForType(self, filterObservable: Observable, typeId: int):
        return filterObservable.pipe(
            ops.map(lambda typePatternListDict:
                    typePatternListDict[typeId] if typeId in typePatternListDict else list()),
        )

    def filterIocsOperator(self, sourceId: int):
        """
        Filters ioc stream
        """
        return rx.compose(
            # Filters duplicates
            ops.distinct(),
            # Gets filters
            ops.flat_map(
                lambda iocResult: rx.combine_latest(
                    rx.of(iocResult),
                    self.getFilterForType(self.globalFilterObservable, iocResult.iocType),
                    self.getFilterForType(self.getSourceFilterStream(sourceId), iocResult.iocType)
                )
            ),
            # Apply Filters
            ops.flat_map(
                lambda iocFilterTuple: rx.of(iocFilterTuple[0]).pipe(
                    ops.filter(lambda iocResult: not self.doesIocMatchSomeFilter(iocResult, iocFilterTuple[1])),
                    ops.filter(lambda iocResult: not self.doesIocMatchSomeFilter(iocResult, iocFilterTuple[2])),
                )
            )
        )

    def doesIocMatchSomeFilter(self, iocResult: IOCResult, filterList: list[str]):
        return any(re.fullmatch(regex, iocResult.iocValue) for regex in filterList)
