from collections import OrderedDict
from uuid import UUID
import re

from src.config import iocIdToIdMapping


class ArticleInfo:
    def __init__(self, articleId, sourceId):
        self.articleId = articleId
        self.sourceId = sourceId

    def __eq__(self, other):
        return isinstance(other, ArticleInfo) and self.articleId == other.articleId and self.sourceId == other.sourceId

class IOCResult:
    """
    Object containing IOC type value pairs
    """
    def __init__(self, iocTypeStr, iocValue):
        self.iocType = iocIdToIdMapping.get(iocTypeStr, -1)
        self.iocValue = iocValue

    def __eq__(self, other):
        return isinstance(other, IOCResult) and self.iocType == other.iocType and self.iocValue == other.iocValue

class ArticleContent:
    """
    Object containing Article content and id
    """
    def __init__(self, articleId: UUID, articleContent: str, sourceId: int):
        self.articleId = articleId
        self.articleContent = articleContent
        self.sourceId = sourceId

    def __eq__(self, other):
        return (isinstance(other, ArticleContent)
                and self.articleId == other.articleId
                and self.articleContent == other.articleContent
                and self.sourceId == other.sourceId)

class IOCFilterPattern:
    def __init__(self, typeId: int, pattern: str):
        self.typeId = typeId
        self.pattern = re.compile(pattern)

class CategoryAssignerRule:
    def __init__(self, category_id: int, pattern: str):
        self.category_id = category_id
        self.pattern = re.compile(pattern, re.IGNORECASE)

class LeastRecentlyUsedDict:
    """
    A dict to act a cache with key value pairs which has a max capacity that auto deletes
    the least recently used key value pair
    """
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.storage = OrderedDict()

    def get(self, key: str):
        if key in self.storage:
            # Move to beginning (As most recently used)
            self.storage.move_to_end(key, last=False)
            return self.storage[key]
        return None

    def put(self, key: str, value):
        if key not in self.storage and len(self.storage) >= self.capacity:
            # Remove the end (Least recently used)
            self.storage.popitem()
        # Set value
        self.storage[key] = value
        # Move to beginning as most recently used
        self.storage.move_to_end(key, last=False)

    def __getitem__(self, item):
        return self.get(item)

    def __setitem__(self, key, value):
        self.put(key, value)

    def __len__(self):
        return len(self.storage)

    def __contains__(self, item):
        return item in self.storage
