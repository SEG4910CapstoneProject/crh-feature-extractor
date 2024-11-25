import unittest

from src.config import ioc_patterns_file
from src.ioc_searcher import IocSearcher

class PatternFileTests(unittest.TestCase):
    def test_url_extractor_simple(self):
        print(ioc_patterns_file)

        expectedUrl = 'http://example.com'
        testUrls = [
            "http[://]example.com",
            "http://example[.]com",
            "http[://]example[.]com",
            "http://example[DOT]com",
        ]

        searcher = IocSearcher(patterns_ini=ioc_patterns_file)

        for url in testUrls:
            result = searcher.search_raw(url, targets={'url'})
            self.assertEqual(expectedUrl, result[0][1])


if __name__ == '__main__':
    unittest.main()
