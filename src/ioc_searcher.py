import re

from iocsearcher.searcher import Searcher


class IocSearcher(Searcher):
    """
    This class overrides rearm/normalization methods to work with the new patterns config file
    and additional normalization desires
    """
    re_scheme_domain_separator = re.compile(r'\[?://]?', re.I)

    @classmethod
    def rearm_url(cls, s):
        s = re.sub(cls.re_scheme_domain_separator, '://', s)
        return super().rearm_url(s)
