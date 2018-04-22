# fragment_factory.py
# Author: Thomas MINIER - MIT License 2017-2018
from abc import ABC, abstractmethod


class FragmentFactory(ABC):
    """A FragmentFactory is an abstract class for defining factories of LDF fragments"""

    @abstractmethod
    def search_triples(self, subject, predicate, obj, limit=0, offset=0):
        """Get all triples matching a triple pattern"""
        pass

    @abstractmethod
    def from_config(config):
        """Build a FragmentFactory from a config file"""
        pass
