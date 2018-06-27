# db_connector.py
# Author: Thomas MINIER - MIT License 2017-2018
from abc import ABC, abstractmethod


class DatabaseConnector(ABC):
    """A DatabaseConnector is an abstract class for creating connectors to a database"""

    @abstractmethod
    def search_triples(self, subject, predicate, obj, limit=0, offset=0):
        """
            Get an iterator over all RDF triples matching a triple pattern.

            Args:
                - subject ``string`` - Subject of the triple pattern
                - predicate ``string`` - Predicate of the triple pattern
                - object ``string`` - Object of the triple pattern
                - limit ``int=0`` ``optional`` -  LIMIT modifier, i.e., maximum number of RDF triples to read
                - offset ``int=0`` ``optional`` -  OFFSET modifier, i.e., number of RDF triples to skip

            Returns:
                A Python iterator over RDF triples matching the given triples pattern
        """
        pass

    @abstractmethod
    def from_config(config):
        """Build a DatabaseConnector from a dictionnary"""
        pass

    @property
    def nb_triples(self):
        """Get the number of RDF triples in the database"""
        return None

    @property
    def nb_subjects(self):
        """Get the number of subjects in the database"""
        return None

    @property
    def nb_predicates(self):
        """Get the number of predicates in the database"""
        return None

    @property
    def nb_objects(self):
        """Get the number of objects in the database"""
        return None
