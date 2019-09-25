# db_connector.py
# Author: Thomas MINIER - MIT License 2017-2018
from abc import ABC, abstractmethod


class DatabaseConnector(ABC):
    """A DatabaseConnector is an abstract class for creating connectors to a database"""

    @abstractmethod
    def search(self, subject, predicate, obj, last_read=None):
        """
            Get an iterator over all RDF triples matching a triple pattern.

            Args:
                - subject ``string`` - Subject of the triple pattern
                - predicate ``string`` - Predicate of the triple pattern
                - object ``string`` - Object of the triple pattern
                - last_read ``string=None`` ``optional`` -  OFFSET ID used to resume scan

            Returns:
                A tuple (`iterator`, `cardinality`), where `iterator` is a Python iterator over RDF triples matching the given triples pattern, and `cardinality` is the estimated cardinality of the triple pattern
        """
        pass

    @abstractmethod
    def from_config(config):
        """Build a DatabaseConnector from a dictionnary"""
        pass

    @property
    def nb_triples(self):
        """Get the number of RDF triples in the database"""
        return 0

    def open(self):
        """Open the database connection"""
        pass

    def close(self):
        """Close the database connection"""
        pass

    def insert(self, subject, predicate, obj):
        """
            Insert a RDF triple into the RDF Graph.
            If not overrided, this method raises an exception as it consider the graph as read-only.
        """
        raise NotImplementedError("The RDF graph is read-only: INSERT DATA queries are not allowed")

    def delete(self, subject, predicate, obj):
        """
            Delete a RDF triple from the RDF Graph.
            If not overrided, this method raises an exception as it consider the graph as read-only.
        """
        raise NotImplementedError("The RDF graph is read-only: DELETE DATA queries are not allowed")

    def start_transaction(self):
        """Start a transaction (if supported by this type of connector)"""
        pass

    def commit_transaction(self):
        """Commit any ongoing transaction (if supported by this type of connector)"""
        pass

    def abort_transaction(self):
        """Abort any ongoing transaction (if supported by this type of connector)"""
        pass

    def __enter__(self):
        """Implementation of the __enter__ method from the context manager spec"""
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        """Implementation of the __close__ method from the context manager spec"""
        self.close()

    def __del__(self):
        """Destructor"""
        self.close()

    @property
    def nb_subjects(self):
        """Get the number of subjects in the database"""
        return 0

    @property
    def nb_predicates(self):
        """Get the number of predicates in the database"""
        return 0

    @property
    def nb_objects(self):
        """Get the number of objects in the database"""
        return 0
