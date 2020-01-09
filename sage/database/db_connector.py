# db_connector.py
# Author: Thomas MINIER - MIT License 2017-2020
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Tuple

from sage.database.db_iterator import DBIterator


class DatabaseConnector(ABC):
    """A DatabaseConnector is an abstract class for creating connectors to a database"""

    def open(self):
        """Open the database connection"""
        pass

    def close(self):
        """Close the database connection"""
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

    @abstractmethod
    def search(self, subject: str, predicate: str, obj: str, last_read: Optional[str] = None, as_of: Optional[datetime] = None) -> Tuple[DBIterator, int]:
        """Get an iterator over all RDF triples matching a triple pattern.

        Args:
          * subject: Subject of the triple pattern.
          * predicate: Predicate of the triple pattern.
          * object: Object of the triple pattern.
          * last_read: A RDF triple ID. When set, the search is resumed for this RDF triple.
          * as_of: A version timestamp. When set, perform all reads against a consistent snapshot represented by this timestamp.
          
        Returns:
          A tuple (`iterator`, `cardinality`), where `iterator` is a Python iterator over RDF triples matching the given triples pattern, and `cardinality` is the estimated cardinality of the triple pattern.

        Example:
          >>> iterator, cardinality = connector.search('?s', 'http://xmlns.com/foaf/0.1/name', '?name')
          >>> print(f"The triple pattern '?s foaf:name ?o' matches {cardinality} RDF triples")
          >>> for s, p, o in iterator:
          >>>   print(f"RDF Triple {s} {p} {o}")
        """
        pass

    @abstractmethod
    def from_config(config: dict):
        """Build a DatabaseConnector from a dictionnary"""
        pass

    def open(self) -> None:
        """Open the database connection"""
        pass

    def close(self) -> None:
        """Close the database connection"""
        pass

    def insert(self, subject: str, predicate: str, obj: str) -> None:
        """Insert a RDF triple into the RDF graph.

        If not overrided, this method raises an exception as it consider the graph as read-only.
        
        Args:
          * subject: Subject of the RDF triple.
          * predicate: Predicate of the RDF triple.
          * obj: Object of the RDF triple.
        
        Throws: `NotImplementedError` if the database connection is read-only.
        """
        raise NotImplementedError("The RDF graph is read-only: INSERT DATA queries are not allowed")

    def delete(self, ssubject: str, predicate: str, obj: str) -> None:
        """Delete a RDF triple from the RDF graph.

        If not overrided, this method raises an exception as it consider the graph as read-only.
        
        Args:
          * subject: Subject of the RDF triple.
          * predicate: Predicate of the RDF triple.
          * obj: Object of the RDF triple.
        
        Throws: `NotImplementedError` if the database connection is read-only.
        """
        raise NotImplementedError("The RDF graph is read-only: DELETE DATA queries are not allowed")

    def start_transaction(self) -> None:
        """Start a transaction (if supported by this type of connector)"""
        pass

    def commit_transaction(self) -> None:
        """Commit any ongoing transaction (if supported by this type of connector)"""
        pass

    def abort_transaction(self) -> None:
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
    def nb_triples(self) -> int:
        """Get the number of RDF triples in the database"""
        return 0

    @property
    def nb_subjects(self) -> int:
        """Get the number of subjects in the database"""
        return 0

    @property
    def nb_predicates(self) -> int:
        """Get the number of predicates in the database"""
        return 0

    @property
    def nb_objects(self) -> int:
        """Get the number of objects in the database"""
        return 0
