# graph.py
# Author: Thomas MINIER - MIT License 2017-2020
from datetime import datetime
from math import inf
from typing import List, Optional, Tuple

from sage.database.db_connector import DatabaseConnector
from sage.database.db_iterator import DBIterator


class Graph(object):
    """A RDF Graph with a dedicated backend"""

    def __init__(self, name: str, description: str, connector: DatabaseConnector, quantum=75, max_results=inf, default_queries: List[dict] = list()):
        super(Graph, self).__init__()
        self._name = name
        self._description = description
        self._connector = connector
        self._quantum = quantum
        self._max_results = max_results
        self._example_queries = default_queries

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def quota(self) -> float:
        return self._quantum

    @property
    def max_results(self) -> float:
        return self._max_results

    @property
    def nb_triples(self) -> int:
        return self._connector.nb_triples

    @property
    def example_queries(self) -> List[dict]:
        return self._example_queries

    def connector(self) -> DatabaseConnector:
        """Get the underlying DatabaseConnector for this dataset"""
        return self._connector

    def search(self, subject: str, predicate: str, obj: str, last_read: Optional[str] = None, as_of: Optional[datetime] = None) -> Tuple[DBIterator, int]:
        """
            Get an iterator over all RDF triples matching a triple pattern.
            Args:
                - subject [string] - Subject of the triple pattern
                - predicate [string] - Preicate of the triple pattern
                - object [string] - Object of the triple pattern
                - last_read ``[string=None]`` ``optional`` -  OFFSET ID used to resume scan
                - as_of ``datetime=None`` ``optional`` - Perform all reads against a consistent snapshot represented by a timestamp.
            Returns:
                A tuple (`iterator`, `cardinality`), where `iterator` is a Python iterator over RDF triples matching the given triples pattern, and `cardinality` is the estimated cardinality of the triple pattern
        """
        return self._connector.search(subject, predicate, obj, last_read=last_read, as_of=as_of)

    def insert(self, subject: str, predicate: str, obj: str):
        """Insert a RDF triple into the RDF graph"""
        self._connector.insert(subject, predicate, obj)

    def delete(self, subject: str, predicate: str, obj: str):
        """Delete a RDF triple from the RDF graph"""
        self._connector.delete(subject, predicate, obj)

    def commit(self) -> None:
        """Commit any ongoing transaction (at the database level)"""
        self._connector.commit_transaction()

    def abort(self) -> None:
        """Abort any ongoing transaction (at the database level)"""
        self._connector.abort_transaction()

    def describe(self, url: str) -> dict:
        """Describe the RDF Dataset in JSON-LD format"""
        return {
            "@context": {
                "schema": "http://schema.org/",
                "void": "http://rdfs.org/ns/void#",
                'sage': 'http://sage.univ-nantes.fr/sage-voc#'
            },
            "@type": "http://schema.org/Dataset",
            "schema:url": url,
            "schema:name": self._name,
            "schema:description": self._description,
            "void:triples": self.nb_triples,
            "void:distinctSubjects": self._connector.nb_subjects if self._connector.nb_subjects is not None else "unknown",
            "void:properties": self._connector.nb_predicates if self._connector.nb_predicates is not None else "unknown",
            "void:distinctObjects": self._connector.nb_objects if self._connector.nb_objects is not None else "unknown",
            "sage:timeQuota": self._quantum,
            "sage:maxResults": self.max_results if self.max_results is not inf else 'inf'
        }

    def get_query(self, q_id: str) -> Optional[str]:
        """Get an example SPARQL query associated with the graph, or None if it was not found"""
        for query in self.example_queries:
            if query['@id'] == q_id:
                return query
        return None
