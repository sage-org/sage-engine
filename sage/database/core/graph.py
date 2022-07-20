from datetime import datetime
from math import inf
from typing import List, Optional, Tuple, Dict

from sage.database.backends.db_connector import DatabaseConnector
from sage.database.backends.db_iterator import DBIterator


class Graph(object):
    """
    An RDF Graph with a dedicated backend used to search/store RDF triples.

    Parameters
    ----------
    uri: str
        URI of the RDF Graph.
    name: str
        Name of the RDF Graph.
    description: str
        Description of the RDF Graph.
    connector: DatabaseConnector
        Database connector used to search/store RDF triples in this graph.
    default_queries: List[Dict[str, str]]
        List of queries that can be executed with this graph.
    """

    def __init__(
        self, uri: str, name: str, description: str, connector: DatabaseConnector,
        default_queries: List[Dict[str, str]] = list()
    ) -> None:
        super(Graph, self).__init__()
        self._uri = uri
        self._name = name
        self._description = description
        self._connector = connector
        self._example_queries = default_queries

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def nb_triples(self) -> int:
        return self._connector.nb_triples

    @property
    def example_queries(self) -> List[dict]:
        return self._example_queries

    def connector(self) -> DatabaseConnector:
        """
        Returns the underlying DatabaseConnector for this dataset.
        """
        return self._connector

    def search(
        self, subject: str, predicate: str, obj: str,
        last_read: Optional[str] = None,
        as_of: Optional[datetime] = None
    ) -> Tuple[DBIterator, int]:
        """
        Get an iterator over all RDF triples matching a triple pattern.

        Parameters
        ----------
        subject: str
            Subject of the triple pattern.
        predicate: str
            Predicate of the triple pattern.
        object: str
            Object of the triple pattern.
        last_read: None | str
            A RDF triple ID. When set, the search is resumed for this RDF triple.
        as_of: None | datetime
            A version timestamp. When set, perform all reads against a
            consistent snapshot represented by this timestamp.

        Returns
        -------
        Tuple[DBIterator, int]
            A tuple (iterator, cardinality) where:
                - iterator: Python iterator over RDF triples matching the given
                  triple pattern.
                - cardinality: Estimated cardinality of the triple pattern.

        Example
        -------
        >>> iterator, cardinality = graph.search('?s', 'http://xmlns.com/foaf/0.1/name', '?name')
        >>> print(f"The triple pattern '?s foaf:name ?o' matches {cardinality} RDF triples")
        >>> for s, p, o in iterator:
        >>>     print(f"RDF Triple {s} {p} {o}")
        """
        return self._connector.search(subject, predicate, obj, last_read=last_read, as_of=as_of)

    def insert(self, subject: str, predicate: str, obj: str):
        """
        Inserts an RDF triple into the RDF graph.

        Parameters
        ----------
        subject: str
            Subject of the RDF triple.
        predicate: str
            Predicate of the RDF triple.
        obj: str
            Object of the RDF triple.
        """
        self._connector.insert(subject, predicate, obj)

    def delete(self, subject: str, predicate: str, obj: str):
        """
        Deletes an RDF triple from the RDF graph.

        Parameters
        ----------
        subject: str
            Subject of the RDF triple.
        predicate: str
            Predicate of the RDF triple.
        obj: str
            Object of the RDF triple.
        """
        self._connector.delete(subject, predicate, obj)

    def commit(self) -> None:
        """
        Commits any ongoing transaction (at the database level).
        """
        self._connector.commit_transaction()

    def abort(self) -> None:
        """
        Aborts any ongoing transaction (at the database level).
        """
        self._connector.abort_transaction()

    def describe(self, url: str) -> dict:
        """
        Describes the RDF Dataset in JSON-LD format.
        """
        return {
            "@context": {
                "schema": "http://schema.org/",
                "void": "http://rdfs.org/ns/void#",
                'sage': 'http://sage.univ-nantes.fr/sage-voc#'
            },
            "@id": self._uri,
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
        """
        Returns an example of a SPARQL query associated with this graph, or None
        if no examples are available.
        """
        for query in self.example_queries:
            if query['@id'] == q_id:
                return query
        return None
