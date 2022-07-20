from typing import Optional, Set, Tuple
from datetime import datetime
from time import time

from sage.database.core.dataset import Dataset
from sage.database.backends.db_iterator import DBIterator
from sage.query_engine.types import QueryContext, Mappings, TriplePattern
from sage.query_engine.exceptions import QuantumExhausted
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.utils import EmptyIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedScanIterator, SavedTriplePattern
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class ScanIterator(PreemptableIterator):
    """
    A ScanIterator evaluates a triple pattern over an RDF graph.

    This iterator generates all solutions that match a triple pattern.

    Parameters
    ----------
    pattern: TriplePattern
        The triple pattern to evaluate.
    muc: None | Mappings - (default = None)
        The current state of variables.
    mu: None | Mappings - (default = None)
        The last triple read when the preemption occured. Note that this triple
        has not been produced by the iterator, and must be the next to be produce.
    last_read: None | str - (default = None)
        An offset ID used to resume the ScanIterator.
    as_of: None | datetime - (default = None)
        A timestamp used to read the database in a consistent snapshot (MVCC).
    """

    def __init__(
        self, pattern: TriplePattern, muc: Optional[Mappings] = None,
        mu: Optional[Mappings] = None, last_read: Optional[str] = None,
        as_of: Optional[datetime] = None
    ):
        super(ScanIterator, self).__init__("scan")
        self._dataset = Dataset()
        self._pattern = pattern
        self._muc = muc
        self._mu = mu
        self._last_read = last_read
        self._timestamp = as_of
        self._source, self._card = self.__create_iterator__(muc, last_read=last_read)

    @property
    def vars(self) -> Set[str]:
        variables = set()
        if self.subject.startswith("?"):
            variables.add(self.subject)
        if self.predicate.startswith("?"):
            variables.add(self.predicate)
        if self.object.startswith("?"):
            variables.add(self.object)
        return variables

    @property
    def subject(self) -> str:
        return self._pattern["subject"]

    @property
    def predicate(self) -> str:
        return self._pattern["predicate"]

    @property
    def object(self) -> str:
        return self._pattern["object"]

    @property
    def graph(self) -> str:
        return self._pattern["graph"]

    @property
    def cardinality(self) -> int:
        return self._card

    @property
    def timestamp(self) -> Optional[datetime]:
        return self._timestamp

    @property
    def last_read(self) -> Optional[str]:
        return self._source.last_read()

    def __create_iterator__(
        self, muc: Optional[Mappings], last_read: Optional[str] = None
    ) -> Tuple[DBIterator, int]:
        """
        Creates an iterator on the database to evaluate a triple pattern,
        according to the current state of variables.

        Parameters
        ----------
        muc: None | Dict[str, str]
            The current state of variables.
        last_read: None | str
            An offset ID used to start reading the database from the last read
            triple.

        Returns
        -------
        Tuple[DBIterator, int]
            Returns an iterator on the database, and an estimate of the
            cardinality of the triple pattern.
        """
        if not self._dataset.has_graph(self.graph):
            return EmptyIterator(), 0
        if muc is None:
            (s, p, o) = (self.subject, self.predicate, self.object)
        else:
            (s, p, o) = (
                muc[self.subject] if self.subject in muc else self.subject,
                muc[self.predicate] if self.predicate in muc else self.predicate,
                muc[self.object] if self.object in muc else self.object)
        graph = self._dataset.get_graph(self.graph)
        return graph.search(s, p, o, last_read=last_read, as_of=self.timestamp)

    def __create_mappings__(self, triple: Tuple[str, str, str]) -> Mappings:
        """
        Transforms an RDF triple into a set of mappings according to the triple
        pattern of the ScanIterator.

        Parameters
        ----------
        triple: Tuple[str, str, str]
            RDF triple to transform into a dictionary of mappings.

        Returns
        -------
        Dict[str, str]
            A set of mappings built from the given triple.

        Example
        -------
        >>> triple = (":Ann", "foaf:knows", ":Bob")
        >>> self.subject = ?s
        >>> self.predicate = foaf:knows
        >>> self.object = ?knows
        >>> self.__create_mappings__(triple)
            { "?s": ":Ann", "?knows": ":Bob" }
        """
        mu = dict()
        if self.subject.startswith("?"):
            mu[self.subject] = triple[0]
        if self.predicate.startswith("?"):
            mu[self.predicate] = triple[1]
        if self.object.startswith("?"):
            mu[self.object] = triple[2]
        return mu

    def next_stage(self, muc: Mappings) -> None:
        """
        Applies the current mappings to the next triple pattern in the pipeline
        of iterators.

        Parameters
        ----------
        muc : Dict[str, str]
            Mappings {?v1: ..., ..., ?vk: ...} computed so far.

        Returns
        -------
        None
        """
        self._muc = muc
        self._mu = None
        self._source, self._card = self.__create_iterator__(muc)

    async def next(self, context: QueryContext = {}) -> Optional[Mappings]:
        """
        Generates the next item from the iterator, following the iterator
        protocol.

        In the current implementation, the ScanIterator is a main actor of
        the preemption model. Before generating the next item, it ensures that
        the quantum is not exhausted. If the quantum is exhausted, a
        QuantumExhausted exception is raised.

        Parameters
        ----------
        context: QueryContext
            Global variables specific to the execution of the query.

        Returns
        -------
        None | Dict[str, str]
            The next item produced by the iterator, or None if all items have
            been produced.

        Raises
        ------
        QuantumExhausted
        """
        start_time = context.get("timestamp")
        quota = context.setdefault("quota", self._dataset.quota)
        while self._mu is None:
            triple = self._source.next()
            if triple is None:  # iterator is exhausted
                return None
            (subject, predicate, object, insert_t, delete_t) = triple
            if (insert_t is None) or (insert_t <= self.timestamp and self.timestamp < delete_t):  # to complain with the MVCC protocol
                self._mu = self.__create_mappings__((subject, predicate, object))
            if ((time() - start_time) * 1000) > quota:  # quantum is exhausted
                raise QuantumExhausted()
        mu = self._mu
        self._mu = None  # None to indicate that we have to retrieve a new triple
        if self._muc is not None:
            return {**self._muc, **mu}
        return mu

    def pop(self, context: QueryContext = {}) -> Optional[Mappings]:
        """
        Generates the next item from the iterator, following the iterator
        protocol.

        This method does not generate any scan on the database. It is used to
        clear internal data structures such as the buffer of the TOPKIterator.

        Parameters
        ----------
        context: QueryContext
            Global variables specific to the execution of the query.

        Returns
        -------
        None | Dict[str, str]
            The next item produced by the iterator, or None if all internal
            data structures are empty.
        """
        return None

    def save(self) -> SavedScanIterator:
        """
        Saves and serializes the iterator as a Protobuf message.

        Returns
        -------
        SavedScanIterator
            The state of the ScanIterator as a Protobuf message.
        """
        saved_scan = SavedScanIterator()

        saved_triple_pattern = SavedTriplePattern()
        saved_triple_pattern.subject = self.subject
        saved_triple_pattern.predicate = self.predicate
        saved_triple_pattern.object = self.object
        saved_triple_pattern.graph = self.graph
        saved_scan.pattern.CopyFrom(saved_triple_pattern)

        saved_scan.cardinality = self.cardinality

        if self.last_read is not None:
            saved_scan.last_read = self.last_read

        if self.timestamp is not None:
            saved_scan.timestamp = self.timestamp.isoformat()

        if self._muc is not None:
            pyDict_to_protoDict(self._muc, saved_scan.muc)

        if self._mu is not None:
            pyDict_to_protoDict(self._mu, saved_scan.mu)

        return saved_scan

    def explain(self, depth: int = 0) -> str:
        """
        Returns a textual representation of the pipeline of iterators.

        Parameters
        ----------
        depth: int - (default = 0)
            Indicates the current depth in the pipeline of iterators. It is
            used to return a pretty printed representation.

        Returns
        -------
        str
            Textual representation of the pipeline of iterators.
        """
        prefix = ("| " * depth) + "|"
        description = (
            f"{prefix}\n{prefix}-ScanIterator <PV=({self.vars}), "
            f"CARD=({self.cardinality}), S=({self.subject}), "
            f"P=({self.predicate}), O=({self.object})>\n")
        return description

    def stringify(self, level: int = 1) -> str:
        """
        Transforms a pipeline of iterators into a SPARQL query.

        Parameters
        ----------
        level: int - (default = 1)
            Indicates the level of nesting of the group. It is used to pretty
            print the SPARQL query.

        Returns
        -------
        str
            A SPARQL query.
        """
        prefix = " " * 2 * level
        subject = self.subject
        if subject.startswith("http"):
            subject = f"<{subject}>"
        predicate = self.predicate
        if predicate.startswith("http"):
            predicate = f"<{predicate}>"
        object = self.object
        if object.startswith("http"):
            object = f"<{object}>"
        return f"{prefix}{subject} {predicate} {object} .\n"
