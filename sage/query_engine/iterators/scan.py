# scan.py
# Author: Thomas MINIER - MIT License 2017-2020
from datetime import datetime
from time import time
from typing import Dict, Optional

from sage.database.db_connector import DatabaseConnector
from sage.query_engine.exceptions import QuantumExhausted
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.utils import selection, vars_positions
from sage.query_engine.protobuf.iterators_pb2 import SavedScanIterator, TriplePattern
from sage.query_engine.protobuf.utils import pyDict_to_protoDict
from sage.query_engine.iterators.utils import find_in_mappings


class ScanIterator(PreemptableIterator):
    """A ScanIterator evaluates a triple pattern over a RDF graph.

    It can be used as the starting iterator in a pipeline of iterators.

    Args:
      * connector: The database connector that will be used to evaluate a triple pattern.
      * pattern: The evaluated triple pattern.
      * context: Information about the query execution.
      * current_mappings: The current mappings when the scan is performed.
      * mu: The last triple read when the preemption occured. This triple must be the next returned triple when the query is resumed.
      * last_read: An offset ID used to resume the ScanIterator.
      * as_of: Perform all reads against a consistent snapshot represented by a timestamp.
    """

    def __init__(self, connector: DatabaseConnector, pattern: Dict[str, str], context: dict, current_mappings: Optional[Dict[str, str]] = None, mu: Optional[Dict[str, str]] = None, last_read: Optional[str] = None, as_of: Optional[datetime] = None):
        super(ScanIterator, self).__init__()
        self._connector = connector
        self._pattern = pattern
        self._context = context
        self._variables = vars_positions(pattern['subject'], pattern['predicate'], pattern['object'])
        self._current_mappings = current_mappings
        self._mu = mu
        self._last_read = last_read
        self._start_timestamp = as_of
        # Create an iterator on the database
        if current_mappings is None:
            it, card = self._connector.search(pattern['subject'], pattern['predicate'], pattern['object'], last_read=last_read, as_of=as_of)
            self._source = it
            self._cardinality = card
        else:
            (s, p, o) = (find_in_mappings(pattern['subject'], current_mappings), find_in_mappings(pattern['predicate'], current_mappings), find_in_mappings(pattern['object'], current_mappings))
            it, card = self._connector.search(s, p, o, last_read=last_read, as_of=as_of)
            self._source = it
            self._cardinality = card

    def __len__(self) -> int:
        return self._cardinality

    def __repr__(self) -> str:
        return f"<ScanIterator ({self._pattern['subject']} {self._pattern['predicate']} {self._pattern['object']})>"

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "scan"

    def last_read(self) -> str:
        return self._source.last_read()

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._source.has_next() or self._mu is not None

    def next_stage(self, mappings: Dict[str, str]):
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        (s, p, o) = (find_in_mappings(self._pattern['subject'], mappings), find_in_mappings(self._pattern['predicate'], mappings), find_in_mappings(self._pattern['object'], mappings))
        it, card = self._connector.search(s, p, o, as_of=self._start_timestamp)
        self._current_mappings = mappings
        self._source = it
        self._cardinality = card
        self._last_read = None
        self._mu = None

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        if self._mu is not None:
            triple = self._mu
            self._mu = None
            return triple
        elif not self.has_next():
            return None
        else:
            triple = self._source.next()
            if triple is not None:
                triple = selection(triple, self._variables)
            timestamp = (time() - self._context['start_timestamp']) * 1000
            if self._context['quantum'] <= timestamp:
                self._mu = triple
                raise QuantumExhausted()
            else:
                return triple

    def save(self) -> SavedScanIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_scan = SavedScanIterator()
        triple = TriplePattern()
        triple.subject = self._pattern['subject']
        triple.predicate = self._pattern['predicate']
        triple.object = self._pattern['object']
        triple.graph = self._pattern['graph']
        saved_scan.pattern.CopyFrom(triple)
        if self._current_mappings is not None:
            pyDict_to_protoDict(self._current_mappings, saved_scan.muc)
        saved_scan.last_read = self._source.last_read()
        if self._start_timestamp is not None:
            saved_scan.timestamp = self._start_timestamp.isoformat()
        if self._mu is not None:
            pyDict_to_protoDict(self._mu, saved_scan.mu)
        return saved_scan
