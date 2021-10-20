# scan.py
# Author: Thomas MINIER - MIT License 2017-2020
from time import time
from datetime import datetime
from typing import Dict, Optional, Set, Any

from sage.database.backends.db_connector import DatabaseConnector
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
      * current_mappings: The current mappings when the scan is performed.
      * mu: The last triple read when the preemption occured. This triple must be the next returned triple when the query is resumed.
      * last_read: An offset ID used to resume the ScanIterator.
      * as_of: Perform all reads against a consistent snapshot represented by a timestamp.
    """

    def __init__(
        self, connector: DatabaseConnector, pattern: Dict[str, str],
        cumulative_cardinality: int = 0, pattern_cardinality: int = -1,
        pattern_produced: int = 0, produced: int = 0, stages: int = 0,
        current_mappings: Optional[Dict[str, str]] = None,
        mu: Optional[Dict[str, str]] = None,
        last_read: Optional[str] = None,
        as_of: Optional[datetime] = None
    ):
        super(ScanIterator, self).__init__()
        self._connector = connector
        self._pattern = pattern
        self._pattern_variables = vars_positions(
            pattern['subject'], pattern['predicate'], pattern['object']
        )
        self._current_mappings = current_mappings
        self._mu = mu
        self._last_read = last_read
        self._start_timestamp = as_of
        # create an iterator on the database
        if current_mappings is None:
            (s, p, o) = (pattern['subject'], pattern['predicate'], pattern['object'])
            self._source, card = self._connector.search(s, p, o, last_read=last_read, as_of=as_of)
        else:
            (s, p, o) = (
                find_in_mappings(pattern['subject'], current_mappings),
                find_in_mappings(pattern['predicate'], current_mappings),
                find_in_mappings(pattern['object'], current_mappings)
            )
            self._source, card = self._connector.search(s, p, o, last_read=last_read, as_of=as_of)
        self._cardinality = card
        self._cumulative_cardinality = cumulative_cardinality
        if pattern_cardinality < 0:
            self._pattern_cardinality = self._cardinality
        else:
            self._pattern_cardinality = pattern_cardinality
        self._pattern_produced = pattern_produced
        self._produced = produced
        self._stages = stages

    def __len__(self) -> int:
        return self._cardinality

    def __repr__(self) -> str:
        return f"<ScanIterator ({self._pattern['subject']} {self._pattern['predicate']} {self._pattern['object']})>"

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "scan"

    def explain(self, height: int = 0, step: int = 3) -> None:
        """Print a description of the iterator"""
        prefix = ''
        if height > step:
            prefix = ('|' + (' ' * (step - 1))) * (int(height / step) - 1)
        prefix += ('|' + ('-' * (step - 1)))
        subject = self._pattern['subject']
        predicate = self._pattern['predicate']
        object = self._pattern['object']
        print(f'{prefix}ScanIterator <({subject} {predicate} {object})>')

    def variables(self) -> Set[str]:
        vars = set()
        if self._pattern['subject'].startswith('?'):
            vars.add(self._pattern['subject'])
        if self._pattern['predicate'].startswith('?'):
            vars.add(self._pattern['predicate'])
        if self._pattern['object'].startswith('?'):
            vars.add(self._pattern['object'])
        return vars

    def last_read(self) -> str:
        return self._source.last_read()

    def next_stage(self, mappings: Dict[str, str]):
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        (s, p, o) = (
            find_in_mappings(self._pattern['subject'], mappings),
            find_in_mappings(self._pattern['predicate'], mappings),
            find_in_mappings(self._pattern['object'], mappings)
        )
        it, card = self._connector.search(s, p, o, as_of=self._start_timestamp)
        self._current_mappings = mappings
        self._source = it
        self._last_read = None
        self._mu = None
        self._cardinality = card
        self._cumulative_cardinality += self._cardinality
        self._produced = 0
        self._stages += 1

    async def next(self, context: Dict[str, Any] = {}) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        while self._mu is None:
            mappings = self._source.next()
            if mappings is None:
                return None
            (subject, predicate, object, insert_t, delete_t) = mappings
            if (insert_t is None) or (insert_t <= self._start_timestamp and self._start_timestamp < delete_t):
                self._mu = selection((subject, predicate, object), self._pattern_variables)
            if 'quota' in context and 'start_timestamp' in context:
                execution_time = (time() - context['start_timestamp']) * 1000
                if execution_time > context['quota']:
                    raise QuantumExhausted()
        self._produced += 1
        self._pattern_produced += 1
        if self._current_mappings is not None:
            mappings = {**self._current_mappings, **self._mu}
        else:
            mappings = self._mu
        self._mu = None
        return mappings

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
        last_read = self._source.last_read()
        if last_read is not None:
            saved_scan.last_read = last_read
        if self._start_timestamp is not None:
            saved_scan.timestamp = self._start_timestamp.isoformat()
        if self._mu is not None:
            pyDict_to_protoDict(self._mu, saved_scan.mu)
        saved_scan.cardinality = self._cardinality
        saved_scan.cumulative_cardinality = self._cumulative_cardinality
        saved_scan.pattern_cardinality = self._pattern_cardinality
        saved_scan.pattern_produced = self._pattern_produced
        saved_scan.produced = self._produced
        saved_scan.stages = self._stages
        return saved_scan
