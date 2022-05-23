# scan.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Optional, Set, Any, Tuple
from time import time
from datetime import datetime

from sage.database.backends.db_connector import DatabaseConnector
from sage.database.backends.db_iterator import DBIterator
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
      * mu: The last triple read when the preemption occured. This triple must be the next triple to return when the query is resumed.
      * last_read: An offset ID used to resume the RandomScanIterator.
      * as_of: Perform all reads against a consistent snapshot represented by a timestamp.
      * read: Number of triples read from the instantiated triple pattern. Used to compute the coverage of the query.
      * size: Cumulative cardinality of all the triple patterns once instantiated. Used to compute the refined-cost of the query.
      * produced: Number of triples produced from all the triple patterns once instantiated. Used to compute the refined-cost of the query.
      * stages: Number of times the triple pattern has been instantiated. Used to compute the refined-cost of the query.
    """

    def __init__(
        self, connector: DatabaseConnector,
        pattern: Dict[str, str],
        current_mappings: Optional[Dict[str, str]] = None,
        mu: Optional[Dict[str, str]] = None,
        last_read: Optional[str] = None,
        as_of: Optional[datetime] = None,
        produced: int = 0,
        cumulative_produced: int = 0,
        cumulative_cardinality: int = 0,
        stages: int = 0
    ):
        super(ScanIterator, self).__init__()
        self._connector = connector
        self._pattern = pattern
        self._mask = vars_positions(pattern['subject'], pattern['predicate'], pattern['object'])
        self._source, self._cardinality = self.create_iterator(mappings=current_mappings, last_read=last_read, as_of=as_of)
        self._current_mappings = current_mappings
        self._mu = mu
        self._last_read = last_read
        self._timestamp = as_of
        self._produced = produced
        self._cumulative_produced = cumulative_produced
        self._cumulative_cardinality = cumulative_cardinality
        self._stages = stages
        self._coverage = 0.0
        self._cost = 0

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
        print(f'{prefix}ScanIterator (cost={self._cost}) (coverage={self._coverage}) <({subject} {predicate} {object})>')

    def variables(self, include_values: bool = False) -> Set[str]:
        return set([v for v in self._mask if v is not None])

    def instantiate(self, mappings: Optional[Dict[str, str]] = None) -> Tuple[str, str, str]:
        if mappings is None:
            return (self._pattern['subject'], self._pattern['predicate'], self._pattern['object'])
        return (
            find_in_mappings(self._pattern['subject'], mappings),
            find_in_mappings(self._pattern['predicate'], mappings),
            find_in_mappings(self._pattern['object'], mappings))

    def create_iterator(self, mappings: Optional[Dict[str, str]] = None, last_read: Optional[str] = None, as_of: Optional[datetime] = None) -> Tuple[DBIterator, int]:
        (s, p, o) = self.instantiate(mappings=mappings)
        return self._connector.search(s, p, o, last_read=last_read, as_of=as_of)

    def last_read(self) -> str:
        return self._source.last_read()

    def next_stage(self, mappings: Dict[str, str], context: Dict[str, Any] = {}):
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        self._source, self._cardinality = self.create_iterator(mappings, as_of=self._timestamp)
        self._current_mappings = mappings
        self._mu = None
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
            if (insert_t is None) or (insert_t <= self._timestamp and self._timestamp < delete_t):
                self._mu = selection((subject, predicate, object), self._mask)
            execution_time = (time() - context.get('timestamp')) * 1000
            if execution_time > context.get('quota'):
                raise QuantumExhausted()
        if self._current_mappings is not None:
            mappings = {**self._current_mappings, **self._mu}
        else:
            mappings = self._mu
        self._mu = None
        self._produced += 1
        self._cumulative_produced += 1
        return mappings

    def update_coverage(self, context: Dict[str, Any] = {}) -> float:
        """Compute and update operators progression.

        This function assumes that only nested loop joins are used.

        Returns: The coverage of the query for the given plan.
        """
        context.setdefault('coverage__stop', False)
        context.setdefault('coverage__cardinalities', [])
        if self._produced == 0 or context['coverage__stop']:
            context['coverage__stop'] = True
            self._coverage = 0.0
            return 0.0
        cardinality = max(self._cardinality, self._produced)
        coverage = (self._produced - 1) / cardinality
        self._coverage = coverage
        for previous_table_cardinality in context['coverage__cardinalities']:
            coverage *= (1.0 / previous_table_cardinality)
        context['coverage__cardinalities'].append(cardinality)
        return coverage

    def update_cost(self, context: Dict[str, Any] = {}) -> float:
        """Compute and update operators cost.

        This function assumes that only nested loop joins are used.

        Returns: The cost of the query for the given plan.
        """
        context.setdefault('cost__cout', 1)
        _, self._cost = self.create_iterator()
        cardinality = max(self._cardinality, self._cumulative_cardinality)
        cardinality = max(cardinality, self._cumulative_produced)
        if self._cumulative_produced == 0:
            selectivity = cardinality
        else:
            stages = max(1, self._stages)
            selectivity = cardinality / stages
        context['cost__cout'] *= selectivity
        return context['cost__cout']

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
        last_read = self.last_read()
        if last_read is not None:
            saved_scan.last_read = last_read
        if self._timestamp is not None:
            saved_scan.timestamp = self._timestamp.isoformat()
        if self._mu is not None:
            pyDict_to_protoDict(self._mu, saved_scan.mu)
        saved_scan.produced = self._produced
        saved_scan.cumulative_produced = self._cumulative_produced
        saved_scan.cumulative_cardinality = self._cumulative_cardinality
        saved_scan.stages = self._stages
        saved_scan.coverage = self._coverage
        saved_scan.cost = self._cost
        return saved_scan
