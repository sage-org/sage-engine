# scan.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Optional

from sage.database.db_iterator import DBIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.utils import selection, vars_positions
from sage.query_engine.protobuf.iterators_pb2 import (SavedScanIterator,
                                                      TriplePattern)


class ScanIterator(PreemptableIterator):
    """A ScanIterator evaluates a triple pattern over a RDF graph.

    It can be used as the starting iterator in a pipeline of iterators.

    Args:
      * source: A DBIterator that yields RDF triple.
      * triple: The triple pattern corresponding to the source iterator.
      * cardinality: The cardinality of the triple pattern.
    """

    def __init__(self, source: DBIterator, triple: Dict[str, str], cardinality: int = 0):
        super(ScanIterator, self).__init__()
        self._source = source
        self._triple = triple
        self._variables = vars_positions(triple['subject'], triple['predicate'], triple['object'])
        self._cardinality = cardinality

    def __len__(self) -> int:
        return self._cardinality

    def __repr__(self) -> str:
        return f"<ScanIterator ({self._triple['subject']} {self._triple['predicate']} {self._triple['object']})>"

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "scan"
    
    def last_read(self) -> str:
        return self._source.last_read()

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._source.has_next()

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must 
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        if not self.has_next():
            raise StopAsyncIteration()
        triple = next(self._source)
        if triple is None:
            return None
        return selection(triple, self._variables)

    def save(self) -> SavedScanIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_scan = SavedScanIterator()
        triple = TriplePattern()
        triple.subject = self._triple['subject']
        triple.predicate = self._triple['predicate']
        triple.object = self._triple['object']
        triple.graph = self._triple['graph']
        saved_scan.triple.CopyFrom(triple)
        saved_scan.last_read = self._source.last_read()
        saved_scan.cardinality = self._cardinality
        return saved_scan
