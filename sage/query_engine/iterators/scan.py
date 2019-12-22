# scan.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Optional

from sage.database.db_iterator import DBIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.utils import (IteratorExhausted, selection,
                                               vars_positions)
from sage.query_engine.protobuf.iterators_pb2 import (SavedScanIterator,
                                                      TriplePattern)


class ScanIterator(PreemptableIterator):
    """A ScanIterator evaluates a triple pattern over a RDF dataset. It yields solution mappings created from RDF triples matching the triple pattern.

    Constructor args:
        - source - An HDT iterator that yields RDF triple in string format.
        - triple - The triple pattern corresponding to the source iterator.
        - cardinality - The cardinality of the triple pattern.
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
        return "scan"

    @property
    def nb_reads(self) -> int:
        return self._source.nb_reads

    @property
    def offset(self) -> int:
        return self._source.offset

    def last_read(self) -> str:
        """Return the index ID of the last element read"""
        return self._source.last_read()

    def has_next(self) -> bool:
        return self._source.has_next()

    async def next(self) -> Optional[Dict[str, str]]:
        """Scan the relation and return the next set of solution mappings"""
        if not self.has_next():
            raise IteratorExhausted()
        triple = next(self._source)
        if triple is None:
            return None
        return selection(triple, self._variables)

    def save(self) -> SavedScanIterator:
        """Save the operator using protocol buffers"""
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
