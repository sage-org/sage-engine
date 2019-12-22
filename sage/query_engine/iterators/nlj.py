# nlj.py
# Author: Thomas MINIER - MIT License 2017-2020
from datetime import datetime
from typing import Dict, Optional

from sage.database.core.graph import Graph
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.utils import (EmptyIterator,
                                               IteratorExhausted,
                                               apply_bindings, tuple_to_triple)
from sage.query_engine.primitives import PreemptiveLoop
from sage.query_engine.protobuf.iterators_pb2 import (SavedIndexJoinIterator,
                                                      TriplePattern)
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class IndexJoinIterator(PreemptableIterator):
    """A IndexJoinIterator implements an Index Join using the iterator paradigm.

    Args:
        - source :class:`.PreemptableIterator` - The outer relation of the join: an iterator that yields solution mappings.
        - innerTriple - The inner relation, i.e., a triple pattern.
        - graph - The document scanned by inner loops.
        - currentBinding - A set of solution mappings used to resume join processing.
        - last_read - An offset ID used to resume processing of an inner loop.
        - as_of - Perform all reads against a consistent snapshot represented by a timestamp.
    """

    def __init__(self, source: PreemptableIterator, innerTriple: Dict[str, str], graph: Graph, currentBinding: Optional[Dict[str, str]] = None, last_read: Optional[str] = None, as_of: Optional[datetime] = None):
        super(IndexJoinIterator, self).__init__()
        self._source = source
        self._innerTriple = innerTriple
        self._currentBinding = currentBinding
        self._graph = graph
        self._last_read = last_read
        self._start_timestamp = as_of
        self._currentIter = None
        if self._currentBinding is not None:
            self._currentIter = self._initInnerLoop(self._innerTriple, self._currentBinding, last_read=last_read)

    def __repr__(self) -> str:
        return f"<IndexJoinIterator ({self._source} JOIN {{ {self._innerTriple['subject']} {self._innerTriple['predicate']} {self._innerTriple['object']} }})>"

    def serialized_name(self) -> str:
        return "join"

    @property
    def currentBinding(self) -> Dict[str, str]:
        return self._currentBinding

    @property
    def iterOffset(self) -> str:
        return self._last_read

    def has_next(self) -> bool:
        return self._source.has_next() or (self._currentIter is not None and self._currentIter.has_next())

    def _initInnerLoop(self, triple: Dict[str, str], mappings: Optional[Dict[str, str]], last_read: Optional[str] = None) -> PreemptableIterator:
        if mappings is None:
            return EmptyIterator(triple)
        (s, p, o) = (apply_bindings(triple['subject'], mappings), apply_bindings(triple['predicate'], mappings), apply_bindings(triple['object'], mappings))
        iterator, card = self._graph.search(s, p, o, last_read=last_read, as_of=self._start_timestamp)
        if card == 0:
            return None
        return ScanIterator(iterator, tuple_to_triple(s, p, o), card)

    async def _innerLoop(self) -> Optional[Dict[str, str]]:
        """Execute one loop of the inner loop"""
        mu = await self._currentIter.next()
        if mu is None:
            return None
        return {**self._currentBinding, **mu}

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next element from the join"""
        if not self.has_next():
            raise IteratorExhausted()
        with PreemptiveLoop() as loop:
            while self._currentIter is None or (not self._currentIter.has_next()):
                self._currentBinding = await self._source.next()
                self._currentIter = self._initInnerLoop(self._innerTriple, self._currentBinding)
                await loop.tick()
        return await self._innerLoop()

    def save(self) -> SavedIndexJoinIterator:
        """Save the operator using protobuf"""
        saved_join = SavedIndexJoinIterator()
        # save source operator
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_join, source_field).CopyFrom(self._source.save())
        # save inner join
        inner = TriplePattern()
        inner.subject = self._innerTriple['subject']
        inner.predicate = self._innerTriple['predicate']
        inner.object = self._innerTriple['object']
        inner.graph = self._innerTriple['graph']
        saved_join.inner.CopyFrom(inner)
        if self._currentBinding is not None:
            pyDict_to_protoDict(self._currentBinding, saved_join.muc)
        if self._currentIter is not None:
            saved_join.last_read = self._currentIter.last_read()
        if self._start_timestamp is not None:
            saved_join.timestamp = self._start_timestamp.isoformat()
        return saved_join
