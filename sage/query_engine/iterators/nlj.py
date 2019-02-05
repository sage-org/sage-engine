# nlj.py
# Author: Thomas MINIER - MIT License 2017-2018
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.utils import apply_bindings, tuple_to_triple
from sage.query_engine.protobuf.iterators_pb2 import TriplePattern, SavedIndexJoinIterator
from sage.query_engine.protobuf.utils import pyDict_to_protoDict
from sage.query_engine.iterators.utils import IteratorExhausted
from asyncio import sleep


class IndexJoinIterator(PreemptableIterator):
    """A IndexJoinIterator implements an Index Join using the iterator paradigm.

    Args:
        - source :class:`.ScanIterator` | :class:`.IndexJoinIterator` - The outer relation of the join: an iterator that yields solution mappings.
        - innerTriple ``dict``- The inner relation, i.e., a triple pattern.
        - hdtDocument :class:`.DatabaseConnector` - The document scanned by inner loops.
        - currentBinding ``dict=None`` - A set of solution mappings used to resume join processing.
        - iterOffset ``[string=None]`` - An offset ID used to resume processing of an inner loop.
    """

    def __init__(self, source, innerTriple, hdtDocument, currentBinding=None, iterOffset=None):
        super(IndexJoinIterator, self).__init__()
        self._source = source
        self._innerTriple = innerTriple
        self._currentBinding = currentBinding
        self._hdtDocument = hdtDocument
        self._iterOffset = iterOffset
        self._currentIter = None
        if self._currentBinding is not None:
            self._currentIter = self._initInnerLoop(self._innerTriple, self._currentBinding, last_read=iterOffset)

    def __repr__(self):
        return "<IndexJoinIterator (%s JOIN {%s %s %s})>" % (str(self._source), self._innerTriple['subject'], self._innerTriple['predicate'], self._innerTriple['object'])

    def serialized_name(self):
        return "join"

    @property
    def currentBinding(self):
        return self._currentBinding

    @property
    def iterOffset(self):
        return self._iterOffset

    def has_next(self):
        return self._source.has_next() or (self._currentIter is not None and self._currentIter.has_next())

    def _initInnerLoop(self, triple, mappings, last_read=None):
        (s, p, o) = (apply_bindings(triple['subject'], mappings), apply_bindings(triple['predicate'], mappings), apply_bindings(triple['object'], mappings))
        iterator, card = self._hdtDocument.search(s, p, o, last_read=last_read)
        if card == 0:
            return None
        return ScanIterator(iterator, tuple_to_triple(s, p, o), card)

    async def _innerLoop(self):
        """Execute one loop of the inner loop"""
        mu = await self._currentIter.next()
        return {**self._currentBinding, **mu}

    async def next(self):
        """Get the next element from the join"""
        if not self.has_next():
            raise IteratorExhausted()
        cpt = 0
        while self._currentIter is None or (not self._currentIter.has_next()):
            cpt += 1
            self._currentBinding = await self._source.next()
            self._currentIter = self._initInnerLoop(self._innerTriple, self._currentBinding)
            # WARNING: await sleep(0) cost a lot, so we only trigger it every 50 cycle.
            # additionnaly, there may be other call to await sleep(0) in index join in the pipeline.
            if cpt > 50:
                cpt = 0
                await sleep(0)
        return await self._innerLoop()

    def save(self):
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
        return saved_join
