# nlj.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.preemptable_iterator import PreemptableIterator
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.utils import apply_bindings, NotYet
from query_engine.protobuf.iterators_pb2 import TriplePattern, SavedNestedLoopJoinIterator
from query_engine.iterators.utils import IteratorExhausted
from asyncio import coroutine, shield


class NestedLoopJoinIterator(PreemptableIterator):
    """A NestedLoopJoinIterator is an iterator over a nested loop join between an iterator, which yields mappings, and a triple pattern.

    Constructor args:
        - source [ScanIterator|NestedLoopJoinIterator] - The outer relation of the join: an iterator that yields solution mappings.
        - innerTriple [TriplePattern] - The inner relation, i.e., a triple pattern.
        - hdtDocument [hdt.HDTDocument] - An HDT document scanned by inner loops.
        - currentBinding [Dict, default=None] - A set of solution mappings used to resume join processing.
        - iterOffset [integer, default=0] - An offset used to resume processing of an inner loop.
    """
    def __init__(self, source, innerTriple, hdtDocument, currentBinding=None, iterOffset=0):
        super(NestedLoopJoinIterator, self).__init__()
        self._source = source
        self._innerTriple = innerTriple
        self._currentBinding = currentBinding
        self._hdtDocument = hdtDocument
        self._iterOffset = iterOffset
        self._mu = None
        self._currentIter = None
        if self._currentBinding is not None:
            self._currentIter = self._initInnerLoop(self._innerTriple, self._currentBinding, offset=iterOffset)

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def __repr__(self):
        return "<NestedLoopJoinIterator (%s JOIN {%s %s %s})>" % (str(self._source), self._innerTriple['subject'], self._innerTriple['predicate'], self._innerTriple['object'])

    @property
    def currentBinding(self):
        return self._currentBinding

    @property
    def iterOffset(self):
        return self._iterOffset

    def has_next(self):
        return self._source.has_next() or self._currentIter is not None or self._currentIter.has_next()

    def _initInnerLoop(self, triple, mappings, offset=0):
        (s, p, o) = (apply_bindings(triple['subject'], mappings), apply_bindings(triple['predicate'], mappings), apply_bindings(triple['object'], mappings))
        if o.startswith('?'):
            o = ""
        iterator, card = self._hdtDocument.search_triples(s, p, o, offset=offset)
        if card == 0:
            return None
        return ScanIterator(iterator, triple, card)

    async def outerLoop(self):
        while self._currentIter is None or (not self._currentIter.has_next()):
            self._currentBinding = await self._source.next()
            if self._currentBinding is None:
                return None
            self._currentIter = self._initInnerLoop(self._innerTriple, self._currentBinding)

    async def innerLoop(self):
        """Execute one loop of the inner loop"""
        self._mu = await self._currentIter.next()
        return {**self._currentBinding, **self._mu}

    @coroutine
    async def next(self):
        """Get the next element from the join"""
        if not self.has_next():
            raise IteratorExhausted()
        while self._currentIter is None or (not self._currentIter.has_next()):
            self._currentBinding = await self._source.next()
            if self._currentBinding is None:
                return None
            self._currentIter = self._initInnerLoop(self._innerTriple, self._currentBinding)
        return await shield(self.innerLoop())

    def save(self):
        """Save the operator using protobuf"""
        savedJoin = SavedNestedLoopJoinIterator()
        if type(self._source).__name__ == 'ScanIterator':
            savedJoin.scan_source.CopyFrom(self._source.save())
        elif type(self._source).__name__ == 'NestedLoopJoinIterator':
            savedJoin.nlj_source.CopyFrom(self._source.save())
        inner = TriplePattern()
        inner.subject = self._innerTriple['subject']
        inner.predicate = self._innerTriple['predicate']
        inner.object = self._innerTriple['object']
        savedJoin.inner.CopyFrom(inner)
        if self._mu is not None:
            for key in self._mu:
                savedJoin.mu[key] = self._mu[key]
        # savedJoin.mu
        if self._currentBinding is not None:
            for key in self._currentBinding:
                savedJoin.muc[key] = self._mu[key]
        if self._currentIter is not None:
            savedJoin.offset = self._currentIter.offset + self._currentIter.nb_reads
        return savedJoin
