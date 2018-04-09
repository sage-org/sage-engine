# nlj.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.utils import apply_bindings, NotYet


class NestedLoopJoinIterator(object):
    """A NestedLoopJoinIterator is an iterator over a nested loop join between an iterator, which yields mappings, and a triple pattern.
    It is implemented as a Snapshot Operator, so it can be halted and resumed at any time.

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
        return self._source.has_next() or self._currentBinding is not None or self._currentIter is not None

    def _initInnerLoop(self, triple, mappings, offset=0):
        (s, p, o) = (apply_bindings(triple['subject'], mappings), apply_bindings(triple['predicate'], mappings), apply_bindings(triple['object'], mappings))
        iterator, c = self._hdtDocument.search_triples(s, p, o, offset=offset)
        return ScanIterator(iterator, triple, 'anonymous')

    def next(self):
        if not self.has_next():
            raise StopIteration()

        # read another set of mappings from the outer loop and init a new innner loop
        if self._currentBinding is None:
            self._currentBinding = next(self._source, None)

            if self._currentBinding is NotYet:
                self._currentBinding = None
                return NotYet
            elif self._currentBinding is None and self._currentIter is None:
                raise StopIteration()

        # initalize a new inner loop
        if self._currentIter is None:
            self._currentIter = self._initInnerLoop(self._innerTriple, self._currentBinding)
        try:
            # read from the inner loop
            value = next(self._currentIter)
            self._iterOffset += 1
            return {**self._currentBinding, **value}
        except StopIteration as e:
            if not self.has_next():
                raise StopIteration()
            # try to schedule a new inner loop using remaining tickets
            self._currentBinding = None
            self._iterOffset = 0
            self._currentIter = None
            return NotYet

    def export(self):
        """Export a snaphost of the operator"""
        return {
            'type': 'NestedLoopJoinIterator',
            'source': self._source.export(),
            'inner': self._innerTriple,
            'mappings': self.currentBinding,
            'offset': self._iterOffset
        }
