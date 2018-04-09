# bind_join.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.utils import apply_bindings, vars_positions, selection


class BindJoinIterator(object):
    """docstring for BindJoinIterator."""
    def __init__(self, triple, bindings, hdtDocument, bindingsOffset=0, iterOffset=0):
        super(BindJoinIterator, self).__init__()
        self._triple = triple
        self._bindings = bindings[bindingsOffset:]
        self._variables = vars_positions(triple[0], triple[1], triple[2])
        self._hdtDocument = hdtDocument
        self._bindingsOffset = bindingsOffset
        self._iterOffset = iterOffset
        self._currentBinding = self._bindings.pop(0)
        self._currentIter = self._buildTripleIterator(self._triple, self._currentBinding, offset=iterOffset)

    @property
    def bindingsOffset(self):
        return self._bindingsOffset

    @property
    def iterOffset(self):
        return self._iterOffset

    @property
    def is_closed(self):
        return len(self._bindings) == 0 and self._currentIter is None

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def _buildTripleIterator(self, triple, mappings, offset=0):
        (s, p, o) = (apply_bindings(triple[0], mappings), apply_bindings(triple[1], mappings), apply_bindings(triple[2], mappings))
        iterator, c = self._hdtDocument.search_triples(s, p, o, offset=offset)
        return iterator

    def next(self):
        if len(self._bindings) == 0 and self._currentIter is None:
            raise StopIteration()
        try:
            rdf_triple = next(self._currentIter)
            self._iterOffset += 1
            return {**self._currentBinding, **dict(selection(rdf_triple, self._variables))}
        except StopIteration as e:
            self._iterOffset = 0
            self._bindingsOffset += 1
            if len(self._bindings) > 0:
                self._currentBinding = self._bindings.pop(0)
                self._currentIter = self._buildTripleIterator(self._triple, self._currentBinding)
            else:
                self._currentIter = None
            return self.next()
