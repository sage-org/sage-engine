# projection.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.preemptable_iterator import PreemptableIterator
from query_engine.protobuf.iterators_pb2 import SavedProjectionIterator
from query_engine.iterators.utils import IteratorExhausted


class ProjectionIterator(PreemptableIterator):
    """A ProjectionIterator performa projection over solution mappings"""
    def __init__(self, source, values=None):
        super(ProjectionIterator, self).__init__()
        self._source = source
        self._values = values

    def __repr__(self):
        return '<ProjectionIterator SELECT %s FROM { %s }>' % (self._values, self._source)

    def has_next(self):
        return self._source.has_next()

    async def next(self):
        """
        Get the next item from the iterator, reading from the left source and then the right source
        """
        if not self.has_next():
            raise IteratorExhausted()
        value = await self._source.next()
        if self._values is None:
            return value
        return {k: v for k, v in value.items() if k in self._values}

    def save(self):
        """Save and serialize the iterator as a machine-readable format"""
        savedOp = SavedProjectionIterator()
        savedOp.values.extend(self._values)
        source = self._source.save()
        if type(self._source).__name__ == 'ScanIterator':
            savedOp.scan_source.CopyFrom(source)
        elif type(self._source).__name__ == 'NestedLoopJoinIterator' or type(self._source).__name__ == 'LeftNLJIterator':
            savedOp.nlj_source.CopyFrom(source)
        elif type(self._source).__name__ == 'BagUnionIterator':
            savedOp.union_source.CopyFrom(source)
        else:
            raise Exception("Unknown source type for ProjectionIterator")
        return savedOp
