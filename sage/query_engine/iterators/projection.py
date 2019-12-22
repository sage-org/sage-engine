# projection.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, List, Optional

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.utils import IteratorExhausted
from sage.query_engine.protobuf.iterators_pb2 import SavedProjectionIterator


class ProjectionIterator(PreemptableIterator):
    """A ProjectionIterator performa projection over solution mappings"""

    def __init__(self, source: PreemptableIterator, values: List[str] = None):
        super(ProjectionIterator, self).__init__()
        self._source = source
        self._values = values

    def __repr__(self) -> str:
        return f"<ProjectionIterator SELECT {self._values} FROM {self._source}>"

    def serialized_name(self) -> str:
        return "proj"

    def has_next(self) -> bool:
        return self._source.has_next()

    async def next(self) -> Optional[Dict[str, str]]:
        """
        Get the next item from the iterator, reading from the left source and then the right source
        """
        if not self.has_next():
            raise IteratorExhausted()
        mappings = await self._source.next()
        if mappings is None:
            return None
        elif self._values is None:
            return mappings
        return {k: v for k, v in mappings.items() if k in self._values}

    def save(self) -> SavedProjectionIterator:
        """Save and serialize the iterator as a machine-readable format"""
        saved_proj = SavedProjectionIterator()
        saved_proj.values.extend(self._values)
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_proj, source_field).CopyFrom(self._source.save())
        return saved_proj
