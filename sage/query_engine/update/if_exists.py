# if_exists.py
# Author: Thomas MINIER - MIT License 2017-2020
from datetime import datetime
from typing import Dict, List, Optional

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator


class IfExistsOperator(PreemptableIterator):
    """An IfExistsOperator checks if all N-Quads in a set exist in the database.
    
    It is used to provide the "serializability per solution group" consistency level.

    Args:
      * quads: RDF quads to validate.
      * dataset: RDF dataset.
      * start_time: A timestamp used to perform all reads against a consistent version of the dataset.
    """

    def __init__(self, quads: List[Dict[str, str]], dataset: Dataset, start_time: datetime):
        super(IfExistsOperator, self).__init__()
        self._quads = quads
        self._dataset = dataset
        self._found_missing = False
        self._start_time = start_time

    def __repr__(self) -> str:
        return f"<IfExistsOperator quads={self._quads}>"

    @property
    def missing_nquads(self) -> bool:
        """Returns True if, at the time of invocation, at least one n-quad was not found in the RDF dataset."""
        return self._found_missing

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "ifexists"

    def has_next(self) -> bool:
        """Return True if the iterator has more quads to validate"""
        return (not self._found_missing) and len(self._quads) > 0

    async def next(self) -> Optional[Dict[str, str]]:
        """Validate the next quad using the RDF dataset.

        This function works in an iterator fashion, so it can be used in a pipeline of iterators.
        It may also contains `non interruptible` clauses which must 
        be atomically evaluated before preemption occurs.

        Returns: always `None` 

        Throws: `StopAsyncIteration` if the iterator has no more quads to validate.
        """
        if not self.has_next():
            raise StopAsyncIteration()
        triple = self._quads.pop()
        if self._dataset.has_graph(triple['graph']):
            try:
                s, p, o = triple['subject'], triple['predicate'], triple['object']
                iterator, _ = self._dataset.get_graph(triple['graph']).search(s, p, o, as_of=self._start_time)
                self._found_missing = not iterator.has_next()
            except Exception:
                self._found_missing = True
        else:
            self._found_missing = True
        return None

    def save(self) -> str:
        """Useless for this operator, as it MUST run completely inside a quantum"""
        return ''
