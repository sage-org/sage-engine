from datetime import datetime
from typing import Dict, List, Optional

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator


class IfExistsIterator(PreemptableIterator):
    """
    An IfExistsOperator checks if all N-Quads in a set exist in the database.

    It is used to provide the "serializability per solution group" consistency
    level.

    Parameters
    ----------
    quads: List[Dict[str, str]]
        RDF quads to validate.
    dataset: Dataset
        RDF dataset.
    start_time: datetime
        A timestamp used to perform all reads against a consistent version of
        the dataset.
    """

    def __init__(self, quads: List[Dict[str, str]], dataset: Dataset, start_time: datetime):
        super(IfExistsIterator, self).__init__("ifexists")
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

    def has_next(self) -> bool:
        """Return True if the iterator has more quads to validate"""
        return (not self._found_missing) and len(self._quads) > 0

    def next_stage(self, mappings: Dict[str, str]) -> None:
        """
        Applies the current mappings to the next triple pattern in the pipeline
        of iterators.

        Parameters
        ----------
        muc : Dict[str, str]
            Mappings {?v1: ..., ..., ?vk: ...} computed so far.

        Returns
        -------
        None
        """
        pass

    async def next(self, context: Dict[str, str]) -> Optional[Dict[str, str]]:
        """
        Validates the next quad using the RDF dataset.

        This function works in an iterator fashion, so it can be used in a
        pipeline of iterators.

        Returns
        None
        """
        if not self._found_missing and len(self._quads) > 0:
            return None
        triple = self._quads.pop()
        if self._dataset.has_graph(triple["graph"]):
            try:
                s, p, o = triple["subject"], triple["predicate"], triple["object"]
                iterator, _ = self._dataset.get_graph(triple['graph']).search(
                    s, p, o, as_of=self._start_time)
                self._found_missing = not await iterator.next() is None
            except Exception:
                self._found_missing = True
        else:
            self._found_missing = True
        return None

    def save(self) -> str:
        """Useless for this operator, as it MUST run completely inside a quantum"""
        return ''
