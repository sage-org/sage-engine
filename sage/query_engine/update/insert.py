# insert.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, List, Optional, Tuple

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedInsertData
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class InsertOperator(PreemptableIterator):
    """A DeleteOperator inserts RDF triples into a RDF dataset.

    Args:
      * quads: List of RDF quads to insert into the RDF dataset.
      * dataset: RDF dataset
    """

    def __init__(self, quads: List[Tuple[str, str, str, str]], dataset: Dataset):
        super(InsertOperator, self).__init__()
        self._quads = quads
        self._dataset = dataset
        # we store how many triples were inserted in each RDF graph
        self._inserted = dict()

    def __repr__(self) -> str:
        return f"<InsertOperator quads={self._quads}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "insert"

    def has_next(self) -> bool:
        """Return True if the iterator has more quads to insert"""
        return len(self._quads) > 0

    async def next(self) -> Optional[Dict[str, str]]:
        """Insert the next quad into the RDF dataset.

        This function works in an iterator fashion, so it can be used in a pipeline of iterators.
        It may also contains `non interruptible` clauses which must 
        be atomically evaluated before preemption occurs.

        Returns: The quad if it was successfully inserted, otwherise it returns `None`. 

        Throws: `StopAsyncIteration` if the iterator has no more quads to insert.
        """
        if not self.has_next():
            raise StopAsyncIteration()
        s, p, o, g = self._quads.pop()
        if self._dataset.has_graph(g):
            self._dataset.get_graph(g).insert(s, p, o)
            # update counters
            if g in self._inserted:
                self._inserted[g] += 1
            else:
                self._inserted[g] = 0
            return {"?s": s, "?p": p, "?o": o, "?graph": g}
        return None

    def save(self) -> SavedInsertData:
        """Save and serialize the iterator as a Protobuf message"""
        saved = SavedInsertData()
        pyDict_to_protoDict(self._inserted, saved.nb_inserted)
        return saved
