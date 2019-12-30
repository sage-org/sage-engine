# insert.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, List, Optional, Tuple

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedDeleteData
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class DeleteOperator(PreemptableIterator):
    """A DeleteOperator deletes RDF triples from a RDF dataset.

    Constructor args:
        - quads `list`: List of RDF quads (subject, predicate, object, graph_uri) to insert into the graph
        - dataset :class:`sage.database.core.Dataset`: RDF dataset
    """

    def __init__(self, quads: List[Tuple[str, str, str, str]], dataset: Dataset):
        super(DeleteOperator, self).__init__()
        self._quads = quads
        self._dataset = dataset
        # we store how many triples were inserted in each RDF graph
        self._inserted = dict()

    def __repr__(self) -> str:
        return f"<DeleteOperator quads={self._quads}>"

    def serialized_name(self) -> str:
        return "delete"

    def has_next(self) -> bool:
        return len(self._quads) > 0

    async def next(self) -> Optional[Dict[str, str]]:
        """Delete one RDF triple from the RDF graph"""
        if not self.has_next():
            raise StopAsyncIteration()
        s, p, o, g = self._quads.pop()
        if self._dataset.has_graph(g):
            self._dataset.get_graph(g).delete(s, p, o)
            # update counters
            if g in self._inserted:
                self._inserted[g] += 1
            else:
                self._inserted[g] = 0
            return {"?s": s, "?p": p, "?o": o, "?graph": g}
        return None

    def save(self) -> SavedDeleteData:
        """Save the operator using protocol buffers"""
        saved = SavedDeleteData()
        pyDict_to_protoDict(self._inserted, saved.nb_inserted)
        return saved
