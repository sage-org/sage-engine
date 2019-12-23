# insert.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, List, Optional, Tuple

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.utils import IteratorExhausted
from sage.query_engine.protobuf.iterators_pb2 import SavedInsertData
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class InsertOperator(PreemptableIterator):
    """A InsertOperator inserts RDF triples into a RDF dataset.

    Constructor args:
        - quads `list`: List of RDF quads (subject, predicate, object, graph_uri) to insert into the graph
        - dataset :class:`sage.database.core.Dataset`: RDF dataset
    """

    def __init__(self, quads: List[Tuple[str, str, str, str]], dataset: Dataset, server_url: str):
        super(InsertOperator, self).__init__()
        self._quads = quads
        self._dataset = dataset
        self._server_url = server_url
        # we store how many triples were inserted in each RDF graph
        self._inserted = dict()

    def __repr__(self) -> str:
        return f"<InsertOperator quads={self._quads}>"

    def serialized_name(self) -> str:
        return "insert"

    def has_next(self) -> bool:
        return len(self._quads) > 0

    async def next(self) -> Optional[Dict[str, str]]:
        """Insert one RDF triple into the RDF dataset"""
        if not self.has_next():
            raise IteratorExhausted()
        s, p, o, g = self._quads.pop()
        if self._dataset.has_graph(g):
            self._dataset.get_graph(g).insert(s, p, o)
            # update counters
            if g in self._inserted:
                self._inserted[g] += 1
            else:
                self._inserted[g] = 0
            return {"?s": s, "?p": p, "?o": o, "?graph": f"{self._server_url}/{g}"}
        return None

    def save(self) -> SavedInsertData:
        """Save the operator using protocol buffers"""
        saved = SavedInsertData()
        pyDict_to_protoDict(self._inserted, saved.nb_inserted)
        return saved
