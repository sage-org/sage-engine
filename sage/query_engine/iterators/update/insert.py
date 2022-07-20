from typing import Dict, List, Optional, Tuple

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedInsertData
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class InsertIterator(PreemptableIterator):
    """
    An InsertIterator inserts RDF triples into a RDF dataset.

    Parameters
    ----------
    quads: List[Tuple[str, str, str, str]]
        List of RDF quads to delete from the RDF dataset.
    dataset: Dataset
        RDF dataset.
    """

    def __init__(self, quads: List[Tuple[str, str, str, str]], dataset: Dataset):
        super(InsertIterator, self).__init__("insert")
        self._quads = quads
        self._dataset = dataset
        self._inserted = dict()  # number of triples inserted in each RDF graph

    def __repr__(self) -> str:
        return f"<InsertOperator quads={self._quads}>"

    def has_next(self) -> bool:
        """Return True if the iterator has more quads to insert"""
        return len(self._quads) > 0

    def next_stage(self, mappings: Dict[str, str]) -> None:
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        pass

    async def next(self, context: Dict[str, str]) -> Optional[Dict[str, str]]:
        """
        Insert the next quad into the RDF dataset.

        This function works in an iterator fashion, so it can be used in a
        pipeline of iterators.

        Parameters
        ----------
        context: Dict[str, Any]
            Global variables specific to the execution of the query.

        Returns
        -------
        None | Dict[str, str]
            The RDF quad if it was successfully inserted, None otherwise.
        """
        while len(self._quads) > 0:
            s, p, o, graph = self._quads.pop()
            if self._dataset.has_graph(graph):
                self._dataset.get_graph(graph).insert(s, p, o)
                if graph not in self._inserted:
                    self._inserted[graph] = 0
                self._inserted[graph] += 1
                return {"?s": s, "?p": p, "?o": o, "?graph": graph}
        return None

    def save(self) -> SavedInsertData:
        """
        Saves and serializes the iterator as a Protobuf message.

        Returns
        -------
        SavedInsertData
            The state of the InsertIterator as a Protobuf message.
        """
        saved_insert = SavedInsertData()
        pyDict_to_protoDict(self._inserted, saved_insert.nb_inserted)
        return saved_insert
