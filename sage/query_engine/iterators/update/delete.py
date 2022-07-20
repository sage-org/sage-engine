from typing import Dict, List, Optional, Tuple

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedDeleteData
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class DeleteIterator(PreemptableIterator):
    """
    A DeleteOperator deletes RDF triples from a RDF dataset.

    Parameters
    ----------
    quads: List[Tuple[str, str, str, str]]
        List of RDF quads to delete from the RDF dataset.
    dataset: Dataset
        RDF dataset.
    """

    def __init__(self, quads: List[Tuple[str, str, str, str]], dataset: Dataset):
        super(DeleteIterator, self).__init__("delete")
        self._quads = quads
        self._dataset = dataset
        self._deleted = dict()  # number of triples inserted in each RDF graph

    def __repr__(self) -> str:
        return f"<DeleteOperator quads={self._quads}>"

    def has_next(self) -> bool:
        """Return True if the iterator has more quads to delete"""
        return len(self._quads) > 0

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
        Deletes the next quad from the RDF dataset.

        This function works in an iterator fashion, so it can be used in a
        pipeline of iterators.

        Parameters
        ----------
        context: Dict[str, Any]
            Global variables specific to the execution of the query.

        Returns
        -------
        None | Dict[str, str]
            The RDF quad if it was successfully deleted, None otherwise.
        """
        while len(self._quads) > 0:
            s, p, o, graph = self._quads.pop()
            if self._dataset.has_graph(graph):
                self._dataset.get_graph(graph).delete(s, p, o)
                if graph not in self._inserted:
                    self._deleted[graph] = 0
                self._deleted[graph] += 1
                return {"?s": s, "?p": p, "?o": o, "?graph": graph}
        return None

    def save(self) -> SavedDeleteData:
        """
        Saves and serializes the iterator as a Protobuf message.

        Returns
        -------
        SavedDeleteData
            The state of the DeleteIterator as a Protobuf message.
        """
        saved_delete = SavedDeleteData()
        pyDict_to_protoDict(self._deleted, saved_delete.nb_deleted)
        return saved_delete
