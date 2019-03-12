# insert.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedInsertData
from sage.query_engine.iterators.utils import IteratorExhausted
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class InsertOperator(PreemptableIterator):
    """A InsertOperator inserts RDF triples into a RDF dataset.

    Constructor args:
        - quads `list`: List of RDF quads (subject, predicate, object, graph_uri) to insert into the graph
        - graph :class:`sage.database.datasets.Dataset`: RDF dataset
    """

    def __init__(self, quads, dataset):
        super(InsertOperator, self).__init__()
        self._quads = quads
        self._dataset = dataset
        # we store how many triples were inserted in each RDF graph
        self._inserted = dict()

    def __repr__(self):
        return "<InsertOperator quads={}>".format(self._quads)

    def serialized_name(self):
        return "insert"

    def has_next(self):
        return len(self._quads) > 0

    async def next(self):
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
        return None

    def save(self):
        """Save the operator using protocol buffers"""
        saved = SavedInsertData()
        pyDict_to_protoDict(self._inserted, saved.nb_inserted)
        return saved
