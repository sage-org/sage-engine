# scan.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.preemptable_iterator import PreemptableIterator
from query_engine.iterators.utils import apply_bindings, vars_positions, selection
from query_engine.protobuf.iterators_pb2 import TriplePattern, SavedScanIterator


class ScanIterator(PreemptableIterator):
    """A ScanIterator scans a HDT relation, i.e. RDF triples matching a triple pattern, and apply selections.

    Constructor args:
        - source [hdt.TripleIterator] - An HDT iterator that yields RDF triple in string format.
        - triple [TriplePattern] - The triple pattern corresponding to the source iterator.
        - tripleName [string] - A key to identify the triple pattern.
        - cardinality [integer, default=None] - The cardinality of the triple pattern.
    """
    def __init__(self, source, triple, cardinality=0):
        super(ScanIterator, self).__init__()
        self._source = source
        self._triple = triple
        self._variables = vars_positions(triple['subject'], triple['predicate'], triple['object'])
        self._cardinality = cardinality

    def __len__(self):
        return self._cardinality

    def __repr__(self):
        return "<ScanIterator { %s %s %s } OFFSET %i>" % (self._triple['subject'], self._triple['predicate'], self._triple['object'], self._source.offset)

    @property
    def nb_reads(self):
        return self._source.nb_reads

    @property
    def offset(self):
        return self._source.offset

    def has_next(self):
        return self._source.has_next()

    async def next(self):
        """Scan the relation and return the next set of solution mappings"""
        mappings = dict(selection(next(self._source), self._variables))
        return mappings

    def save(self):
        """Save the operator using protocol buffers"""
        saveScan = SavedScanIterator()
        triple = TriplePattern()
        triple.subject = self._triple['subject']
        triple.predicate = self._triple['predicate']
        triple.object = self._triple['object']
        saveScan.triple.CopyFrom(triple)
        saveScan.offset = self.offset + self.nb_reads
        saveScan.cardinality = self._cardinality
        return saveScan
