# serializable.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from rdflib import Variable


def apply_templates(mappings, templates):
    """
        Returns an iterator that applies each mapping in a set to a set of quads templates
        and returns the distinct quads produced.
    """
    # a set used for deduplication
    seen_before = set()
    for mapping in mappings:
        for s, p, o, g in templates:
            subj, pred, obj = s, p, o
            if s.startswith('?') and s in mapping:
                subj = mapping[s]
            if p.startswith('?') and p in mapping:
                pred = mapping[p]
            if o.startswith('?') and o in mapping:
                obj = mapping[o]
            quad = (subj, pred, obj, g)
            # deduplicate quads
            if quad not in seen_before:
                seen_before.add(quad)
                yield quad


class SerializableUpdate(PreemptableIterator):
    """A SerializableUpdate iterator evaluates a SPARQL Update query under serializability."""

    def __init__(self, dataset, read_input, delete_templates, insert_templates):
        super(SerializableUpdate, self).__init__()
        self._dataset = dataset
        self._read_input = read_input
        self._delete_templates = delete_templates
        self._insert_templates = insert_templates

    def serialized_name(self):
        return "serializable_update"

    def has_next(self):
        """
            Query execution is not finished iff:
                - the read set is not entierly built, or
                - all deletes have not been performed, or
                - all insert have not been performed.
        """
        return self._read_input.has_next()

    async def next(self):
        """Advance in the update execution"""
        if not self.has_next():
            raise StopIteration()

        # read all mappings from the predecessor
        mappings = list()
        while self._read_input.has_next():
            mu = await self._read_input.next()
            mappings.append(mu)

        # apply all deletes
        for s, p, o, g in apply_templates(mappings, self._delete_templates):
            if self._dataset.has_graph(g):
                self._dataset.get_graph(g).delete(s, p, o)

        # apply all inserts
        for s, p, o, g in apply_templates(mappings, self._insert_templates):
            if self._dataset.has_graph(g):
                self._dataset.get_graph(g).insert(s, p, o)
        return None

    def save(self):
        """Useless for this operator, as it MUST run completely inside a quantum"""
        return ''
