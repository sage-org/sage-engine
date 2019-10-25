# serializable.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from rdflib import Variable


def apply_templates(mapping, templates):
    """Apply a set of mappings to a set of Quads templates"""
    res = set()
    for s, p, o, g in templates:
        subj, pred, obj = s, p, o
        if s.startswith('?') and s in mapping:
            subj = mapping[s]
        if p.startswith('?') and p in mapping:
            pred = mapping[p]
        if o.startswith('?') and o in mapping:
            obj = mapping[o]
        res.add((s, p, o, g))
    return res


class SerializableUpdate(PreemptableIterator):
    """A SerializableUpdate iterator evaluates a SPARQL Update query under serializability."""

    def __init__(self, dataset, read_input, delete_templates, insert_templates):
        super(SerializableUpdate, self).__init__()
        self._dataset = dataset
        self._read_input = read_input
        self._delete_templates = delete_templates
        self._insert_templates = insert_templates
        self._delete_set = set()
        self._insert_set = set()

    def serialized_name(self):
        return "serializable_update"

    def has_next(self):
        """
            Query execution is not finished iff:
                - the read set is not entierly built, or
                - all deletes have not been performed, or
                - all insert have not been performed.
        """
        return self._read_input.has_next() or len(self._delete_set) > 0 or len(self._insert_set) > 0

    async def next(self):
        """Advance in the update execution"""
        if not self.has_next():
            raise StopIteration()

        if self._read_input.has_next():
            # read a mapping from the predecessor
            mu = await self._read_input.next()
            # apply delete/insert templates to produce new quads to delete/insert
            if len(self._delete_templates) > 0:
                self._delete_set.update(apply_templates(mu, self._delete_templates))
            if len(self._insert_templates) > 0:
                self._insert_set.update(apply_templates(mu, self._insert_templates))
        elif len(self._delete_set) > 0:
            # delete a quad
            s, p, o, g = self._delete_set.pop()
            if self._dataset.has_graph(g):
                self._dataset.get_graph(g).delete(s, p, o)
        else:
            # insert a new quad
            s, p, o, g = self._insert_set.pop()
            if self._dataset.has_graph(g):
                self._dataset.get_graph(g).insert(s, p, o)
        return None

    def save(self):
        """Useless for this operator, as it MUST run completely inside a quantum"""
        return ''
