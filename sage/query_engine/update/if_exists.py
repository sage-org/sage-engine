# if_exists.py
# Author: Thomas MINIER - MIT License 2017-2020
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator


class IfExistsOperator(PreemptableIterator):
    """
        A IfExistsOperator checks if all N-Quads in a set exist in the database.
        It is used to provide the "serializability per solution group" consistency level.
    """

    def __init__(self, quads, dataset, start_time):
        super(IfExistsOperator, self).__init__()
        self._quads = quads
        self._dataset = dataset
        self._found_missing = False
        self._start_time = start_time

    def __repr__(self):
        return "<IfExistsOperator quads={}>".format(self._quads)

    @property
    def missing_nquads(self):
        """Returns True if, at the time of invocation, at least one n-quad was not found in the RDF dataset."""
        return self._found_missing

    def serialized_name(self):
        return "ifexists"

    def has_next(self):
        return (not self._found_missing) and len(self._quads) > 0

    async def next(self):
        """Check if the next n-quad exists in the dataset."""
        if not self.has_next():
            raise StopIteration()
        triple = self._quads.pop()
        if self._dataset.has_graph(triple['graph']):
            try:
                s, p, o = triple['subject'], triple['predicate'], triple['object']
                iterator, _ = self._dataset.get_graph(triple['graph']).search(s, p, o, as_of=self._start_time)
                self._found_missing = not iterator.has_next()
            except Exception:
                self._found_missing = True
        else:
            self._found_missing = True
        return None

    def save(self):
        """Useless for this operator, as it MUST run completely inside a quantum"""
        return ''
