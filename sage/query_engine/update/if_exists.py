# if_exists.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator


class IfExistsOperator(PreemptableIterator):
    """A IfExistsOperator checks if all N-Quads in a set exist in the database."""

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
        s, p, o, g = self._quads.pop()
        if self._dataset.has_graph(g):
            try:
                _, card = self._dataset.get_graph(g).search(s, p, o, as_of=self._start_time)
                self._found_missing = card > 0
            except Exception:
                self._found_missing = False
        else:
            self._found_missing = False
        return None

    def save(self):
        """Useless for this operator, as it MUST run completely inside a quantum"""
        return ''
