# serializable.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Iterable, List, Tuple

from rdflib import Variable

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator

Quad = Tuple[str, str, str, str]


def apply_templates(mappings: List[Dict[str, str]], templates: List[Quad]) -> Iterable[Quad]:
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
    """A SerializableUpdate iterator evaluates a SPARQL INSERT/DELETE query as a serializable transaction.
    
    Args:
      * dataset: RDF dataset to update.
      * read_input: Iterator that evaluates a WHERE clause.
      * delete_templates: List of delete templates from the DELETE clause (nquads to delete).
      * insert_templates: List of insert templates from the INSERT clause (nquads to insert).
    """

    def __init__(self, dataset: Dataset, read_input: PreemptableIterator, delete_templates: List[Quad], insert_templates: List[Quad]):
        super(SerializableUpdate, self).__init__()
        self._dataset = dataset
        self._read_input = read_input
        self._delete_templates = delete_templates
        self._insert_templates = insert_templates

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "serializable_update"

    def has_next(self) -> bool:
        """Return True if the iterator has more quads to process.
        
        This iterator has not finished to process quads iff:
          * the read set is not entierly built, or
          * all deletes have not been performed, or
          * all insert have not been performed.
        """
        return self._read_input.has_next()

    async def next(self) -> None:
        """Execute the SPARQL INSERT/DELETE query.

        This function blocks until the whole query has been processed.
        hence, it breaks the iterator model as all the work is done in a single call to next()
        It may also contains `non interruptible` clauses which must 
        be atomically evaluated before preemption occurs.

        Returns: Always `None` 

        Throws:
          * `StopAsyncIteration` if the iterator has fnished query processing.
          * `SerializationFailure` if the SPARQL UPDATE query cannot be serialized as a transaction.
        """
        if not self.has_next():
            raise StopAsyncIteration()

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

    def save(self) -> str:
        """Useless for this operator, as it MUST run completely inside a quantum"""
        return ''
