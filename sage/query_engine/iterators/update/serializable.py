from typing import Dict, Iterable, List, Tuple

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator, UnaryPreemtableIterator


def apply_templates(
    mappings: List[Dict[str, str]], templates: List[Tuple[str, str, str, str]]
) -> Iterable[Tuple[str, str, str, str]]:
    """
    Returns an iterator that applies each mapping in a set to a set of quads templates
    and returns the distinct quads produced.
    """
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
            if quad not in seen_before:
                seen_before.add(quad)
                yield quad


class SerializableUpdateIterator(UnaryPreemtableIterator):
    """
    A SerializableUpdateIterator iterator evaluates a SPARQL INSERT/DELETE query
    as a serializable transaction.

    Parameters
    ----------
    dataset: Dataset
        RDF dataset to update.
    source: PreemptableIterator
        Iterator that evaluates a WHERE clause.
    delete_templates: List[Tuple[str, str, str, str]]
        List of delete templates from the DELETE clause (nquads to delete).
    insert_templates: List[Tuple[str, str, str, str]]
        List of insert templates from the INSERT clause (nquads to insert).
    """

    def __init__(
        self, source: PreemptableIterator, dataset: Dataset,
        delete_templates: List[Tuple[str, str, str, str]],
        insert_templates: List[Tuple[str, str, str, str]]
    ) -> None:
        super(SerializableUpdateIterator, self).__init__("serializable_update", source)
        self._dataset = dataset
        self._delete_templates = delete_templates
        self._insert_templates = insert_templates

    def has_next(self) -> bool:
        """Return True if the iterator has more quads to process.

        This iterator has not finished to process quads iff:
          * the read set is not entierly built, or
          * all deletes have not been performed, or
          * all insert have not been performed.
        """
        return self._source.has_next()

    async def next(self, context: Dict[str, str]) -> None:
        """
        Executes the SPARQL INSERT/DELETE query.

        This function blocks until the whole query has been processed.
        hence, it breaks the iterator model as all the work is done in a single
        call to next().

        Parameters
        ----------
        context: Dict[str, Any]
            Global variables specific to the execution of the query.

        Returns
        -------
        None | Dict[str, str]
            The RDF quad if it was successfully inserted, None otherwise.

        Raises
        ------
        StopAsyncIteration
            Raised if the iterator has fnished query processing.
        SerializationFailure
            Raised if the SPARQL UPDATE query cannot be serialized as a
            transaction.
        """
        mappings = list()
        mu = self.source.next()
        while mu is not None:
            mappings.append(mu)
            mu = await self._source.next()

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
