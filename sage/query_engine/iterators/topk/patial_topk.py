from typing import Optional

from sage.database.core.dataset import Dataset
from sage.query_engine.exceptions import TOPKLimitReached
from sage.query_engine.protobuf.iterators_pb2 import SavedPartialTOPKIterator
from sage.query_engine.types import QueryContext, Mappings
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.topk.topk import TOPKIterator
from sage.query_engine.iterators.topk.topk_struct import PartialTOPKStruct
from sage.query_engine.iterators.topk.order_conditions import OrderConditions


class PartialTOPKIterator(TOPKIterator):
    """
    A PartialTOPKIterator evaluates a SPARQL TOP-K operator in a pipeline of
    iterators.

    This iterator computes a per quantum TOP-K. A the end of each quantum, the
    client merge the TOP-Ks to compute the final TOP-K. To avoid transferring
    useless mappings, the client also updates the threshold to enter in the
    TOP-K.

    Parameters
    ----------
    source: PreemptableIterator
        The child of the iterator.
    expression: OrderConditions
        The conditions of the ORDER BY clause.
    limit: int
        The number of solutions to produce.
    None | Mappings - (default = None)
        The lowest solution in the TOP-K. A new solution must be greater than
        the threshold to enter in the TOP-K. The threshold is updated by the
        client after each quantum.
    """

    def __init__(
        self, source: PreemptableIterator, expression: OrderConditions,
        limit: int, threshold: Optional[Mappings] = None
    ) -> None:
        super(PartialTOPKIterator, self).__init__(
            "partial_topk", source, expression, limit)
        self._topk = PartialTOPKStruct(
            self._keys, limit=limit, threshold=threshold)
        self._dataset = Dataset()

    async def next(self, context: QueryContext = {}) -> Optional[Mappings]:
        """
        Generates the next item from the iterator, following the iterator
        protocol.

        Parameters
        ----------
        context: QueryContext
            Global variables specific to the execution of the query.

        Returns
        -------
        None | Mappings
            The next item produced by the iterator, or None if all items have
            been produced.

        Raises
        ------
        QuantumExhausted
        """
        max_limit = context.setdefault("max_limit", self._dataset.max_limit)
        mu = await self.source.next(context=context)
        while mu is not None:
            # evaluates the ORDER BY conditions with the current mappings
            for index, result in enumerate(self.expression.eval(mu)):
                mu[f"__order_condition_{index}"] = result
            # inserts the solution in the TOP-K
            self._topk.insert(mu)
            # if we have exceeded the server resources, the TOP-K is sent to the client
            if len(self._topk) >= max_limit:
                raise TOPKLimitReached()
            # updates the threshold for the RankFilterIterators to allow early pruning
            context["threshold"] = self._topk.threshold()
            mu = await self.source.next(context=context)
        # once the previous iterator is exhausted, produces the TOP-K results
        return self.pop(context=context)

    def pop(self, context: QueryContext = {}) -> Optional[Mappings]:
        """
        Generates the next item from the iterator, following the iterator
        protocol.

        This method does not generate any scan on the database. It is used to
        clear internal data structures such as the buffer of the TOPKIterator.

        Parameters
        ----------
        context: QueryContext
            Global variables specific to the execution of the query.

        Returns
        -------
        None | Mappings
            The next item produced by the iterator, or None if all internal
            data structures are empty.
        """
        if len(self._topk) > 0:
            return self._topk.pop()
        return None

    def save(self) -> SavedPartialTOPKIterator:
        """
        Saves and serializes the iterator as a Protobuf message.

        Returns
        -------
        SavedPartialTOPKIterator
            The state of the TOPKIterator as a Protobuf message.
        """
        saved_partial_topk = SavedPartialTOPKIterator()

        source_field = f"{self.source.name}_source"
        getattr(saved_partial_topk, source_field).CopyFrom(self.source.save())

        saved_partial_topk.expression = self.expression.stringify()
        saved_partial_topk.limit = self.limit

        return saved_partial_topk
