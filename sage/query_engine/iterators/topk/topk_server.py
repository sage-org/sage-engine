from typing import Optional, List

from sage.query_engine.protobuf.iterators_pb2 import SavedTOPKServerIterator, SolutionMappings
from sage.query_engine.protobuf.utils import pyDict_to_protoDict
from sage.query_engine.types import QueryContext, Mappings
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.topk.topk import TOPKIterator
from sage.query_engine.iterators.topk.topk_struct import TOPKStruct
from sage.query_engine.iterators.topk.order_conditions import OrderConditions


class TOPKServerIterator(TOPKIterator):
    """
    A TOPKServerIterator evaluates a SPARQL TOP-K in a pipeline of iterators.

    The TOP-K is evaluated on the server.

    Parameters
    ----------
    source: PreemptableIterator
        The child of the iterator.
    expression: OrderConditions
        The conditions of the ORDER BY clause.
    limit: int
        The number of solutions to produce.
    topk: List[Mappings] - (default = [])
        The current state of the TOP-K.
    """

    def __init__(
        self, source: PreemptableIterator, expression: OrderConditions,
        limit: int, topk: List[Mappings] = []
    ) -> None:
        super(TOPKServerIterator, self).__init__(
            "topk_server", source, expression, limit)
        self._topk = TOPKStruct(self._keys, limit=limit)
        for mappings in topk:
            self._topk.insert(mappings)

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
        mu = await self.source.next(context=context)
        while mu is not None:
            # evaluates the ORDER BY conditions with the current mappings
            for index, result in enumerate(self.expression.eval(mu)):
                mu[f"__order_condition_{index}"] = result
            # inserts the solution in the TOP-K
            self._topk.insert(mu)
            # updates the threshold for the RankFilterIterators to allow early pruning
            context["threshold"] = self._topk.threshold()
            mu = await self.source.next(context=context)
        # once the previous iterator is exhausted, produces the TOP-K results
        if len(self._topk) > 0:
            mu = self._topk.pop()
            for key, _ in self._keys:  # deletes extra keys
                del mu[key]
            return mu
        return None

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
        None | Dict[str, str]
            The next item produced by the iterator, or None if all internal
            data structures are empty.
        """
        return None

    def save(self) -> SavedTOPKServerIterator:
        """
        Saves and serializes the iterator as a Protobuf message.

        Returns
        -------
        SavedTOPKIterator
            The state of the TOPKIterator as Protobuf message.
        """
        saved_topk = SavedTOPKServerIterator()

        source_field = f"{self.source.name}_source"
        getattr(saved_topk, source_field).CopyFrom(self.source.save())

        saved_topk.expression = self.expression.stringify()
        saved_topk.limit = self.limit

        saved_solutions = list()
        for solution in self._topk.flatten():
            saved_mappings = SolutionMappings()
            pyDict_to_protoDict(solution, saved_mappings.bindings)
            saved_solutions.append(saved_mappings)
        saved_topk.topk.extend(saved_solutions)

        return saved_topk
