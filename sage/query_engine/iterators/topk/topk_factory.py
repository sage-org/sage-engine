from sage.query_engine.types import QueryContext
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.topk.topk import TOPKIterator
from sage.query_engine.iterators.topk.topk_server import TOPKServerIterator
from sage.query_engine.iterators.topk.patial_topk import PartialTOPKIterator
from sage.query_engine.iterators.topk.order_conditions import OrderConditions


class TOPKFactory():

    @staticmethod
    def create(
        context: QueryContext, source: PreemptableIterator,
        expression: OrderConditions, limit: int
    ) -> TOPKIterator:
        """
        Returns a TOPK preemptable iterator.

        Parameters
        ----------
        context: QueryContext
            Global variables specific to the execution of the query.
        source: PreemptableIterator
            The child of the iterator.
        expression: OrderConditions
            The conditions of the ORDER BY clause.
        limit: int
            The number of solutions to produce.

        Returns
        -------
        TOPKIterator
            A preemptable iterator to compute the TOP-K.
        """
        strategy = context.setdefault("topk_strategy", "topk_server")
        if strategy == "topk_server":
            return TOPKServerIterator(source, expression, limit)
        elif strategy == "partial_topk":
            return PartialTOPKIterator(source, expression, limit)
        raise Exception(f"Unknown TOP-K strategy '{strategy}'")
