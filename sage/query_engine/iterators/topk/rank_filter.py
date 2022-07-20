from typing import Optional

from sage.query_engine.types import QueryContext, Mappings
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator, UnaryPreemtableIterator
from sage.query_engine.iterators.topk.order_conditions import OrderConditions
from sage.query_engine.protobuf.iterators_pb2 import SavedRankFilterIterator


class RankFilterIterator(UnaryPreemtableIterator):
    """
    A RankFilterIterator is used with a TOP-K iterator to perform early pruning.

    This iterator acts like a FilterIterator. It uses the threshold defined by a
    TOPKIterator to reject as soon as possible the solution mappings that cannot
    enter in the TOP-K.

    To be efficient, this iterator must be push as far down the pipeline of
    iterators as possible, by using the filter-push-down heuristic for example.

    Parameters
    ----------
    source: PreemptableIterator
        The child of the iterator.
    expression: OrderConditions
        The conditions of the ORDER BY clause.
    partial: bool
        False if this iterator relies only on a subset of the ORDER BY
        conditions, True otherwise.
    """

    def __init__(
        self, source: PreemptableIterator, expression: OrderConditions,
        is_partial: bool = False
    ) -> None:
        super(RankFilterIterator, self).__init__("rank_filter", source)
        self._expression = expression
        self._is_partial = is_partial
        self._keys = []
        for index, (_, order) in enumerate(self._expression.conditions):
            self._keys.append((f"__order_condition_{index}", order))

    @property
    def expression(self) -> OrderConditions:
        return self._expression

    @property
    def is_partial(self) -> bool:
        return self._is_partial

    def next_stage(self, muc: Mappings) -> None:
        """
        Applies the current mappings to the next triple pattern in the pipeline
        of iterators.

        Parameters
        ----------
        muc : Mappings
            Mappings {?v1: ..., ..., ?vk: ...} computed so far.

        Returns
        -------
        None
        """
        self.source.next_stage(muc)

    def __greater_than_threshold__(
        self, mappings: Mappings, threshold: Optional[Mappings]
    ) -> bool:
        """
        Returns True if the solution can enter in the TOP-K, False otherwise.

        If this iterator relies only on a subset of the ORDER BY conditions, a
        solution mappings cannot be rejected in case of a tie. That is why we
        need the `partial` flag.

        Parameters
        ----------
        mappings: Mappings
            A solution mappings.
        threshold: None | Mappings
            The current threshold to enter in the TOP-K.

        Returns
        -------
        bool
            True if the solution can enter in the TOP-K, False otherwise.
        """
        if threshold is None:
            return True
        for key, order in self._keys:
            if order == "DESC":
                if mappings[key] > threshold[key]:
                    return True
                elif mappings[key] < threshold[key]:
                    return False
            elif order == "ASC":
                if mappings[key] < threshold[key]:
                    return True
                elif mappings[key] > threshold[key]:
                    return False
        return self.is_partial

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
        threshold = context.setdefault("threshold", None)
        while True:
            mu = await self.source.next(context=context)
            if mu is None:
                return None
            # evaluates the ORDER BY conditions with the current mappings
            for index, result in enumerate(self.expression.eval(mu)):
                mu[f"__order_condition_{index}"] = result
            # filter solutions that cannot enter in the TOP-K
            if self.__greater_than_threshold__(mu, threshold):
                return mu

    async def pop(self, context: QueryContext = {}) -> Optional[Mappings]:
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
        return None

    def save(self) -> SavedRankFilterIterator:
        """
        Saves and serializes the iterator as a Protobuf message.

        Returns
        -------
        SavedRankFilterIterator
            The state of the RankFilterIterator as a Protobuf message.
        """
        saved_rank_filter = SavedRankFilterIterator()

        source_field = f"{self.source.name}_source"
        getattr(saved_rank_filter, source_field).CopyFrom(self.source.save())

        saved_rank_filter.expression = self.expression.stringify()
        saved_rank_filter.is_partial = self.is_partial

        return saved_rank_filter

    def explain(self, depth: int = 0) -> str:
        """
        Returns a textual representation of the pipeline of iterators.

        Parameters
        ----------
        depth: int - (default = 0)
            Indicates the current depth in the pipeline of iterators. It is
            used to return a pretty printed representation.

        Returns
        -------
        str
            Textual representation of the pipeline of iterators.
        """
        prefix = ("| " * depth) + "|"
        description = (
            f"{prefix}\n{prefix}-RankFilterIterator <PV=({self.vars}), "
            f"EXPR=({self.expression.stringify()})>\n")
        return description + self.source.explain(depth=depth + 1)

    def stringify(self, level: int = 1) -> str:
        """
        Transforms a pipeline of iterators into a SPARQL query.

        Parameters
        ----------
        level: int - (default = 1)
            Indicates the level of nesting of the group. It is used to pretty
            print the SPARQL query.

        Returns
        -------
        str
            A SPARQL query.
        """
        return self.source.stringify(level=level)
