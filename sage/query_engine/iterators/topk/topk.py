from sage.query_engine.types import Mappings
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator, UnaryPreemtableIterator
from sage.query_engine.iterators.topk.order_conditions import OrderConditions


class TOPKIterator(UnaryPreemtableIterator):
    """
    A TOPKIterator evaluates a SPARQL TOP-K in a pipeline of iterators.

    Parameters
    ----------
    source: PreemptableIterator
        The child of the iterator.

    expression: OrderConditions
        The conditions of the ORDER BY clause.

    limit: int
        The number of solutions to produce.
    """

    def __init__(
        self, name: str, source: PreemptableIterator,
        expression: OrderConditions, limit: int
    ) -> None:
        super(TOPKIterator, self).__init__(name, source)
        self._expression = expression
        self._limit = limit
        self._keys = []
        for index, (_, order) in enumerate(self._expression.conditions):
            self._keys.append((f"__order_condition_{index}", order))

    @property
    def expression(self) -> OrderConditions:
        return self._expression

    @property
    def limit(self) -> int:
        return self._limit

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
            f"{prefix}\n{prefix}-TOPKIterator <PV=({self.vars}), "
            f"LIMIT=({self.limit}), EXPR=({self.expression.stringify()})>\n")
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
        topk = f"ORDER BY {self.expression.stringify()} LIMIT {self.limit}"
        return self.source.stringify(level=level) + topk
