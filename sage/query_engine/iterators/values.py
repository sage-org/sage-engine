from typing import List, Optional, Set

from sage.query_engine.types import QueryContext, Mappings
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedValuesIterator, SolutionMappings
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class ValuesIterator(PreemptableIterator):
    """
    A ValuesIterator evaluates a VALUES clause in the pipeline of iterators.

    This iterator produces all mappings that are defined in the VALUES clause.

    Parameters
    ----------
    items: List[Mappings]
        The mappings to produce.
    muc: None | Mappings - (default = None)
        The current mappings at this point in the evaluation of the query.
    next_item: int - (default = 0)
        The index of the next mappings to produce.
    """

    def __init__(
        self, items: List[Mappings], muc: Optional[Mappings] = None,
        next_item: int = 0
    ):
        super().__init__("values")
        self._items = items
        self._muc = muc
        self._next_item = next_item

    @property
    def vars(self) -> Set[str]:
        return set(self._items[0].keys())

    @property
    def cardinality(self) -> int:
        return len(self._items)

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
        self._muc = muc
        self._next_item = 0

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
        if self._next_item >= self.cardinality:
            return None
        mu = self._items[self._next_item]
        self._next_item += 1
        if self._muc is not None:
            if any([self._muc[key] != mu[key] for key in self._muc.keys() & mu.keys()]):  # mappings are not compatible
                return None
            return {**self._muc, **mu}
        return mu

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
        return None

    def save(self) -> SavedValuesIterator:
        """
        Saves and serializes the iterator as a Protobuf message.

        Returns
        -------
        SavedValuesIterator
            The state of the ValuesIterator as a Protobuf message.
        """
        saved_values = SavedValuesIterator()

        saved_items = list()
        for item in self._items:
            saved_mappings = SolutionMappings()
            pyDict_to_protoDict(item, saved_mappings.bindings)
            saved_items.append(saved_mappings)
        saved_values.items.extend(saved_items)

        saved_values.next_item = self._next_item

        if self._muc is not None:
            pyDict_to_protoDict(self._muc, saved_values.muc)

        return saved_values

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
        return f"{prefix}\n{prefix}-ValuesIterator <PV=({self.vars})>\n"

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
        prefix = " " * 2 * level
        variables = " ".join(self.vars)
        solution_mappings = list()
        for mappings in self._items:
            solution_mappings.append(f"({ ' '.join(mappings.values()) })")
        return f"{prefix}VALUES ({variables}) {{ { ' '.join(solution_mappings) } }} .\n"
