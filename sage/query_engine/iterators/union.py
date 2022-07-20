from typing import Optional
from random import random

from sage.query_engine.types import QueryContext, Mappings
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator, BinaryPreemtableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedBagUnionIterator


class BagUnionIterator(BinaryPreemtableIterator):
    """
    A BagUnionIterator evaluates an UNION clause with a bag semantics in a
    pipeline of iterators.

    This operator sequentially produces all solutions from the left iterator,
    and then do the same for the right iterator.

    Parameters
    ----------
    left: PreemptableIterator
        The left child of the iterator.
    right: PreemptableIterator
        The right child of the iterator.
    """

    def __init__(self, left: PreemptableIterator, right: PreemptableIterator):
        super(BagUnionIterator, self).__init__("union", left, right)

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
        self.left.next_stage(muc)
        self.right.next_stage(muc)

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
        mu = await self.left.next(context=context)
        if mu is not None:
            return mu
        return await self.right.next(context=context)

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

    def save(self) -> SavedBagUnionIterator:
        """
        Saves and serializes the iterator as a Protobuf message.

        Returns
        -------
        SavedBagUnionIterator
            The state of the BagUnionIterator as a Protobuf message.
        """
        saved_union = SavedBagUnionIterator()

        left_field = f"{self.left.name}_left"
        getattr(saved_union, left_field).CopyFrom(self.left.save())

        right_field = f"{self.right.name}_right"
        getattr(saved_union, right_field).CopyFrom(self.right.save())

        return saved_union

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
        description = f"{prefix}\n{prefix}-BagUnionIterator <PV=({self.vars})>\n"
        description += self.left.explain(depth=depth + 1)
        description += self.right.explain(depth=depth + 1)
        return description

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
        return ((
            f"{prefix}{{\n{self.left.stringify(level=level + 1)}"
            f"{prefix}}} UNION {{\n"
            f"{self.right.stringify(level=level + 1)}{prefix}}}\n"))


class RandomBagUnionIterator(BagUnionIterator):
    """
    A RandomBagUnionIterator evaluates an UNION clause with a bag semantics in a
    pipeline of iterators.

    This operator randomly reads from the left and right operands to produce
    solution mappings.

    Parameters
    ----------
    left: PreemptableIterator
        The left child of the iterator.
    right: PreemptableIterator
        The right child of the iterator.
    """

    def __init__(self, left: PreemptableIterator, right: PreemptableIterator):
        super(BagUnionIterator, self).__init__(left, right)

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
        """
        if random() < 0.5:
            left = self.left
            right = self.right
        else:
            left = self.right
            right = self.left
        mu = await left.next(context=context)
        if mu is not None:
            return mu
        return await right.next(context=context)
