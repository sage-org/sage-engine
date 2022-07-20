from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Set, List

from sage.query_engine.types import QueryContext, Mappings, SavedPlan


class PreemptableIterator(ABC):
    """
    An abstract class for a preemptable iterator.

    Parameters
    ----------
    name: str
        The name of the preemptable iterator. It is used to identify the
        iterator in the Protobuf messages.
    """

    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def vars(self) -> Set[str]:
        return set()

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def save(self) -> SavedPlan:
        """
        Saves and serializes the iterator as a Protobuf message.

        Returns
        -------
            A Protobuf message.
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    def flatten(self) -> List[PreemptableIterator]:
        """
        Transforms a pipeline of iterators into a list using a postfix notation.

        Returns
        -------
        List[PreemptableIterator]
            A list of preemptable iterators created using a postfix notation.
        """
        return [self]


class UnaryPreemtableIterator(PreemptableIterator):
    """
    An abstract class for a unary preemptable iterator.

    Parameters
    ----------
    name: str
        The name of the preemptable iterator. It is used to identify the
        iterator in the Protobuf messages.
    source: PreemptableIterator
        The child of the iterator.
    """

    def __init__(self, name: str, source: PreemptableIterator) -> None:
        super().__init__(name)
        self._source = source

    @property
    def source(self) -> PreemptableIterator:
        return self._source

    @source.setter
    def source(self, iterator: PreemptableIterator) -> None:
        self._source = iterator

    @property
    def vars(self) -> Set[str]:
        return self.source.vars

    def flatten(self) -> List[PreemptableIterator]:
        """
        Transforms a pipeline of iterators into a list using a postfix notation.

        Returns
        -------
        List[PreemptableIterator]
            A list of preemptable iterators created using a postfix notation.
        """
        return self.source.flatten() + [self]


class BinaryPreemtableIterator(PreemptableIterator):
    """
    An abstract class for a binary preemptable iterator.

    Parameters
    ----------
    name: str
        The name of the preemptable iterator. It is used to identify the
        iterator in the Protobuf messages.
    left: PreemptableIterator
        The left child of the iterator.
    right: PreemptableIterator
        The right child of the iterator.
    """

    def __init__(self, name: str, left: PreemptableIterator, right: PreemptableIterator) -> None:
        super().__init__(name)
        self._left = left
        self._right = right

    @property
    def left(self) -> PreemptableIterator:
        return self._left

    @left.setter
    def left(self, iterator: PreemptableIterator) -> None:
        self._left = iterator

    @property
    def right(self) -> PreemptableIterator:
        return self._right

    @right.setter
    def right(self, iterator: PreemptableIterator) -> None:
        self._right = iterator

    @property
    def vars(self) -> Set[str]:
        return self.left.vars.union(self.right.vars)

    def flatten(self) -> List[PreemptableIterator]:
        """
        Transforms a pipeline of iterators into a list using a postfix notation.

        Returns
        -------
        List[PreemptableIterator]
            A list of preemptable iterators created using a postfix notation.
        """
        return self.left.flatten() + self.right.flatten() + [self]
