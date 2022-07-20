from typing import List
from abc import ABC, abstractmethod

from sage.query_engine.types import QueryContext
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator


class JoinOrderingStrategy(ABC):

    @abstractmethod
    def compute(self, iterators: List[ScanIterator]) -> PreemptableIterator:
        """
        Creates a left-deep tree from a list of ScanIterators.

        Parameters
        ----------
        iterators: List[ScanIterator]
            The ScanIterators to order.

        Returns
        -------
        PreemptableIterator
            A left-deep tree created from a list of ScanIterators.
        """
        pass


class ForceOrder(JoinOrderingStrategy):

    def compute(self, iterators: List[ScanIterator]) -> PreemptableIterator:
        """
        Creates a left-deep tree from a list of ScanIterators.

        ScanIterators are ordered in the same order they appear in the query.

        Parameters
        ----------
        iterators: List[ScanIterator]
            The ScanIterators to order.

        Returns
        -------
        PreemptableIterator
            A left-deep tree created from a list of ScanIterators.
        """
        pipeline = iterators.pop(0)
        while len(iterators) > 0:
            pipeline = IndexJoinIterator(pipeline, iterators.pop(0))
        return pipeline


class IncreasingCardinityOrdering(JoinOrderingStrategy):

    def __find_connected_pattern__(
        self, pipeline: PreemptableIterator, iterators: List[ScanIterator]
    ) -> int:
        """
        Returns the index of the first triple pattern for which at least one
        variable is bounded. If no such triple pattern can be found, it returns
        the triple pattern with the smallest cardinality, i.e. the 1-th
        ScanIterator.

        Parameters
        ----------
        pipeline: PreemptableIterator
            The pipeline of iterators on top of which we want to insert the
            next ScanIterator.
        iterators: List[ScanIterator]
            The ScanIterators to order.

        Returns
        -------
        int
            The index of the first connected ScanIterator.
        """
        for index, iterator in enumerate(iterators):
            if len(pipeline.vars.intersection(iterator.vars)) > 0:
                return index
        return 0  # if cartesian product, pick the smallest one

    def compute(self, iterators: List[ScanIterator]) -> PreemptableIterator:
        """
        Creates a left-deep tree from a list of ScanIterators.

        ScanIterators are ordered by increasing cardinality. If possible,
        cartesian products are avoided.

        Parameters
        ----------
        iterators: List[ScanIterator]
            The ScanIterators to order.

        Returns
        -------
        PreemptableIterator
            A left-deep tree created from a list of ScanIterators.
        """
        iterators = sorted(iterators, key=lambda it: (it.cardinality, it.predicate))
        pipeline = iterators.pop(0)
        while len(iterators) > 0:
            next = self.__find_connected_pattern__(pipeline, iterators)
            pipeline = IndexJoinIterator(pipeline, iterators.pop(next))
        return pipeline


class JoinOrderingFactory():

    @staticmethod
    def create(context: QueryContext) -> JoinOrderingStrategy:
        """
        Returns a Join Ordering strategy.

        Parameters
        ----------
        context: QueryContext
            Global variables specific to the execution of the query.

        Returns
        -------
        JoinOrderingStrategy
            A strategy to compute the join ordering.
        """
        if context.setdefault("force_order", False):
            return ForceOrder()
        return IncreasingCardinityOrdering()
