from typing import List

from sage.query_engine.types import QueryContext
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator, UnaryPreemtableIterator, BinaryPreemtableIterator
from sage.query_engine.iterators.values import ValuesIterator
from sage.query_engine.iterators.scan import ScanIterator


class FilterPushDown():
    """
    This class implements an heuristic that consists in pushing the filters as
    low as possible in the pipeline of iterators.
    """

    def find_parent(
        self, pipeline: List[PreemptableIterator], i: int
    ) -> PreemptableIterator:
        """
        Returns the closest parent of the i-th iterator in the pipeline.
        There is 3 cases to consider:
            - if the (i + 1)-th iterator is unary, then the i-th iterator is
              its child.
            - if the (i + 1)-th iterator is binary, then the i-th iterator is
              its right child.
            - in the last case, the closest parent of i is the j-th iterator
              such that there is an even number of iterators between i and j.

        Parameters
        ----------
        pipeline: List[PreemptableIterator]
            A pipeline of iterators in postfix notation.
        i: int
            The index of an iterator in the pipeline.

        Returns
        -------
        PreemptableIterator
            The closest parent of the i-th iterator.
        """
        if isinstance(pipeline[i + 1], UnaryPreemtableIterator):
            return pipeline[i + 1]
        elif isinstance(pipeline[i + 1], BinaryPreemtableIterator):
            return pipeline[i + 1]
        children = 1
        for j in range(i + 1, len(pipeline)):
            if isinstance(pipeline[j], (ScanIterator, ValuesIterator)):
                children += 1
            elif isinstance(pipeline[j], BinaryPreemtableIterator):
                children -= 1
                if children == 1:
                    return pipeline[j]
        raise Exception("FilterPushDown - An unexpected error occured...")

    def remove_filter(
        self, pipeline: List[PreemptableIterator], i: int
    ) -> None:
        """
        Removes the FilterIterator located at the i-th position from the
        pipeline of iterators.

        Parameters
        ----------
        pipeline: List[PreemptableIterator]
            A pipeline of iterators in postfix notation.
        i: int
            The index of a FilterIterator in the pipeline.
        """
        if isinstance(pipeline[i + 1], UnaryPreemtableIterator):
            self.find_parent(pipeline, i)._source = pipeline[i - 1]
        elif isinstance(pipeline[i + 1], BinaryPreemtableIterator):
            self.find_parent(pipeline, i)._right = pipeline[i - 1]
        elif isinstance(pipeline[i + 1], (ScanIterator, ValuesIterator)):
            self.find_parent(pipeline, i)._left = pipeline[i - 1]
        else:
            raise Exception("FilterPushDown - An unexpected error occured...")

    def move_filter(
        self, pipeline: List[PreemptableIterator], i: int, j: int
    ) -> None:
        """
        Moves the FilterIterator located at the i-th position so that it becomes
        the parent of the j-th iterator.

        Parameters
        ----------
        pipeline: List[PreemptableIterator]
            A pipeline of iterators in postfix notation.
        i: int
            The index of a FilterIterator in the pipeline.
        j: int
            The position to which the iterator is to be moved.
        """
        pipeline[i]._source = pipeline[j]
        if isinstance(pipeline[j + 1], UnaryPreemtableIterator):
            self.find_parent(pipeline, j)._source = pipeline[i]
        elif isinstance(pipeline[j + 1], BinaryPreemtableIterator):
            self.find_parent(pipeline, j)._right = pipeline[i]
        elif isinstance(pipeline[j + 1], (ScanIterator, ValuesIterator)):
            self.find_parent(pipeline, j)._left = pipeline[i]
        else:
            raise Exception("FilterPushDown - An unexpected error occured...")
        pipeline.insert(j + 1, pipeline.pop(i))

    def visit(
        self, iterator: PreemptableIterator, context: QueryContext = {}
    ) -> PreemptableIterator:
        """
        Pushes FilterIterators as far down the pipeline of iterators as
        possible, starting from the provided iterator.

        Parameters
        ----------
        iterator: PreemptableIterator
            A pipeline of iterators.
        context: QueryContext
            Global variables specific to the execution of the query.

        Returns
        -------
        PreemptableIterator
            A PreemptableIterator where FilterIterators were pushed down the
            pipeline of iterators.

        NOTE: This implementation does not handle UNION clauses...
        """
        pipeline = iterator.flatten()

        update = True
        nb_iterations = 0
        while update and nb_iterations < 100:
            update = False
            nb_iterations += 1
            i = 0
            while i < len(pipeline) and not update:
                if pipeline[i].name in ["filter", "rank_filter"]:
                    variables = pipeline[i].expression.variables()
                    j = i - 2
                    while j >= 0:  # filter-push-down
                        if pipeline[j].name == "union":  # filter-push-down inside UNION clauses is not supported
                            break
                        elif pipeline[j + 1].name in ["filter", "rank_filter"]:  # to avoid a non-ending switch of two filters
                            j -= 1
                            continue
                        elif pipeline[j].vars.issuperset(variables):  # it can be moved one step further
                            self.remove_filter(pipeline, i)  # remove the filter from its current position i
                            self.move_filter(pipeline, i, j)  # move the filter to its new position j
                            i = j + 1
                            update = True
                        j -= 1
                i += 1
        return iterator
