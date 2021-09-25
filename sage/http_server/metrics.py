from typing import Union, List
from sage.query_engine.protobuf.iterators_pb2 import (RootTree,
                                                      SavedBagUnionIterator,
                                                      SavedFilterIterator,
                                                      SavedIndexJoinIterator,
                                                      SavedProjectionIterator,
                                                      SavedScanIterator,
                                                      SavedValuesIterator)

SavedIterator = Union[
    RootTree,
    SavedBagUnionIterator,
    SavedFilterIterator,
    SavedIndexJoinIterator,
    SavedProjectionIterator,
    SavedScanIterator,
    SavedValuesIterator
]


def coverage(saved_plan: RootTree) -> float:
    saved_iterator = getattr(saved_plan, saved_plan.WhichOneof('source'))
    return estimate_coverage(saved_iterator)


def triples_scanned(saved_plan: RootTree) -> float:
    saved_iterator = getattr(saved_plan, saved_plan.WhichOneof('source'))
    return count_triples_scanned(saved_iterator)


def flatten_leaves(
    saved_iterator: SavedIterator, iterators: List[Union[SavedScanIterator, SavedValuesIterator]]
) -> None:
    if (type(saved_iterator) is SavedFilterIterator) or (type(saved_iterator) is SavedProjectionIterator):
        source_iterator = getattr(saved_iterator, saved_iterator.WhichOneof('source'))
        flatten_leaves(source_iterator, iterators)
    elif type(saved_iterator) is SavedScanIterator or type(saved_iterator) is SavedValuesIterator:
        iterators.append(saved_iterator)
    elif type(saved_iterator) is SavedIndexJoinIterator:
        left_iterator = getattr(saved_iterator, saved_iterator.WhichOneof('left'))
        right_iterator = getattr(saved_iterator, saved_iterator.WhichOneof('right'))
        flatten_leaves(left_iterator, iterators)
        flatten_leaves(right_iterator, iterators)
    else:
        raise Exception(f"Unsupported iterator type '{type(saved_iterator)}'")


def estimate_coverage(saved_iterator: SavedIterator) -> float:
    leaf_iterators = list()
    flatten_leaves(saved_iterator, leaf_iterators)
    space_covered = 0
    for i in range(len(leaf_iterators)):
        leaf_iterator = leaf_iterators[i]
        if leaf_iterator.produced == 0:
            break
        iterator_coverage = (leaf_iterator.produced - 1) / (leaf_iterator.runtime_cardinality)
        for j in range(i):
            previous_scan_iterator = leaf_iterators[j]
            iterator_coverage *= (1 / previous_scan_iterator.runtime_cardinality)
        space_covered += iterator_coverage
    return space_covered


def count_triples_scanned(saved_iterator: SavedIterator) -> int:
    if (type(saved_iterator) is SavedFilterIterator) or (type(saved_iterator) is SavedProjectionIterator):
        source_iterator = getattr(saved_iterator, saved_iterator.WhichOneof('source'))
        return count_triples_scanned(source_iterator)
    elif type(saved_iterator) is SavedScanIterator:
        return saved_iterator.produced
    elif type(saved_iterator) is SavedValuesIterator:
        return 0
    elif (type(saved_iterator) is SavedIndexJoinIterator) or (type(saved_iterator) is SavedBagUnionIterator):
        left_iterator = getattr(saved_iterator, saved_iterator.WhichOneof('left'))
        right_iterator = getattr(saved_iterator, saved_iterator.WhichOneof('right'))
        return count_triples_scanned(left_iterator) + count_triples_scanned(right_iterator)
    else:
        raise Exception(f"Unknown iterator type '{type(saved_iterator)}'")
