from typing import Union, List
from sage.query_engine.protobuf.iterators_pb2 import (RootTree,
                                                      SavedBagUnionIterator,
                                                      SavedFilterIterator,
                                                      SavedIndexJoinIterator,
                                                      SavedProjectionIterator,
                                                      SavedScanIterator)

SavedProtobufPlan = Union[
    RootTree,
    SavedBagUnionIterator,
    SavedFilterIterator,
    SavedIndexJoinIterator,
    SavedProjectionIterator,
    SavedScanIterator
]


def coverage(saved_plan: RootTree, runtime_cardinality: bool = True) -> float:
    saved_iterator = getattr(saved_plan, saved_plan.WhichOneof('source'))
    return estimate_coverage(saved_iterator, runtime_cardinality=runtime_cardinality)


def triples_scanned(saved_plan: RootTree) -> float:
    saved_iterator = getattr(saved_plan, saved_plan.WhichOneof('source'))
    return count_triples_scanned(saved_iterator)


def flatten_scans(
    saved_iterator: SavedProtobufPlan, scans_state: List[SavedScanIterator]
) -> None:
    if (type(saved_iterator) is SavedFilterIterator) or (type(saved_iterator) is SavedProjectionIterator):
        source_iterator = getattr(saved_iterator, saved_iterator.WhichOneof('source'))
        flatten_scans(source_iterator, scans_state)
    elif type(saved_iterator) is SavedScanIterator:
        # print(saved_iterator.pattern)
        # print(f'last position: {saved_iterator.last_position}')
        # print(f'current position: {saved_iterator.current_position}')
        # print(f'cardinality: {saved_iterator.cardinality}')
        print(saved_iterator)
        scans_state.append(saved_iterator)
    elif type(saved_iterator) is SavedIndexJoinIterator:
        left_iterator = getattr(saved_iterator, saved_iterator.WhichOneof('left'))
        right_iterator = getattr(saved_iterator, saved_iterator.WhichOneof('right'))
        flatten_scans(left_iterator, scans_state)
        flatten_scans(right_iterator, scans_state)
    else:
        raise Exception(f"Unsupported iterator type '{type(saved_iterator)}'")

# def estimate_coverage(saved_iterator: SavedProtobufPlan) -> float:
#     scan_iterators = list()
#     flatten_scans(saved_iterator, scan_iterators)
#     covered_space = 0
#     for i in range(len(scan_iterators)):
#         scan_iterator = scan_iterators[i]
#         if scan_iterator.current_position <= scan_iterator.last_position:
#             continue # no progress on this scan
#         progression = scan_iterator.current_position - scan_iterator.last_position
#         cardinality = scan_iterator.cardinality
#         previously_covered_space = 1
#         for j in range(i):
#             previous_scan_iterator = scan_iterators[j]
#             # print(f'previous scan cardinality: {previous_scan_iterator.cardinality}')
#             previously_covered_space *= (1 / previous_scan_iterator.cardinality)
#         # print(f'previously covered space = {previously_covered_space * 100} %')
#         # print(f'space covered by this scan = {(previously_covered_space * (progression / cardinality)) * 100} %')
#         covered_space += (previously_covered_space * (progression / cardinality))
#     # print(f'covered space = {covered_space * 100} %')
#     return covered_space


def get_scan_cardinality(
    scan_iterator: SavedScanIterator, runtime_cardinality: bool = True
) -> int:
    if runtime_cardinality:
        return scan_iterator.runtime_cardinality
    else:
        return scan_iterator.pattern_cardinality


def estimate_coverage(
    saved_iterator: SavedProtobufPlan, runtime_cardinality: bool = True
) -> float:
    scan_iterators = list()
    flatten_scans(saved_iterator, scan_iterators)
    space_covered = 0
    for i in range(len(scan_iterators)):
        scan_iterator = scan_iterators[i]
        if scan_iterator.produced == 0:
            break
        scan_cardinality = get_scan_cardinality(scan_iterator, runtime_cardinality=runtime_cardinality)
        scan_coverage = (scan_iterator.produced - 1) / (scan_cardinality)
        for j in range(i):
            previous_scan_iterator = scan_iterators[j]
            previous_scan_cardinality = get_scan_cardinality(previous_scan_iterator, runtime_cardinality=runtime_cardinality)
            scan_coverage *= (1 / previous_scan_cardinality)
        space_covered += scan_coverage
    return space_covered


def count_triples_scanned(saved_iterator: SavedProtobufPlan) -> int:
    if (type(saved_iterator) is SavedFilterIterator) or (type(saved_iterator) is SavedProjectionIterator):
        source_iterator = getattr(saved_iterator, saved_iterator.WhichOneof('source'))
        return count_triples_scanned(source_iterator)
    elif type(saved_iterator) is SavedScanIterator:
        return saved_iterator.produced
    elif (type(saved_iterator) is SavedIndexJoinIterator) or (type(saved_iterator) is SavedBagUnionIterator):
        left_iterator = getattr(saved_iterator, saved_iterator.WhichOneof('left'))
        right_iterator = getattr(saved_iterator, saved_iterator.WhichOneof('right'))
        return count_triples_scanned(left_iterator) + count_triples_scanned(right_iterator)
    else:
        raise Exception(f"Unknown iterator type '{type(saved_iterator)}'")
