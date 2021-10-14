from typing import Union, List, Tuple

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.values import ValuesIterator
from sage.query_engine.iterators.scan import ScanIterator


# def cost(plan: PreemptableIterator) -> float:
#     iterators = flatten_leaves(plan)
#     cardinalities = [0.0] * len(iterators)
#     for i in range(len(iterators)):
#         if (iterators[i]._produced == 0) and (iterators[i]._stages == 0) and (i > 0):
#             left_cardinality = cardinalities[i - 1]
#             right_cardinality, _ = get_cardinality(iterators[i])
#             # print(f'Cout({iterators[i]._pattern}) = {left_cardinality} x {right_cardinality} = {left_cardinality * right_cardinality}')
#             cardinalities[i] = left_cardinality * right_cardinality
#         else:
#             # print(f'Cout({iterators[i]._pattern}) = {compute_cout(iterators[i])}')
#             cardinalities[i] = compute_cout(iterators[i])
#     return sum(cardinalities)


def coverage(plan: PreemptableIterator) -> float:
    iterators = flatten_leaves(plan)
    coverage = 0
    for i in range(len(iterators)):
        if iterators[i]._produced == 0:
            break
        coverage += compute_coverage(iterators, i)
    return coverage


def flatten_leaves(iterator: PreemptableIterator) -> None:
    if (iterator.serialized_name() == 'filter') or (iterator.serialized_name() == 'proj'):
        return flatten_leaves(iterator._source)
    elif (iterator.serialized_name() == 'scan') or (iterator.serialized_name() == 'values'):
        return [iterator]
    elif (iterator.serialized_name() == 'join') or (iterator.serialized_name() == 'union'):
        return flatten_leaves(iterator._left) + flatten_leaves(iterator._right)
    else:
        raise Exception(f'Unsupported iterator type {iterator.serialized_name()}')


def get_cardinality(iterator: Union[ScanIterator, ValuesIterator]) -> Tuple[int, float]:
    if iterator.serialized_name() == 'scan':
        cardinality = max(iterator._pattern_cardinality, iterator._produced)
        step = iterator._pattern_cardinality / iterator._cardinality
    else:
        cardinality = max(iterator._cardinality, iterator._produced)
        step = 1
    return cardinality, step


# def compute_cout(iterator: Union[ScanIterator, ValuesIterator]) -> float:
#     if iterator.serialized_name() == 'scan':
#         if iterator._cumulative_cardinality == 0:
#             cardinality = iterator._cardinality
#         else:
#             cardinality = iterator._cumulative_cardinality
#         # print(iterator._pattern)
#         # print(f'avg card: {cardinality / (iterator._stages + 1)} - stages: {iterator._stages} - card: {cardinality}')
#         return cardinality / (iterator._stages + 1)
#     else:
#         return iterator._cardinality


def compute_coverage(iterators: List[Union[ScanIterator, ValuesIterator]], index: int) -> float:
    produced = iterators[index]._produced - 1
    cardinality, step = get_cardinality(iterators[index])
    coverage = (produced * step) / cardinality
    for k in range(index):
        cardinality, step = get_cardinality(iterators[k])
        coverage *= 1 / cardinality
    return coverage
