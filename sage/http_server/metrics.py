from typing import Union, List, Tuple

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.values import ValuesIterator
from sage.query_engine.iterators.scan import ScanIterator


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


def compute_coverage(iterators: List[Union[ScanIterator, ValuesIterator]], index: int) -> float:
    produced = iterators[index]._produced - 1
    cardinality, step = get_cardinality(iterators[index])
    coverage = (produced * step) / cardinality
    for k in range(index):
        cardinality, step = get_cardinality(iterators[k])
        coverage *= 1 / cardinality
    return coverage
