from typing import Union, List

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.values import ValuesIterator
from sage.query_engine.iterators.scan import ScanIterator


def coverage(plan: PreemptableIterator) -> float:
    return estimate_coverage(plan)


def triples_scanned(plan: PreemptableIterator) -> float:
    return count_triples_scanned(plan)


def flatten_leaves(
    iterator: PreemptableIterator, iterators: List[Union[ScanIterator, ValuesIterator]]
) -> None:
    if (iterator.serialized_name() == 'filter') or (iterator.serialized_name() == 'proj'):
        flatten_leaves(iterator._source, iterators)
    elif (iterator.serialized_name() == 'scan') or (iterator.serialized_name() == 'values'):
        iterators.append(iterator)
    elif (iterator.serialized_name() == 'join') or (iterator.serialized_name() == 'union'):
        flatten_leaves(iterator._left, iterators)
        flatten_leaves(iterator._right, iterators)
    else:
        raise Exception(f'Unsupported iterator type {iterator.serialized_name()}')


def get_cardinality(iterator: Union[ScanIterator, ValuesIterator]) -> int:
    if (iterator._runtime_cardinality == 0) and (iterator._produced > 0):
        return iterator._cardinality
    else:
        return iterator._runtime_cardinality


def estimate_coverage(iterator: PreemptableIterator) -> float:
    leaf_iterators = list()
    flatten_leaves(iterator, leaf_iterators)
    space_covered = 0
    for i in range(len(leaf_iterators)):
        leaf_iterator = leaf_iterators[i]
        if leaf_iterator._produced == 0:
            break
        iterator_cardinality = get_cardinality(leaf_iterator)
        iterator_coverage = (leaf_iterator._produced - 1) / iterator_cardinality
        for j in range(i):
            previous_leaf_iterator = leaf_iterators[j]
            previous_iterator_cardinality = get_cardinality(previous_leaf_iterator)
            iterator_coverage *= (1 / previous_iterator_cardinality)
        space_covered += iterator_coverage
    return space_covered


def count_triples_scanned(iterator: PreemptableIterator) -> int:
    if (iterator.serialized_name() == 'filter') or (iterator.serialized_name() == 'proj'):
        return count_triples_scanned(iterator._source)
    elif iterator.serialized_name() == 'scan':
        return iterator._produced
    elif iterator.serialized_name() == 'values':
        return 0
    elif (iterator.serialized_name() == 'join') or (iterator.serialized_name() == 'union'):
        return count_triples_scanned(iterator._left) + count_triples_scanned(iterator._right)
    else:
        raise Exception(f'Unsupported iterator type {iterator.serialized_name()}')
