# utils.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, List, Optional, Tuple


class IteratorExhausted(Exception):
    """Exception raised when a closed iterator was requested to produce a value"""
    pass


class EmptyIterator(object):
    """An Iterator that yields nothing"""

    def __init__(self):
        super(EmptyIterator, self).__init__()

    def __len__(self) -> int:
        return 0

    async def next(self) -> None:
        raise IteratorExhausted()

    def peek(self) -> None:
        return self.next()

    def has_next(self) -> bool:
        return False


class ArrayIterator(object):
    def __init__(self, array: List[Dict[str, str]]):
        super(ArrayIterator, self).__init__()
        self._array = array

    def has_next(self) -> bool:
        return len(self._array) > 0

    def next(self) -> Optional[Dict[str, str]]:
        if not self.has_next():
            raise StopIteration()
        mu = self._array.pop(0)
        return mu


def selection(triple: Tuple[str, str, str], variables: List[str]) -> Dict[str, str]:
    """Apply selection on a RDF triple"""
    bindings = dict()
    if variables[0] is not None:
        bindings[variables[0]] = triple[0]
    if variables[1] is not None:
        bindings[variables[1]] = triple[1]
    if variables[2] is not None:
        bindings[variables[2]] = triple[2]
    return bindings


def apply_bindings(elt: str, bindings: Dict[str, str] = dict()) -> Dict[str, str]:
    """Try to apply bindings to a subject, predicate or object"""
    if not elt.startswith('?'):
        return elt
    return bindings[elt] if elt in bindings else elt


def vars_positions(subject: str, predicate: str, obj: str) -> List[str]:
    """Find position of SPARQL variables in a triple pattern"""
    return [var if var.startswith('?') else None for var in [subject, predicate, obj]]


def tuple_to_triple(s: str, p: str, o: str) -> Dict[str, str]:
    return {
        'subject': s,
        'predicate': p,
        'object': o
    }
