# utils.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, List, Optional, Tuple
import math

class EmptyIterator(object):
    """An Iterator that yields nothing"""

    def __init__(self):
        super(EmptyIterator, self).__init__()

    def __len__(self) -> int:
        return 0

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return False

    async def next(self) -> None:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        return  None



class ArrayIterator(object):
    """An iterator that sequentially yields all items from a list.

    Argument: List of solution mappings.
    """

    def __init__(self, array: List[Dict[str, str]]):
        super(ArrayIterator, self).__init__()
        self._array = array

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return len(self._array) > 0

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        if not self.has_next():
            return None
        mu = self._array.pop(0)
        return mu


def selection(triple: Tuple[str, str, str], variables: List[str]) -> Dict[str, str]:
    """Apply a selection on a RDF triple, producing a set of solution mappings.

    Args:
      * triple: RDF triple on which the selection is applied.
      * variables: Input variables of the selection.

    Returns:
      A set of solution mappings built from the selection results.

    Example:
      >>> triple = (":Ann", "foaf:knows", ":Bob")
      >>> variables = ["?s", None, "?knows"]
      >>> selection(triple, variables)
      { "?s": ":Ann", "?knows": ":Bob" }
    """
    bindings = dict()
    if variables[0] is not None:
        bindings[variables[0]] = triple[0]
    if variables[1] is not None:
        bindings[variables[1]] = triple[1]
    if variables[2] is not None:
        bindings[variables[2]] = triple[2]
    return bindings


def find_in_mappings(variable: str, mappings: Dict[str, str] = dict()) -> str:
    """Find a substitution for a SPARQL variable in a set of solution mappings.

    Args:
      * variable: SPARQL variable to look for.
      * bindings: Set of solution mappings to search in.

    Returns:
      The value that can be substituted for this variable.

    Example:
      >>> mappings = { "?s": ":Ann", "?knows": ":Bob" }
      >>> find_in_mappings("?s", mappings)
      ":Ann"
      >>> find_in_mappings("?unknown", mappings)
      "?unknown"
    """
    if not variable.startswith('?'):
        return variable
    return mappings[variable] if variable in mappings else variable


def vars_positions(subject: str, predicate: str, obj: str) -> List[str]:
    """Find the positions of SPARQL variables in a triple pattern.

    Args:
      * subject: Subject of the triple pattern.
      * predicate: Predicate of the triple pattern.
      * obj: Object of the triple pattern.

    Returns:
      The positions of SPARQL variables in the input triple pattern.

    Example:
      >>> vars_positions("?s", "http://xmlns.com/foaf/0.1/name", '"Ann"@en')
      [ "?s", None, None ]
      >>> vars_positions("?s", "http://xmlns.com/foaf/0.1/name", "?name")
      [ "?s", None, "?name" ]
    """
    return [var if var.startswith('?') else None for var in [subject, predicate, obj]]


def tuple_to_triple(s: str, p: str, o: str) -> Dict[str, str]:
    """Convert a tuple-based triple pattern into a dict-based triple pattern.

    Args:
      * s: Subject of the triple pattern.
      * p: Predicate of the triple pattern.
      * o: Object of the triple pattern.

    Returns:
      The triple pattern as a dictionnary.

    Example:
      >>> tuple_to_triple("?s", "foaf:knows", ":Bob")
      { "subject": "?s", "predicate": "foaf:knows", "object": "Bob" }
    """
    return {
        'subject': s,
        'predicate': p,
        'object': o
    }

def worker_range(card, nb, i):
    """
    Args:
    * card: cardinality of the scan.
    * nb: number of nbworkers
    * i: id of the worker

    examples:
    worker_range(124,3,2):
        42,82

    card:124, worker:1, start:0, end:41
    card:124, worker:2, start:42, end:82
    card:124, worker:3, start:83, end:124
    card:3, worker:1, start:0, end:0
    card:3, worker:2, start:1, end:1
    card:3, worker:3, start:2, end:2
    card:1, worker:1, start:0, end:0
    card:1, worker:2, start:-1, end:-1
    card:1, worker:3, start:-1, end:-1
    card:2, worker:1, start:0, end:0
    card:2, worker:2, start:1, end:1
    card:2, worker:3, start:-1, end:-1
    """
    if card < 1 or nb < 1 or i > nb or i < 1:
        raise("invalid range call")
    if card <= nb:
        if i <= card:
            return i - 1, i - 1
        else:
            return -1, -1
    start = card / nb * (i - 1)
    end = start + card / nb
    return math.ceil(start), math.floor(end)
