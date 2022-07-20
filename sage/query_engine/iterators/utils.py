from typing import Dict, List, Optional, Union, Any
from rdflib.term import Literal, URIRef, Variable
from rdflib.util import from_n3


class EmptyIterator():
    """
    An Iterator that yields nothing.
    """

    @property
    def cardinality(self) -> int:
        return 0

    async def next(self, context: Dict[str, Any] = {}) -> Optional[Dict[str, str]]:
        """
        Generates the next item from the iterator, following the iterator
        protocol.

        Parameters
        ----------
        context: Dict[str, Any]
            Global variables specific to the execution of the query.

        Returns
        -------
        None | Dict[str, Any]
            The next item produced by the iterator, or None if all items have
            been produced.
        """
        return None


class ArrayIterator():
    """
    An iterator that sequentially yields all items from a list.

    Parameters
    ----------
    array: List[Dict[str, str]]
        An array of solution mappings.
    """

    def __init__(self, array: List[Dict[str, str]]):
        self._array = array

    @property
    def cardinality(self) -> int:
        return len(self._array)

    async def next(self, context: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """
        Generates the next item from the iterator, following the iterator
        protocol.

        Parameters
        ----------
        context: Dict[str, Any]
            Global variables specific to the execution of the query.

        Returns
        -------
        None | Dict[str, Any]
            The next item produced by the iterator, or None if all items have
            been produced.
        """
        try:
            return self._array.pop(0)
        except StopIteration:
            return None


def tuple_to_triple(s: str, p: str, o: str) -> Dict[str, str]:
    """
    Convert a tuple-based triple pattern into a dict-based triple pattern.

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
    return {"subject": s, "predicate": p, "object": o}


def to_rdflib_term(value: str) -> Union[Literal, URIRef, Variable]:
    """
    Convert a N3 term to a RDFLib Term.

    Argument: A RDF Term in N3 format.

    Returns: The RDF Term in rdflib format.
    """
    if value.startswith('http'):
        return URIRef(value)
    elif '"^^http' in value:
        index = value.find('"^^http')
        value = f"{value[0:index+3]}<{value[index+3:]}>"
    return from_n3(value)
