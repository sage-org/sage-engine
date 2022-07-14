from typing import Dict, List, Optional, Tuple, Union, Any
from rdflib.plugins.sparql.sparql import Bindings, QueryContext
from rdflib.plugins.sparql.parserutils import Expr
from rdflib.term import Literal, URIRef, Variable
from rdflib.util import from_n3


class EmptyIterator(object):
    """An Iterator that yields nothing"""

    def __init__(self):
        super(EmptyIterator, self).__init__()

    def __len__(self) -> int:
        return 0

    async def next(self) -> None:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        return None


class ArrayIterator(object):
    """An iterator that sequentially yields all items from a list.

    Argument: List of solution mappings.
    """

    def __init__(self, array: List[Dict[str, str]], produced: int = 0):
        super(ArrayIterator, self).__init__()
        self._array = array
        self._produced = produced

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        try:
            mappings = self._array.pop(0)
            self._produced += 1
            return mappings
        except StopIteration:
            return None
        if not self.has_next():
            return None


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


def to_rdflib_term(value: str) -> Union[Literal, URIRef, Variable]:
    """Convert a N3 term to a RDFLib Term.

    Argument: A RDF Term in N3 format.

    Returns: The RDF Term in rdflib format.
    """
    if value.startswith('http'):
        return URIRef(value)
    elif '"^^http' in value:
        index = value.find('"^^http')
        value = f"{value[0:index+3]}<{value[index+3:]}>"
    return from_n3(value)


def eval_rdflib_expr(expr: Expr, mappings: Dict[str, str]) -> Any:
    """Evaluate the FILTER expression with a set mappings.

    Argument: A set of solution mappings.

    Returns: The outcome of evaluating the SPARQL FILTER on the input set of solution mappings.
    """
    if isinstance(expr, Variable):
        return mappings[expr.n3()]
    d = {Variable(key[1:]): to_rdflib_term(value) for key, value in mappings.items()}
    context = QueryContext(bindings=Bindings(d=d))
    return expr.eval(context)
