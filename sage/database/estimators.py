# estimators.py
# Author: Thomas MINIER - MIT License 2017-2020
from sage.database.utils import get_kind


def pattern_shape_estimate(subject: str, predicate: str, obj: str) -> int:
    """Get the ordering number of a triple pattern, according to heurisitcs from [1].

    [1] Tsialiamanis et al., "Heuristics-based Query Optimisation for SPARQL", in EDBT 2012.

    Args:
      * subject: Subject of the triple pattern.
      * predicate: Predicate of the triple pattern.
      * obj: Object of the triple pattern.
    
    Returns:
      The ordering number of a triple pattern, as defined in [1].
    """
    kind = get_kind(subject, predicate, obj)
    if kind == 'spo':
        return 1
    elif kind == 's?o':
        return 2
    elif kind == '?po':
        return 3
    elif kind == 'sp?':
        return 4
    elif kind == '??o':
        return 5
    elif kind == 's??':
        return 6
    elif kind == '?p?':
        return 7
    return 8
