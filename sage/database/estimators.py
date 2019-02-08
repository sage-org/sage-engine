# estimators.py
# Author: Thomas MINIER - MIT License 2017-2018
from sage.database.utils import get_kind


def pattern_shape_estimate(subject, predicate, obj):
    """
        Get the ordering number of a triple pattern, according to heurisitcs from
        Tsialiamanis et al., 'Heuristics-based Query Optimisation for SPARQL', in EDBT 2012
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
