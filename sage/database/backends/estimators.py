from sage.database.backends.utils import get_kind


def pattern_shape_estimate(subject: str, predicate: str, obj: str) -> int:
    """
    Get the ordering number of a triple pattern, according to the heurisitc
    defined in [1].

    [1] Tsialiamanis et al., "Heuristics-based Query Optimisation for SPARQL", in EDBT 2012.

    Parameters
    ----------
    subject: str
        Subject of the triple pattern.
    predicate: str
        Predicate of the triple pattern.
    obj: str
        Object of the triple pattern.

    Returns
    -------
    int
        The ordering number of a triple pattern, as defined in [1].
    """
    kind = get_kind(subject, predicate, obj)
    if kind == "spo":
        return 1
    elif kind == "s?o":
        return 2
    elif kind == "?po":
        return 3
    elif kind == "sp?":
        return 4
    elif kind == "??o":
        return 5
    elif kind == "s??":
        return 6
    elif kind == "?p?":
        return 7
    return 8
