from typing import Optional


def is_var(term: Optional[str]) -> bool:
    """
    Test if a RDF term is a SPARQL variable.

    Parameters
    ----------
    term: None | str
        A RDF term to test.

    Returns
    -------
    bool
        True if the RDF term is a SPARQL variable, False otherwise.
    """
    return term is None or term.startswith("?")


def get_kind(subj: Optional[str], pred: Optional[str], obj: Optional[str]) -> str:
    """
    Get the type of a triple pattern.

    Possible types are ???, sp?, ?po, s?o, ?p?, s??, ??o and spo.

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
    str
        The type of the input triple pattern.

    Example
    -------
    >>> print(get_kind(None, "http://xmlns.com/foaf/0.1/", ""Bob"@en"))
        "?po"
    >>> print(get_kind(None, "http://xmlns.com/foaf/0.1/", None))
        "?p?"
    """
    if is_var(subj) and is_var(pred) and is_var(obj):
        return "???"
    elif not is_var(subj) and not is_var(pred) and is_var(obj):
        return "sp?"
    elif is_var(subj) and not is_var(pred) and not is_var(obj):
        return "?po"
    elif not is_var(subj) and is_var(pred) and not is_var(obj):
        return "s?o"
    elif is_var(subj) and not is_var(pred) and is_var(obj):
        return "?p?"
    elif not is_var(subj) and is_var(pred) and is_var(obj):
        return "s??"
    elif is_var(subj) and is_var(pred) and not is_var(obj):
        return "??o"
    return "spo"
