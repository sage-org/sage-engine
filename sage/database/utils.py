# utils.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Optional


def is_var(term: Optional[str]) -> bool:
    """Return True if a RDF term is a SPARQL variable (i.e., is None)"""
    return term is None


def get_kind(subj: Optional[str], pred: Optional[str], obj: Optional[str]) -> str:
    """Get the type of a given triple pattern"""
    if is_var(subj) and is_var(pred) and is_var(obj):
        return '???'
    elif not is_var(subj) and not is_var(pred) and is_var(obj):
        return 'sp?'
    elif is_var(subj) and not is_var(pred) and not is_var(obj):
        return '?po'
    elif not is_var(subj) and is_var(pred) and not is_var(obj):
        return 's?o'
    elif is_var(subj) and not is_var(pred) and is_var(obj):
        return '?p?'
    elif not is_var(subj) and is_var(pred) and is_var(obj):
        return 's??'
    elif is_var(subj) and is_var(pred) and not is_var(obj):
        return '??o'
    else:
        return 'spo'
