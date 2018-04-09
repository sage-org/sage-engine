# utils.py
# Author: Thomas MINIER - MIT License 2017-2018


def get_vars(triple):
    """Get variables in a triple pattern"""
    return set([v for k, v in triple.items() if v.startswith('?')])


def find_connected_pattern(variables, triples):
    """Find the first pattern in a set of triples pattern connected to a set of variables"""
    pos = 0
    for triple in triples:
        tripleVars = get_vars(triple['triple'])
        if len(variables & tripleVars) > 0:
            return triple, pos, variables | tripleVars
        pos += 1
    return None, None, variables
