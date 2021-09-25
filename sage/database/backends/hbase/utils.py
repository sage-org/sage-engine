# utils.py
# Author: Thomas MINIER - MIT License 2019
from hashlib import md5


def hash_term(t):
    """Hash a RDF Term to encode it as as key"""
    m = md5()
    m.update(t)
    return m.hexdigest()


def build_row_key(s: str, p: str, o: str) -> str:
    """Build a row key from a triple of RDF terms"""
    key = list()
    for x in [s, p, o]:
        if x is not None:
            key.append(hash_term(x.encode('utf-8')))
    return '_'.join(key)
