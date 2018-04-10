# utils.py
# Author: Thomas MINIER - MIT License 2017-2018
from collections import OrderedDict
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from json import dumps
from time import time

NotYet = 'Not Yet'


class IteratorExhausted(Exception):
    pass


class EmptyIterator(object):
    """An Iterator that yields nothing"""
    def __init__(self):
        super(EmptyIterator, self).__init__()

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def __len__(self):
        return 0

    def next(self):
        raise StopIteration()

    def peek(self):
        return self.next()

    def has_next(self):
        return False

    @property
    def nb_reads(self):
        return 0


def selection(triple, variables, formatter=None):
    """Apply selection on a RDF triple"""
    bindings = set()
    if formatter is not None:
        triple = formatter(triple)
    if variables[0] is not None:
        bindings.add((variables[0], triple[0]))
    if variables[1] is not None:
        bindings.add((variables[1], triple[1]))
    if variables[2] is not None:
        bindings.add((variables[2], triple[2]))
    return bindings


def apply_bindings(elt, bindings={}):
    """Try to apply bindings to a subject, predicate or object"""
    if not elt.startswith('?'):
        return elt
    return bindings[elt] if elt in bindings else elt


def vars_positions(subject, predicate, obj):
    """Find position of SPARQL variables in a triple pattern"""
    return [var if var.startswith('?') else None for var in [subject, predicate, obj]]


def itimeout(iterator, timeout=20):
    """Apply a timeout on an iterator: after `timeout`ms, no more read are allowed."""
    startTime = time()
    for v in iterator:
        elapsedTime = (time() - startTime) * 1000
        if v is NotYet and elapsedTime >= timeout:
            break
        elif v is not NotYet:
            yield v
            if elapsedTime >= timeout:
                break


def cleaniterator(iterator):
    """Build a clean iterator, i.e., filter out None values"""
    return filter(lambda v: v is not None, iterator)


def flattenValue(value):
    if type(value[1]) is list:
        return [(value[0], subval) for subval in value[1]]
    else:
        return [value]


def drop_while(relation, predicate):
    """Drop values while a predicate is met by the values read from the iterator.
    Returns the first value that doesn't meet the predicate.
    """
    v = relation.next()
    while v is not None and predicate(v):
        v = relation.next()
    return v


def collect_while(relation, predicate, firstVal=None):
    """Collect values from an iterator while they met a given predicate.
    Returns the collected values and the first value that doesn't meet the predicate
    """
    res = []
    if firstVal is not None:
        res += flattenValue(firstVal)
    v = relation.peek(None)
    while v is not None and predicate(v):
        res += flattenValue(relation.next())
        v = relation.peek(None)
    return res
