# utils.py
# Author: Thomas MINIER - MIT License 2017-2018


class IteratorExhausted(Exception):
    """Exception raised when a closed iterator was requested to produce a value"""
    pass


class EmptyIterator(object):
    """An Iterator that yields nothing"""

    def __init__(self):
        super(EmptyIterator, self).__init__()

    def __len__(self):
        return 0

    async def next(self):
        raise IteratorExhausted()

    def peek(self):
        return self.next()

    def has_next(self):
        return False

    @property
    def nb_reads(self):
        return 0

    @property
    def offset(self):
        return 0


class ArrayIterator(object):
    def __init__(self, array):
        super(ArrayIterator, self).__init__()
        self._array = array

    def has_next(self):
        return len(self._array) > 0

    def next(self):
        if not self.has_next():
            raise StopIteration()
        mu = self._array.pop(0)
        return mu


def selection(triple, variables):
    """Apply selection on a RDF triple"""
    bindings = dict()
    if variables[0] is not None:
        bindings[variables[0]] = triple[0]
    if variables[1] is not None:
        bindings[variables[1]] = triple[1]
    if variables[2] is not None:
        bindings[variables[2]] = triple[2]
    return bindings


def apply_bindings(elt, bindings={}):
    """Try to apply bindings to a subject, predicate or object"""
    if not elt.startswith('?'):
        return elt
    return bindings[elt] if elt in bindings else elt


def vars_positions(subject, predicate, obj):
    """Find position of SPARQL variables in a triple pattern"""
    return [var if var.startswith('?') else None for var in [subject, predicate, obj]]


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


def tuple_to_triple(s, p, o):
    return {
        'subject': s,
        'predicate': p,
        'object': o
    }
