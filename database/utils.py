# utils.py
# Author: Thomas MINIER - MIT License 2017-2018


class DoubleDict(object):
    """A DoubleDict is a two-way dictionnary"""
    def __init__(self):
        super(DoubleDict, self).__init__()
        self._keys_to_values = dict()
        self._values_to_keys = dict()

    def insert(self, k, v):
        self._keys_to_values[k] = v
        self._values_to_keys[v] = k

    def key_get(self, k, default=None):
        return self._keys_to_values[k] if k in self._keys_to_values else default

    def value_get(self, v, default=None):
        return self._values_to_keys[v] if v in self._values_to_keys else default

    def has_key(self, k):
        return k in self._keys_to_values

    def has_value(self, v):
        return v in self._values_to_keys


class TripleDictionnary(object):
    """A TripleDictionnary stores RDF triples in bitmap format"""
    def __init__(self):
        super(TripleDictionnary, self).__init__()
        self._subjectDict = DoubleDict()
        self._predicateDict = DoubleDict()
        self._objectDict = DoubleDict()
        self._bitSubjectValue = 1
        self._bitPredicateValue = 1
        self._bitObjectValue = 1

    def insert_triple(self, s, p, o):
        if not self._subjectDict.has_value(s):
            self._subjectDict.insert(self._bitSubjectValue, s)
            self._bitSubjectValue += 1
        if not self._predicateDict.has_value(p):
            self._predicateDict.insert(self._bitPredicateValue, p)
            self._bitPredicateValue += 1
        if not self._objectDict.has_value(o):
            self._objectDict.insert(self._bitObjectValue, o)
            self._bitObjectValue += 1
        return self.triple_to_bit(s, p, o)

    def triple_to_bit(self, s, p, o):
        return (self._subjectDict.value_get(s, 0), self._predicateDict.value_get(p, 0), self._objectDict.value_get(o, 0))

    def bit_to_triple(self, s, p, o):
        return (self._subjectDict.key_get(s), self._predicateDict.key_get(p), self._objectDict.key_get(o))
