# runtime.py
# Author: Thomas MINIER - MIT License 2017-2018
from rdflib import Literal, URIRef
from numbers import Number
from uuid import uuid4


def sparql_bind(b):
    """Return True if the binding is bound, False otherwise"""
    return b is not None


def sameTerm(x, y):
    """Return True if two terms are the same , i.e., have the same type and value"""
    return type(x) == type(y) and x == y


def isIRI(x):
    """Return True if the term is a RDF URI/IRI"""
    return x.startswith('http')


def isLiteral(x):
    """Return True if the term is a RDF Literal"""
    return type(x) is Literal


def isNumeric(x):
    """Return True if the term is an integer/float/double/etc"""
    return isinstance(x, Number)


def sparql_lang(x):
    """Return the LANG of a RDF Literal, or an empty string if it does not exists"""
    return x.lang if type(x) is Literal else ""


def sparql_datatype(x):
    """Return the Datatype of a typed RDF Literal, or an empty string if it does not exists"""
    # TODO complete this method for integer and float
    return x.datatype if type(x) is Literal else ""


def sparql_STRUUID():
    """Return a string that is the scheme specific part of UUID"""
    return uuid4().hex


def sparql_UUID():
    """Return a fresh IRI from the UUID URN scheme."""
    return "\"urn:uuid:{}\"".format(uuid4())


FILTER_RUNTIME = {
    "uuid4": uuid4,
    "Number": Number,
    "Literal": Literal,
    "URIRef": URIRef,
    "isIRI": isIRI,
    "isLiteral": isLiteral,
    "isNumeric": isNumeric,
    "sparql_bind": sparql_bind,
    "sparql_lang": sparql_lang,
    "sparql_STRUUID": sparql_STRUUID,
    "sparql_UUID": sparql_UUID,
    "sameTerm": sameTerm
}
