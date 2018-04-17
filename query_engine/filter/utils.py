# utils.py
# Author: Thomas MINIER - MIT License 2017-2018
from rdflib import Literal
from rdflib.namespace import XSD


def string_to_rdf(literal):
    """Turn a string into a RDFlib URI or Literal.
    Returns the original string if the string is a SPARQL variable"""
    if literal.startswith('?'):
        return literal
    elif literal.startswith('http'):
        return "'{}'".format(literal)
    elif literal.startswith('"'):
        splited_literal = literal.split('"')
        value = splited_literal[1]
        right_part = splited_literal[2] if len(splited_literal) >= 2 else None
        datatype = None
        lang = None
        if right_part is not None and right_part.startswith("^^"):
            datatype = right_part.split("^^")[1].strip("<>")
        elif right_part is not None and right_part.startswith("@"):
            lang = right_part.split("@")[1]
        if datatype is None and lang is None:
            return literal
        elif datatype == 'http://www.w3.org/2001/XMLSchema#date' or datatype == 'http://www.w3.org/2001/XMLSchema#dateTime':
            return "strptime(\"{}\", \"%Y-%m-%dT%H:%M:%S\")".format(value)
        return Literal(value, datatype=datatype, lang=lang)
    return "'{}'".format(literal)
