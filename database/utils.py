# utils.py
# Author: Thomas MINIER - MIT License 2017-2018
from rdflib import Namespace, URIRef, Literal

# Commonly used namepsaces
RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
HYDRA = Namespace("http://www.w3.org/ns/hydra/core#")
VOID = Namespace("http://rdfs.org/ns/void#")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
DCTERMS = Namespace("http://purl.org/dc/terms/")


def string_to_literal(literal):
    if literal.startswith('"'):
        splited_literal = literal.split('"')
        value = splited_literal[1]
        right_part = splited_literal[2] if len(splited_literal) >= 2 else None
        datatype = None
        lang = None
        if right_part is not None and right_part.startswith("^^"):
            datatype = right_part.split("^^")[1].strip("<>")
        elif right_part is not None and right_part.startswith("@"):
            lang = right_part.split("@")[1]
        return Literal(value, datatype=datatype, lang=lang)
    return URIRef(literal)
