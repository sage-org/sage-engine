# utils.py
# Author: Thomas MINIER - MIT License 2017-2020
import re

from typing import Dict, List, Set, Tuple, Union
from rdflib.namespace import XSD
from rdflib.term import BNode, Literal, URIRef, Variable

RDFTerm = Union[Variable, URIRef, Literal]
TriplePattern = Tuple[RDFTerm, RDFTerm, RDFTerm]

exponent = r'[eE][+-]?[0-9]+'

r_integer = re.compile(r'[0-9]+')
r_decimal = re.compile(r'([0-9]+\.[0-9]*|\.[0-9]+)')
r_double = re.compile(rf'([0-9]+\.[0-9]*{exponent}|\.[0-9]+{exponent}|[0-9]+{exponent})')
r_boolean = re.compile(r'(true|false)')


def get_vars(triple: Dict[str, str]) -> Set[str]:
    """Get SPARQL variables in a triple pattern"""
    return set([v for k, v in triple.items() if v.startswith('?')])


def find_connected_pattern(variables: List[str], triples: List[Dict[str, str]]) -> Tuple[Dict[str, str], int, Set[str]]:
    """Find the first pattern in a set of triples pattern connected to a set of variables"""
    pos = 0
    for triple in triples:
        tripleVars = get_vars(triple['triple'])
        if len(variables & tripleVars) > 0:
            return triple, pos, variables | tripleVars
        pos += 1
    return None, None, variables


def equality_variables(subject: str, predicate: str, obj: str) -> Tuple[str, Tuple[str, str, str]]:
    """Find all variables from triple pattern with the same name, and then returns the equality expression + the triple pattern used to evaluate correctly the pattern.
    """
    if subject == predicate:
        return f"{subject} = {predicate + '__2'}", (subject, predicate + '__2', obj), ""
    elif subject == obj:
        return f"{subject} = {obj + '__2'}", (subject, predicate, obj + '__2')
    elif predicate == obj:
        return f"{predicate} = {obj + '__2'}", (subject, predicate, obj + '__2')
    return None, (subject, predicate, obj)


def format_literal(term: Literal) -> str:
    """Convert a rdflib Literal into the format used by SaGe.

    Argument: The rdflib Literal to convert.

    Returns: The RDF Literal in Sage text format.
    """
    lang = term.language
    dtype = term.datatype
    lit = str(term)
    if lang is not None or dtype is not None:
        return term.n3()
    if re.fullmatch(r_integer, lit):
        dtype = XSD.integer
    elif re.fullmatch(r_decimal, lit):
        dtype = XSD.decimal
    elif re.fullmatch(r_double, lit):
        dtype = XSD.double
    elif re.fullmatch(r_boolean, lit):
        dtype = XSD.boolean
    return Literal(lit, lang, dtype).n3()


def format_term(term: Union[BNode, Literal, URIRef, Variable]) -> str:
    """Convert a rdflib RDF Term into the format used by SaGe.

    Argument: The rdflib RDF Term to convert.

    Returns: The RDF term in Sage text format.
    """
    if isinstance(term, URIRef):
        return str(term)
    elif isinstance(term, BNode):
        return '?v_' + str(term)
    elif isinstance(term, Literal):
        return format_literal(term)
    else:
        return term.n3()


def localize_triples(triples: List[Dict[str, str]], graphs: List[str]) -> List[Dict[str, str]]:
    """Performs data localization of a set of triple patterns.

    Args:
      * triples: Triple patterns to localize.
      * graphs: List of RDF graphs URIs used for data localization.

    Yields:
      The localized triple patterns.
    """
    localized_triples = list()
    for (s, p, o) in triples:
        for graph in graphs:
            localized_triples.append({
                'subject': format_term(s),
                'predicate': format_term(p),
                'object': format_term(o),
                'graph': graph
            })
    return localized_triples


def get_quads_from_update(node: dict, default_graph: str) -> List[Tuple[str, str, str, str]]:
    """Get all quads from a SPARQL update operation (Delete or Insert).

    Args:
      * node: Node of the logical query execution plan.
      * default_graph: URI of the default RDF graph.

    Returns:
      The list of all N-Quads found in the input node.
    """
    quads = list()
    # first, gell all regular RDF triples, localized on the default RDF graph
    if node.triples is not None:
        quads += [(format_term(s), format_term(p), format_term(o), default_graph) for s, p, o in node.triples]
    # then, add RDF quads from all GRAPH clauses
    if node.quads is not None:
        for g, triples in node.quads.items():
            if len(triples) > 0:
                quads += [(format_term(s), format_term(p), format_term(o), format_term(g)) for s, p, o in triples]
    return quads


def fully_bounded(triple_pattern: TriplePattern) -> bool:
    s, p, o = triple_pattern
    if not isinstance(s, URIRef):
        return False
    elif not isinstance(p, URIRef):
        return False
    elif not (isinstance(o, URIRef) or isinstance(o, Literal)):
        return False
    else:
        return True
