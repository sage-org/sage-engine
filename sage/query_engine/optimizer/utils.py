import re

from typing import List, Set, Tuple, Optional
from rdflib.namespace import XSD
from rdflib.term import BNode, Literal, URIRef

from sage.query_engine.types import (
    RDFLibTerm, RDFLibTriplePattern, RDFLibMappings, RDFLibOperator,
    TriplePattern, Mappings)

exponent = r'[eE][+-]?[0-9]+'

r_integer = re.compile(r'[0-9]+')
r_decimal = re.compile(r'([0-9]+\.[0-9]*|\.[0-9]+)')
r_double = re.compile(rf'([0-9]+\.[0-9]*{exponent}|\.[0-9]+{exponent}|[0-9]+{exponent})')
r_boolean = re.compile(r'(true|false)')


def get_vars(triple_pattern: TriplePattern) -> Set[str]:
    """
    Extracts SPARQL variables in a triple pattern.

    Parameters
    ----------
    triple_pattern: TriplePattern
        The triple pattern from which we want to extract the variables.

    Returns
    -------
    Set[str]
        The variables that appear in the triple pattern.
    """
    return set([v for k, v in triple_pattern.items() if v.startswith('?')])


def equality_variables(
    subject: str, predicate: str, obj: str
) -> Tuple[str, Tuple[str, str, str]]:
    """
    Finds all variables with the same name in a triple pattern, and then returns
    the equality expression with the triple pattern used to correctly evaluate
    the pattern.
    """
    if subject == predicate:
        return f"{subject} = {predicate + '__2'}", (subject, predicate + '__2', obj), ""
    elif subject == obj:
        return f"{subject} = {obj + '__2'}", (subject, predicate, obj + '__2')
    elif predicate == obj:
        return f"{predicate} = {obj + '__2'}", (subject, predicate, obj + '__2')
    return None, (subject, predicate, obj)


def format_literal(literal: Literal) -> str:
    """
    Converts an RDFLib Literal into the format used by SaGe.

    Parameters
    ----------
    literal: Literal
        The RDFLib Literal to convert.

    Returns
    -------
    str
        The RDF Literal in the SaGe text format.
    """
    lang = literal.language
    dtype = literal.datatype
    value = str(literal)
    if lang is not None or dtype is not None:
        return literal.n3()
    if re.fullmatch(r_integer, value):
        dtype = XSD.integer
    elif re.fullmatch(r_decimal, value):
        dtype = XSD.decimal
    elif re.fullmatch(r_double, value):
        dtype = XSD.double
    elif re.fullmatch(r_boolean, value):
        dtype = XSD.boolean
    return Literal(value, lang, dtype).n3()


def format_term(term: RDFLibTerm) -> str:
    """
    Converts an RDFLib term into the format used by SaGe.

    Parameters
    ----------
    term: RDFLibTerm
        The RDFLib term to convert.

    Returns
    -------
    str
        The RDF term in the SaGe text format.
    """
    if isinstance(term, URIRef):
        return str(term)
    elif isinstance(term, BNode):
        return '?v_' + str(term)
    elif isinstance(term, Literal):
        return format_literal(term)
    return term.n3()


def format_mappings(mappings: RDFLibMappings) -> Mappings:
    """
    Converts RDFLib mappings into the format used by SaGe.

    Parameters
    ----------
    mappings: RDFLibMappings
        The RDFLib mappings to convert.

    Returns
    -------
    Mappings
        The RDF mappings formated for SaGe.
    """
    formated_mappings = dict()
    for variable, value in mappings.items():
        variable = format_term(variable)
        value = format_term(value)
        formated_mappings[variable] = value
    return formated_mappings


def format_solution_mappings(solutions: List[RDFLibMappings]) -> List[Mappings]:
    """
    Converts a list of RDFLib mappings into the format used by SaGe.

    Parameters
    ----------
    solutions: List[RDFLibMappings]
        A list of RDFLib mappings to convert.

    Returns
    -------
    List[Mappings]
        The list of RDF mappings formated for SaGe.
    """
    return [format_mappings(mappings) for mappings in solutions]


def format_triple_pattern(
    triple_pattern: RDFLibTriplePattern, graph: Optional[str] = None
) -> TriplePattern:
    """
    Converts an RDFLib triple pattern into the format used by SaGe.

    Parameters
    ----------
    triple_pattern: RDFLibTriplePattern
        The RDFLib triple pattern to convert.
    graph: None | str
        The RDF Graph on which the triple is to be evaluated.

    Returns
    -------
    TriplePattern
        The triple pattern formated for SaGe.
    """
    return {
        'subject': format_term(triple_pattern[0]),
        'predicate': format_term(triple_pattern[1]),
        'object': format_term(triple_pattern[2]),
        'graph': graph}


def localize_triples(
    triples: List[TriplePattern], graphs: List[str]
) -> List[TriplePattern]:
    """
    Performs data localization of a set of triple patterns.

    Parameters
    ----------
    triples: List[TriplePattern]
        Triple patterns to localize.
    graphs: List[str]
        List of RDF graphs URIs used for data localization.

    Returns
    -------
    List[TriplePattern]
        The localized triple patterns.
    """
    localized_triples = list()
    for (s, p, o) in triples:
        for graph in graphs:
            localized_triples.append({
                'subject': format_term(s),
                'predicate': format_term(p),
                'object': format_term(o),
                'graph': graph})
    return localized_triples


def get_quads_from_update(node: RDFLibOperator, default_graph: str) -> List[Tuple[str, str, str, str]]:
    """
    Get all quads from a SPARQL update operation (Delete or Insert).

    Parameters
    ----------
    node: RDFLibOperator
        Node of the logical query execution plan.
    default_graph: str
        URI of the default RDF graph.

    Returns
    -------
    List[Tuple[str, str, str, str]]
        The list of all N-Quads found in the input node.
    """
    quads = list()
    # first, get all regular RDF triples, localized on the default RDF graph
    if node.triples is not None:
        quads += [(format_term(s), format_term(p), format_term(o), default_graph) for s, p, o in node.triples]
    # then, adds RDF quads from all GRAPH clauses
    if node.quads is not None:
        for g, triples in node.quads.items():
            if len(triples) > 0:
                quads += [(format_term(s), format_term(p), format_term(o), format_term(g)) for s, p, o in triples]
    return quads
