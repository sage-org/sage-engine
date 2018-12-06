# query_parser.py
# Author: Thomas MINIER - MIT License 2017-2018
from rdflib import URIRef, BNode
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery
from query_engine.iterators.projection import ProjectionIterator
from query_engine.iterators.union import BagUnionIterator
from query_engine.iterators.filter import FilterIterator
from query_engine.optimizer.plan_builder import build_left_plan


class UnsupportedSPARQL(Exception):
    """Thrown when a SPARQL feature is not supported by the Sage query engine"""
    pass


def format_triple(graph):
    """Get a function used to convert a rdflib RDF triple into the format used by Sage"""
    def __formatter(triple):
        s, p, o = triple
        return {
            'subject': format_term(s),
            'predicate': format_term(p),
            'object': format_term(o),
            'graph': graph
        }
    return __formatter


def format_term(term):
    """Convert a rdflib RDF Term into the format used by Sage"""
    if type(term) is URIRef:
        return str(term)
    elif type(term) is BNode:
        return '?v_' + str(term)
    else:
        return term.n3()


def fetch_graph_triples(node, current_graph, server_url):
    """Fetch triples in a BGP or a BGP nested in a GRAPH clause"""
    if node.name == 'Graph' and node.p.name == 'BGP':
        graph_uri = format_graph_uri(format_term(node.term), server_url)
        return list(map(format_triple(graph_uri), node.p.triples))
    elif node.name == 'BGP':
        return list(map(format_triple(current_graph), node.triples))
    else:
        raise UnsupportedSPARQL('Unsupported SPARQL Feature: a Sage engine can only perform joins between Graphs and BGPs')


def format_graph_uri(uri, server_url):
    """Format a GRAPH IRI if its belong to the same server than the current one"""
    if uri.startswith(server_url):
        index = uri.index(server_url)
        return uri[index + len(server_url):]
    return '_:UnkownGraph'


def parse_query(query, dataset, default_graph, server_url):
    """Parse a regular SPARQL query into a query execution plan"""
    q_parsed = translateQuery(parseQuery(query)).algebra
    cardinalities = dict()
    iterator = parse_query_node(q_parsed, dataset, default_graph, server_url, cardinalities)
    return iterator, cardinalities


def parse_query_node(node, dataset, current_graph, server_url, cardinalities):
    """
        Recursively parse node in the query logical plan to build a preemptable physical query execution plan.

        Args:
            * node - Node of the logical plan to parse (in rdflib format)
            * dataset - RDF dataset used to execute the query
            * current_graph - IRI of the current RDF graph queried
            * server_url - URL of the SaGe server
            * cardinalities - Map<triple,integer> used to track triple patterns cardinalities
    """
    if node.name == 'SelectQuery':
        return parse_query_node(node.p, dataset, current_graph, server_url, cardinalities)
    elif node.name == 'Project':
        query_vars = list(map(lambda t: '?' + str(t), node._vars))
        child = parse_query_node(node.p, dataset, current_graph, server_url, cardinalities)
        return ProjectionIterator(child, query_vars)
    elif node.name == 'BGP':
        # bgp_vars = node._vars
        triples = list(map(format_triple(current_graph), node.triples))
        iterator, query_vars, c = build_left_plan(triples, dataset, current_graph)
        # track cardinalities of every triple pattern
        cardinalities.update(c)
        return iterator
    elif node.name == 'Union':
        left = parse_query_node(node.p1, dataset, current_graph, server_url, cardinalities)
        right = parse_query_node(node.p2, dataset, current_graph, server_url, cardinalities)
        return BagUnionIterator(left, right)
    elif node.name == 'Filter':
        expression = parse_filter_expr(node.expr)
        iterator = parse_query_node(node.p, dataset, current_graph, server_url, cardinalities)
        return FilterIterator(iterator, expression)
    elif node.name == 'Join':
        # only allow for joining BGPs from different GRAPH clauses
        triples = fetch_graph_triples(node.p1, current_graph, server_url) + fetch_graph_triples(node.p2, current_graph, server_url)
        iterator, query_vars, c = build_left_plan(triples, dataset, current_graph)
        # track cardinalities of every triple pattern
        cardinalities.update(c)
        return iterator
    else:
        raise UnsupportedSPARQL("Unsupported SPARQL feature: {}".format(node.name))


def parse_filter_expr(expr):
    """Stringify a rdflib Filter expression"""
    if not hasattr(expr, 'name'):
        return format_term(expr)
    else:
        if expr.name == 'RelationalExpression':
            return "({} {} {})".format(parse_filter_expr(expr.expr), expr.op, parse_filter_expr(expr.other))
        elif expr.name == 'AdditiveExpression':
            expression = parse_filter_expr(expr.expr)
            for i in range(len(expr.op)):
                expression = "({} {} {})".format(expression, expr.op[i], parse_filter_expr(expr.other[i]))
            return expression
        elif expr.name == 'ConditionalAndExpression':
            expression = parse_filter_expr(expr.expr)
            for other in expr.other:
                expression = "({} && {})".format(expression, parse_filter_expr(other))
            return expression
        elif expr.name == 'ConditionalOrExpression':
            expression = parse_filter_expr(expr.expr)
            for other in expr.other:
                expression = "({} || {})".format(expression, parse_filter_expr(other))
            return expression
        raise UnsupportedSPARQL("Unsupported SPARQL FILTER expression: {}".format(expr.name))
