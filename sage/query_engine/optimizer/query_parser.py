# query_parser.py
# Author: Thomas MINIER - MIT License 2017-2018
# import pyparsing
# pyparsing.ParserElement.enablePackrat()
from rdflib import URIRef, BNode, Variable
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate
from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.update.insert import InsertOperator
from sage.query_engine.update.delete import DeleteOperator
from sage.query_engine.update.if_exists import IfExistsOperator
from sage.query_engine.update.update_sequence import UpdateSequenceOperator
from sage.query_engine.update.serializable import SerializableUpdate
from sage.query_engine.optimizer.plan_builder import build_left_plan
from sage.query_engine.exceptions import UnsupportedSPARQL
from sage.http_server.utils import format_graph_uri
from datetime import datetime
from pyparsing import ParseException
from enum import Enum


class ConsistencyLevel(Enum):
    """The consistency level choosen for executing the query"""
    ATOMIC_PER_ROW = 1
    SERIALIZABLE = 2
    ATOMIC_PER_QUANTUM = 3


def localize_triples(triples, graphs):
    """Using a set of RDF graphs, performs data localization of a set of RDF triples"""
    for t in triples:
        s, p, o = format_term(t[0]), format_term(t[1]), format_term(t[2])
        for graph in graphs:
            yield {
                'subject': s,
                'predicate': p,
                'object': o,
                'graph': graph
            }


def format_term(term):
    """Convert a rdflib RDF Term into the format used by SaGe"""
    if type(term) is URIRef:
        return str(term)
    elif type(term) is BNode:
        return '?v_' + str(term)
    else:
        return term.n3()


def get_triples_from_graph(node, current_graphs, server_url):
    """Fetch triples in a BGP or a BGP nested in a GRAPH clause"""
    if node.name == 'Graph' and node.p.name == 'BGP':
        graph_uri = format_graph_uri(format_term(node.term), server_url)
        return list(localize_triples(node.p.triples, [graph_uri]))
    elif node.name == 'BGP':
        return list(localize_triples(node.triples, current_graphs))
    else:
        raise UnsupportedSPARQL('Unsupported SPARQL Feature: a Sage engine can only perform joins between Graphs and BGPs')

def get_quads_from_update(operation, default_graph, server_url):
    """Get all quads from a SPARQL update operation (Delete or Insert)"""
    quads = list()
    # first, gell all regular RDF triples, localized on the default RDF graph
    if operation.triples is not None:
        quads += [(format_term(s), format_term(p), format_term(o), default_graph) for s, p, o in operation.triples]
    # then, add RDF quads from all GRAPH clauses
    if operation.quads is not None:
        for g, triples in operation.quads.items():
            if len(triples) > 0:
                graph_uri = format_graph_uri(format_term(g), server_url)
                quads += [(format_term(s), format_term(p), format_term(o), graph_uri) for s, p, o in triples]
    return quads

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


def parse_query(query, dataset, default_graph, server_url):
    """Parse a regular SPARQL query into a query execution plan"""
    # rdflib has no tool for parsing both read and update query,
    # so we must rely on a try/catch dirty trick...
    try:
        logical_plan = translateQuery(parseQuery(query)).algebra
        cardinalities = list()
        iterator = parse_query_node(logical_plan, dataset, [default_graph], server_url, cardinalities)
        return iterator, cardinalities
    except ParseException:
        return parse_update(query, dataset, default_graph, server_url)


def parse_query_node(node, dataset, current_graphs, server_url, cardinalities):
    """
        Recursively parse node in the query logical plan to build a preemptable physical query execution plan.

        Args:
            * node - Node of the logical plan to parse (in rdflib format)
            * dataset - RDF dataset used to execute the query
            * current_graphs - List of IRI of the current RDF graph queried
            * server_url - URL of the SaGe server
            * cardinalities - Map<triple,integer> used to track triple patterns cardinalities
    """
    if node.name == 'SelectQuery':
        # in case of a FROM clause, set the new default graphs used
        graphs = current_graphs
        if node.datasetClause is not None:
            graphs = [format_graph_uri(format_term(graph_iri.default), server_url) for graph_iri in node.datasetClause]
        return parse_query_node(node.p, dataset, graphs, server_url, cardinalities)
    elif node.name == 'Project':
        query_vars = list(map(lambda t: '?' + str(t), node._vars))
        child = parse_query_node(node.p, dataset, current_graphs, server_url, cardinalities)
        return ProjectionIterator(child, query_vars)
    elif node.name == 'BGP':
        # bgp_vars = node._vars
        triples = list(localize_triples(node.triples, current_graphs))
        iterator, query_vars, c = build_left_plan(triples, dataset, current_graphs)
        # track cardinalities of every triple pattern
        cardinalities += c
        return iterator
    elif node.name == 'Union':
        left = parse_query_node(node.p1, dataset, current_graphs, server_url, cardinalities)
        right = parse_query_node(node.p2, dataset, current_graphs, server_url, cardinalities)
        return BagUnionIterator(left, right)
    elif node.name == 'Filter':
        expression = parse_filter_expr(node.expr)
        iterator = parse_query_node(node.p, dataset, current_graphs, server_url, cardinalities)
        return FilterIterator(iterator, expression)
    elif node.name == 'Join':
        # only allow for joining BGPs from different GRAPH clauses
        triples = get_triples_from_graph(node.p1, current_graphs, server_url) + get_triples_from_graph(node.p2, current_graphs, server_url)
        iterator, query_vars, c = build_left_plan(triples, dataset, current_graphs)
        # track cardinalities of every triple pattern
        cardinalities += c
        return iterator
    else:
        raise UnsupportedSPARQL("Unsupported SPARQL feature: {}".format(node.name))


def parse_update(query, dataset, default_graph, server_url, consistency=ConsistencyLevel.ATOMIC_PER_ROW):
    """
        Parse a SPARQL INSERT DATA or DELETE DATA query, and returns a preemptable physical query execution plan to execute it.
    """
    operations = translateUpdate(parseUpdate(query))
    if len(operations) > 1:
        raise UnsupportedSPARQL("Only a single INSERT DATA/DELETE DATA is permitted by query. Consider sending yourt query in multiple SPARQL queries.")
    operation = operations[0]
    if operation.name == 'InsertData' or operation.name == 'DeleteData':
        # create RDF quads to insert/delete into/from the default graph
        quads = get_quads_from_update(operation, default_graph, server_url)
        # build the preemptable update operator used to insert/delete RDF triples
        if operation.name == 'InsertData':
            return InsertOperator(quads, dataset, server_url), dict()
        else:
            return DeleteOperator(quads, dataset, server_url), dict()
    elif operation.name == 'Modify':
        where_root = operation.where
        # unravel shitty things chained together
        if where_root.name == 'Join':
            if where_root.p1.name == 'BGP' and len(where_root.p1.triples) == 0:
                where_root = where_root.p2
            elif where_root.p2.name == 'BGP' and len(where_root.p2.triples) == 0:
                where_root = where_root.p1

        # for consistency = serializable, use a SerializableUpdate iterator
        if consistency == ConsistencyLevel.SERIALIZABLE:
            # build the read iterator
            read_iterator = parse_query_node(where_root, dataset, [default_graph], server_url, dict())
            # get the delete and/or insert templates
            delete_templates = list()
            insert_templates = list()
            if operation.delete is not None:
                delete_templates = get_quads_from_update(operation.delete, default_graph, server_url)
            if operation.insert is not None:
                insert_templates = get_quads_from_update(operation.insert, default_graph, server_url)

            # build the SerializableUpdate iterator
            return SerializableUpdate(dataset, read_iterator, delete_templates, insert_templates)
        else:
            # build the IF EXISTS operation from a WHERE clause with bounded RDF triples
            # This is a trick to avoid extending the SPARQL update parser with a new IF_EXISTS/ASK clause
            # Moving toward a dedicated parser with custom keywords would be a nice thing to do (later)

            # assert that all RDF triples from the WHERE clause are bounded
            if_exists_quads = where_root.triples
            for s, p, o in if_exists_quads:
                if type(s) is Variable or type(s) is BNode or type(p) is Variable or type(p) is BNode or type(o) is Variable or type(o) is BNode:
                    raise UnsupportedSPARQL("Only INSERT DATA and DELETE DATA queries are supported by the SaGe server. For evaluating other type of SPARQL UPDATE queries, please use a Sage Smart Client.")
            # localize all triples in the default graph
            # TODO change that????
            if_exists_quads = list(localize_triples(where_root.triples, [default_graph]))

            # get the delete and/or insert triples
            delete_quads = list()
            insert_quads = list()
            if operation.delete is not None:
                delete_quads = get_quads_from_update(operation.delete, default_graph, server_url)
            if operation.insert is not None:
                insert_quads = get_quads_from_update(operation.insert, default_graph, server_url)

            # build the UpdateSequenceOperator operator
            start_timestamp = datetime.now()
            if_exists_op = IfExistsOperator(if_exists_quads, dataset, start_timestamp)
            delete_op = DeleteOperator(delete_quads, dataset, server_url)
            insert_op = DeleteOperator(insert_quads, dataset, server_url)
            return UpdateSequenceOperator(if_exists_op, delete_op, insert_op), dict()
    else:
        raise UnsupportedSPARQL("Only INSERT DATA and DELETE DATA queries are supported by the SaGe server. For evaluating other type of SPARQL UPDATE queries, please use a Sage Smart Client.")
