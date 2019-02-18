# plan_builder.py
# Author: Thomas MINIER - MIT License 2017-2018
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.iterators.loader import load
from sage.query_engine.iterators.utils import EmptyIterator
from sage.query_engine.optimizer.utils import find_connected_pattern, get_vars, equality_variables
from functools import reduce


def build_query_plan(query, dataset, default_graph, saved_plan=None):
    """Build a pipeline of iterators used to evaluate a query"""
    cardinalities = []
    if saved_plan is not None:
        return load(saved_plan, dataset), []

    root = None
    if query['type'] == 'union':
        root, cardinalities = build_union_plan(query['union'], dataset, default_graph)
    elif query['type'] == 'bgp':
        root, cardinalities = build_join_plan(query['bgp'], dataset, default_graph)
    else:
        raise Exception('Unkown query type found during query optimization')

    # apply (possible) filter clause(s)
    if 'filters' in query and len(query['filters']) > 0:
        # exclude empty strings
        filters = list(filter(lambda x: len(x) > 0, query['filters']))
        if len(filters) > 0:
            # reduce all filters in a conjunctive expression
            expression = reduce(lambda x, y: "({}) && ({})".format(x, y), filters)
            root = FilterIterator(root, expression)
    return root, cardinalities


def build_union_plan(union, dataset, default_graph):
    """Build a Bushy tree of Unions, where leaves are BGPs, from a list of BGPS"""

    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def mapper(duo):
        """Build a join between two source iterators"""
        if len(duo) == 1:
            return duo[0]
        return BagUnionIterator(duo[0], duo[1])
    sources = []
    cardinalities = []
    for bgp in union:
        iterator, cards = build_join_plan(bgp, dataset, default_graph)
        sources.append(iterator)
        cardinalities += cards
    if len(sources) == 1:
        return sources[0], cardinalities
    while len(sources) > 1:
        sources = list(map(mapper, chunks(sources, 2)))
    return sources[0], cardinalities


def build_join_plan(bgp, dataset, default_graph):
    """Build a join plan with a projection at the end"""
    iterator, query_vars, cardinalities = build_left_plan(bgp, dataset, default_graph)
    return ProjectionIterator(iterator, query_vars), cardinalities


def build_left_plan(bgp, dataset, default_graph):
    """Build a Left-linear tree of joins from a BGP"""
    # gather metadata about triple patterns
    triples = []
    cardinalities = []

    # analyze each triple pattern in the BGP
    for triple in bgp:
        # select the graph used to evaluate the pattern
        graph_uri = triple['graph'] if 'graph' in triple and len(triple['graph']) > 0 else default_graph
        triple['graph'] = graph_uri
        # get iterator and statistics about the pattern
        if dataset.has_graph(graph_uri):
            it, c = dataset.get_graph(graph_uri).search(triple['subject'], triple['predicate'], triple['object'])
        else:
            it, c = EmptyIterator(), 0
        triples += [{'triple': triple, 'cardinality': c, 'iterator': it}]
        cardinalities += [{'triple': triple, 'cardinality': c}]

    # sort triples by ascending cardinality
    triples = sorted(triples, key=lambda v: v['cardinality'])

    # start the pipeline with the Scan with the most selective pattern
    pattern = triples.pop(0)
    query_vars = get_vars(pattern['triple'])

    # add a equality filter if the pattern has several variables that binds to the same value
    # example: ?s rdf:type ?s => Filter(Scan(?s rdf:type ?s_2), ?s == ?s_2)
    eq_expr, new_pattern = equality_variables(pattern['triple']['subject'], pattern['triple']['predicate'], pattern['triple']['object'])
    if eq_expr is not None:
        # copy pattern with rewritten values
        triple = pattern['triple'].copy()
        triple["subject"] = new_pattern[0]
        triple["predicate"] = new_pattern[1]
        triple["object"] = new_pattern[2]
        # build a pipline with Index Scan + Equality filter
        pipeline = ScanIterator(pattern['iterator'], triple, pattern['cardinality'])
        pipeline = FilterIterator(pipeline, eq_expr)
        # update query variables
        query_vars = query_vars | get_vars(triple)
    else:
        pipeline = ScanIterator(pattern['iterator'], pattern['triple'], pattern['cardinality'])

    # build the left linear tree of joins
    while len(triples) > 0:
        pattern, pos, query_vars = find_connected_pattern(query_vars, triples)
        # no connected pattern = disconnected BGP => pick the first remaining pattern in the BGP
        if pattern is None:
            pattern = triples[0]
            query_vars = query_vars | get_vars(pattern['triple'])
            pos = 0
        graph_uri = pattern['triple']['graph']
        pipeline = IndexJoinIterator(pipeline, pattern['triple'], dataset.get_graph(graph_uri))
        triples.pop(pos)
    return pipeline, query_vars, cardinalities
