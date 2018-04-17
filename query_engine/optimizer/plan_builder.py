# plan_builder.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.projection import ProjectionIterator
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.nlj import NestedLoopJoinIterator, LeftNLJIterator
from query_engine.iterators.union import BagUnionIterator
from query_engine.iterators.utils import EmptyIterator
from query_engine.iterators.loader import load
from query_engine.optimizer.utils import find_connected_pattern, get_vars


def build_query_plan(query, hdtDocument, savedPlan=None, projection=None):
    if savedPlan is not None:
        return load(savedPlan, hdtDocument)

    optional = query['optional'] if 'optional' in query and len(query['optional']) > 0 else None

    if query['type'] == 'union':
        return build_union_plan(query['patterns'], hdtDocument, projection)
    elif query['type'] == 'bgp':
        return build_join_plan(query['bgp'], hdtDocument, optional=optional, projection=projection)
    else:
        raise Exception('Unkown query type found during query optimization')


def build_union_plan(union, hdtDocument, projection=None):
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
    sources = [build_left_plan(bgp, hdtDocument) for bgp in union]
    if len(sources) == 1:
        return sources[0]
    while len(sources) > 1:
        sources = list(map(mapper, chunks(sources, 2)))
    return sources[0]


def build_join_plan(bgp, hdtDocument, optional=None, projection=None):
    """Build a join plan between a BGP and a possible OPTIONAL clause"""
    iterator, qVars = build_left_plan(bgp, hdtDocument)
    if optional is not None:
        iterator, qVars = build_left_plan(optional, hdtDocument, source=iterator, sourceVars=qVars, optional=True)
    values = projection if projection is not None else qVars
    return ProjectionIterator(iterator, values)


def build_left_plan(bgp, hdtDocument, source=None, sourceVars=None, optional=False):
    """Build a Left-linear tree of joins/left-joins from a BGP/OPTIONAL BGP"""
    # gather metadata about triple patterns
    triples = []
    iteratorConstructor = LeftNLJIterator if optional else NestedLoopJoinIterator
    for triple in bgp:
        it, c = hdtDocument.search_triples(triple['subject'], triple['predicate'], triple['object'])
        triples += [{'triple': triple, 'cardinality': c, 'iterator': it}]
    # sort triples by ascending cardinality
    triples = sorted(triples, key=lambda v: v['cardinality'])
    # if no input iterator provided, build a Scan with the most selective pattern
    if source is None:
        pattern = triples.pop(0)
        acc = ScanIterator(pattern['iterator'], pattern['triple'], pattern['cardinality'])
        queryVariables = get_vars(pattern['triple'])
    else:
        pattern = None
        acc = source
        queryVariables = sourceVars
    # build the left linear tree
    while len(triples) > 0:
        pattern, pos, queryVariables = find_connected_pattern(queryVariables, triples)
        # no connected pattern = disconnected BGP => pick the first remaining pattern in the BGP
        if pattern is None:
            pattern = triples[0]
            queryVariables = queryVariables | get_vars(pattern['triple'])
            pos = 0
        acc = iteratorConstructor(acc, pattern['triple'], hdtDocument)
        triples.pop(pos)
    return acc, queryVariables
