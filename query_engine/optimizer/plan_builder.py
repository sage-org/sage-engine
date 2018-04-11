# plan_builder.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.projection import ProjectionIterator
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.nlj import NestedLoopJoinIterator
from query_engine.iterators.union import BagUnionIterator
from query_engine.iterators.utils import EmptyIterator
from query_engine.iterators.loader import load
from query_engine.optimizer.utils import find_connected_pattern, get_vars


def build_query_plan(query, hdtDocument, savedPlan=None, projection=None):
    if savedPlan is not None:
        return load(savedPlan, hdtDocument)

    if query['type'] == 'union':
        return build_union_plan(query['patterns'], hdtDocument, projection)
    elif query['type'] == 'bgp':
        return build_left_plan(query['bgp'], hdtDocument, projection)
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


def build_left_plan(bgp, hdtDocument, projection=None):
    """Build a Left-linear tree of joins from a BGP"""
    # gather metadata about triple patterns
    triples = []
    for triple in bgp:
        it, c = hdtDocument.search_triples(triple['subject'], triple['predicate'], triple['object'])
        triples += [{'triple': triple, 'cardinality': c, 'iterator': it}]
    # sort triples by ascending cardinality
    triples = sorted(triples, key=lambda v: v['cardinality'])
    # a pattern with no matching triples => no results for this BGP
    if triples[0]['cardinality'] == 0:
        return EmptyIterator()
    # build the left linear tree
    pattern = triples.pop(0)
    acc = ScanIterator(pattern['iterator'], pattern['triple'], pattern['cardinality'])
    queryVariables = get_vars(pattern['triple'])
    while len(triples) > 0:
        pattern, pos, queryVariables = find_connected_pattern(queryVariables, triples)
        # no connected pattern = disconnected BGP => no results for this BGP
        if pattern is None:
            return EmptyIterator()
        acc = NestedLoopJoinIterator(acc, pattern['triple'], hdtDocument)
        triples.pop(pos)
    values = projection if projection is not None else queryVariables
    return ProjectionIterator(acc, values)
