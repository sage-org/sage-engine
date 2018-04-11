# plan_builder.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.projection import ProjectionIterator
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.nlj import NestedLoopJoinIterator
from query_engine.iterators.utils import EmptyIterator
from query_engine.iterators.loader import load
from query_engine.optimizer.utils import find_connected_pattern, get_vars


def build_left_plan(bgp, hdtDocument, savedPlan=None, projection=None):
    """Build a Left-linear tree of joins from a BGP"""
    if savedPlan is not None:
        return load(savedPlan, hdtDocument)
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
