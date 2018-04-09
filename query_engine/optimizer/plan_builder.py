# plan_builder.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.nlj import NestedLoopJoinIterator
from query_engine.iterators.utils import EmptyIterator
from query_engine.iterators.loader import load
from query_engine.optimizer.utils import find_connected_pattern, get_vars


def build_left_plan(bgp, hdtDocument, controls=None, source=None, sourceVars=None, queryVariables=None):
    """Build a Left-linear tree of joins from a BGP"""
    if controls is not None and len(controls) > 0:
        return load(controls, hdtDocument)
    # gather metadata about triple patterns
    triples = []
    for name, triple in bgp.items():
        it, c = hdtDocument.search_triples(triple['subject'], triple['predicate'], triple['object'])
        triples += [{'name': name, 'triple': triple, 'cardinality': c, 'iterator': it}]
    # sort triples by ascending cardinality
    triples = sorted(triples, key=lambda v: v['cardinality'])
    # build the left linear tree
    acc = source
    if acc is None and sourceVars is None:
        pattern = triples.pop(0)
        acc = ScanIterator(pattern['iterator'], pattern['triple'], pattern['name'])
        queryVariables = get_vars(pattern['triple'])
    else:
        queryVariables = sourceVars
    while len(triples) > 0:
        pattern, pos, queryVariables = find_connected_pattern(queryVariables, triples)
        # no connected pattern = disconnected BGP => no results for this BGP
        if pattern is None:
            return EmptyIterator()
        acc = NestedLoopJoinIterator(acc, pattern['triple'], hdtDocument)
        triples.pop(pos)
    return acc
