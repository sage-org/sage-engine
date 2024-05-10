# join_builder.py
# Author: Thomas MINIER - MIT License 2017-2020
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.utils import EmptyIterator
from sage.query_engine.optimizer.utils import (equality_variables,
                                               find_connected_pattern,
                                               get_vars)


def create_sort_key(subject_priority):
    """Creates a custom sort key function that prioritizes a specific subject."""
    def custom_sort_key(item):
        # Check if the subject matches the prioritized subject
        is_priority = item['triple']['subject'] == subject_priority
        # Return a tuple that uses the boolean value of is_priority and cardinality
        return (not is_priority, item['cardinality'])
    return custom_sort_key

def build_left_join_tree(bgp: List[Dict[str, str]], dataset: Dataset, default_graph: str, context: dict, as_of: Optional[datetime] = None) -> Tuple[PreemptableIterator, List[str], Dict[str, str]]:
    """Build a Left-linear join tree from a Basic Graph pattern.

    Args:
      * bgp: Basic Graph pattern used to build the join tree.
      * dataset: RDF dataset on which the BGPC is evaluated.
      * default_graph: URI of the default graph used for BGP evaluation.
      * context: Information about the query execution.
      * as_of: A timestamp used to perform all reads against a consistent version of the dataset. If `None`, use the latest version of the dataset, which does not guarantee snapshot isolation.

    Returns: A tuple (`iterator`, `query_vars`, `cardinalities`) where:
      * `iterator` is the root of the Left-linear join tree.
      * `query_vars` is the list of all SPARQL variables found in the BGP.
      * `cardinalities` is the list of estimated cardinalities of all triple patterns in the BGP.
    """
    # gather metadata about triple patterns
    triples = []
    cardinalities = []

    if 'bid_variable' in context:
        print(f"join builder bid_variable: {context['bid_variable']}")

    # analyze each triple pattern in the BGP
    for triple in bgp:
        # select the graph used to evaluate the pattern
        graph_uri = triple['graph'] if 'graph' in triple and len(triple['graph']) > 0 else default_graph
        triple['graph'] = graph_uri
        # get iterator and statistics about the pattern
        if dataset.has_graph(graph_uri):
            it = ScanIterator(dataset.get_graph(graph_uri), triple, context, as_of=as_of)
            c = it.__len__()
        else:
            it, c = EmptyIterator(), 0
        triples += [{'triple': triple, 'cardinality': c, 'iterator': it}]
        cardinalities += [{'triple': triple, 'cardinality': c}]

    # sort triples by ascending cardinality
    triples = sorted(triples, key=lambda v: v['cardinality'])
    #print(f"triples: {triples}")

    if 'bid_variable' in context:
        sort_key = create_sort_key(context['bid_variable'])
        triples = sorted(triples, key=sort_key)
        print(f"triples sorted with {context['bid_variable']}: {triples}")

    # start the pipeline with the Scan with the most selective pattern
    pattern = triples.pop(0)
    query_vars = get_vars(pattern['triple'])

    # add a equality filter if the pattern has several variables that binds to the same value
    # example: ?s rdf:type ?s => Filter(Scan(?s rdf:type ?s_2), ?s == ?s_2)
    # eq_expr, new_pattern = equality_variables(pattern['triple']['subject'], pattern['triple']['predicate'], pattern['triple']['object'])
    # if eq_expr is not None:
    #     # copy pattern with rewritten values
    #     triple = pattern['triple'].copy()
    #     triple["subject"] = new_pattern[0]
    #     triple["predicate"] = new_pattern[1]
    #     triple["object"] = new_pattern[2]
    #     # build a pipline with Index Scan + Equality filter
    #     pipeline = ScanIterator(pattern['iterator'], triple, pattern['cardinality'])
    #     pipeline = FilterIterator(pipeline, eq_expr)
    #     # update query variables
    #     query_vars = query_vars | get_vars(triple)
    # else:
    #     pipeline = ScanIterator(pattern['iterator'], pattern['triple'], pattern['cardinality'])

    pipeline = pattern['iterator']

    # build the left linear tree of joins
    while len(triples) > 0:
        pattern, pos, query_vars = find_connected_pattern(query_vars, triples)
        # no connected pattern = disconnected BGP => pick the first remaining pattern in the BGP
        if pattern is None:
            pattern = triples[0]
            query_vars = query_vars | get_vars(pattern['triple'])
            pos = 0
        graph_uri = pattern['triple']['graph']
        pipeline = IndexJoinIterator(pipeline, pattern['iterator'], context)
        triples.pop(pos)
    return pipeline, query_vars, cardinalities
