# loader.py
# Author: Thomas MINIER - MIT License 2017-2020
from datetime import datetime
from typing import Union, Dict, Any
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.iterators.values import ValuesIterator
from sage.query_engine.iterators.limit import LimitIterator
from sage.query_engine.iterators.topk import TOPKIterator
from sage.query_engine.iterators.topk_collab import TOPKCollabIterator
from sage.query_engine.protobuf.iterators_pb2 import (RootTree,
                                                      SavedBagUnionIterator,
                                                      SavedFilterIterator,
                                                      SavedIndexJoinIterator,
                                                      SavedProjectionIterator,
                                                      SavedScanIterator,
                                                      SavedValuesIterator,
                                                      SavedLimitIterator,
                                                      SavedTOPKIterator,
                                                      SavedTOPKCollabIterator)
from sage.query_engine.protobuf.utils import protoTriple_to_dict

SavedProtobufPlan = Union[RootTree, SavedBagUnionIterator, SavedFilterIterator, SavedIndexJoinIterator, SavedProjectionIterator, SavedScanIterator]


def load(
    saved_plan: SavedProtobufPlan, dataset: Dataset,
    context: Dict[str, Any] = {}
) -> PreemptableIterator:
    """Load a preemptable physical query execution plan from a saved state.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.
      * context: Information about the execution context of the query.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    # unpack the plan from the serialized protobuf message
    if isinstance(saved_plan, bytes):
        root = RootTree()
        root.ParseFromString(saved_plan)
        sourceField = root.WhichOneof('source')
        saved_plan = getattr(root, sourceField)
    # load the plan based on the current node
    if type(saved_plan) is SavedFilterIterator:
        return load_filter(saved_plan, dataset, context=context)
    if type(saved_plan) is SavedProjectionIterator:
        return load_projection(saved_plan, dataset, context=context)
    elif type(saved_plan) is SavedScanIterator:
        return load_scan(saved_plan, dataset, context=context)
    elif type(saved_plan) is SavedIndexJoinIterator:
        return load_nlj(saved_plan, dataset, context=context)
    elif type(saved_plan) is SavedBagUnionIterator:
        return load_union(saved_plan, dataset, context=context)
    elif type(saved_plan) is SavedValuesIterator:
        return load_values(saved_plan, dataset, context=context)
    elif type(saved_plan) is SavedLimitIterator:
        return load_limit(saved_plan, dataset, context=context)
    elif type(saved_plan) is SavedTOPKIterator:
        return load_topk(saved_plan, dataset, context=context)
    elif type(saved_plan) is SavedTOPKCollabIterator:
        return load_topkCollab(saved_plan, dataset, context=context)
    else:
        raise Exception(f"Unknown iterator type '{type(saved_plan)}' when loading controls")


def load_projection(
    saved_plan: SavedProjectionIterator, dataset: Dataset,
    context: Dict[str, Any] = {}
) -> PreemptableIterator:
    """Load a ProjectionIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.
      * context: Information about the execution context of the query.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset, context=context)

    values = saved_plan.values if len(saved_plan.values) > 0 else None

    return ProjectionIterator(source, values)


def load_filter(
    saved_plan: SavedFilterIterator, dataset: Dataset,
    context: Dict[str, Any] = {}
) -> PreemptableIterator:
    """Load a FilterIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.
      * context: Information about the execution context of the query.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset, context=context)

    constrained_variables = list(saved_plan.variables)

    query = f'SELECT * WHERE {{ ?s ?p ?o . FILTER({saved_plan.expression}) }}'
    compiled_expr = parseQuery(query)
    compiled_expr = translateQuery(compiled_expr).algebra.p.p.expr

    return FilterIterator(
        source, saved_plan.expression, constrained_variables, compiled_expr)


def load_scan(
    saved_plan: SavedScanIterator, dataset: Dataset,
    context: Dict[str, Any] = {}
) -> PreemptableIterator:
    """Load a ScanIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.
      * context: Information about the execution context of the query.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    pattern = protoTriple_to_dict(saved_plan.pattern)
    connector = dataset.get_graph(pattern['graph'])

    if saved_plan.timestamp is not None and saved_plan.timestamp != '':
        as_of = datetime.fromisoformat(saved_plan.timestamp)
    else:
        as_of = None

    current_mappings = None
    if len(saved_plan.muc) > 0:
        current_mappings = dict(saved_plan.muc)

    mu = None
    if len(saved_plan.mu) > 0:
        mu = dict(saved_plan.mu)

    return ScanIterator(
        connector, pattern, current_mappings=current_mappings, mu=mu,
        last_read=saved_plan.last_read, as_of=as_of)


def load_values(
    saved_plan: SavedValuesIterator, dataset: Dataset,
    context: Dict[str, Any] = {}
) -> PreemptableIterator:
    """Load a ValuesIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.
      * context: Information about the execution context of the query.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    values = list()
    for value in saved_plan.values:
        values.append(dict(value.bindings))

    current_mappings = None
    if len(saved_plan.muc) > 0:
        current_mappings = dict(saved_plan.muc)

    return ValuesIterator(
        values, current_mappings=current_mappings, next_value=saved_plan.next_value)


def load_nlj(
    saved_plan: SavedIndexJoinIterator, dataset: Dataset,
    context: Dict[str, Any] = {}
) -> PreemptableIterator:
    """Load a IndexJoinIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.
      * context: Information about the execution context of the query.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    leftField = saved_plan.WhichOneof('left')
    left = load(getattr(saved_plan, leftField), dataset, context=context)

    rightField = saved_plan.WhichOneof('right')
    right = load(getattr(saved_plan, rightField), dataset, context=context)

    current_mappings = None
    if len(saved_plan.muc) > 0:
        current_mappings = dict(saved_plan.muc)

    return IndexJoinIterator(left, right, current_mappings=current_mappings)


def load_union(
    saved_plan: SavedBagUnionIterator, dataset: Dataset,
    context: Dict[str, Any] = {}
) -> PreemptableIterator:
    """Load a BagUnionIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.
      * context: Information about the execution context of the query.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    leftField = saved_plan.WhichOneof('left')
    left = load(getattr(saved_plan, leftField), dataset, context=context)

    rightField = saved_plan.WhichOneof('right')
    right = load(getattr(saved_plan, rightField), dataset, context=context)

    return BagUnionIterator(left, right)


def load_limit(
    saved_plan: SavedLimitIterator, dataset: Dataset,
    context: Dict[str, Any] = {}
) -> PreemptableIterator:
    """Load a SavedLimitIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.
      * context: Information about the execution context of the query.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset, context=context)

    return LimitIterator(
        source, limit=saved_plan.limit, produced=saved_plan.produced)


def load_topk(
    saved_plan: SavedTOPKIterator, dataset: Dataset,
    context: Dict[str, Any] = {}
) -> PreemptableIterator:
    """Load a SavedTOPKIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.
      * context: Information about the execution context of the query.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset, context=context)

    query = f'SELECT * WHERE {{ ?s ?p ?o }} ORDER BY {saved_plan.expression}'
    parsed_query = parseQuery(query)
    compiled_order_conditions = translateQuery(parsed_query).algebra.p.p.expr

    topk = list()
    for solution in saved_plan.topk:
        topk.append(dict(solution.bindings))

    return TOPKIterator(
        source, saved_plan.expression, compiled_order_conditions,
        saved_plan.limit, topk=topk)


def load_topkCollab(
    saved_plan: SavedTOPKCollabIterator, dataset: Dataset,
    context: Dict[str, Any] = {}
) -> PreemptableIterator:
    """Load a SavedTOPKCollabIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.
      * context: Information about the execution context of the query.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset, context=context)

    query = f'SELECT * WHERE {{ ?s ?p ?o }} ORDER BY {saved_plan.expression}'
    parsed_query = parseQuery(query)
    compiled_order_conditions = translateQuery(parsed_query).algebra.p.p.expr

    threshold = None
    if len(saved_plan.threshold) > 0:
        threshold = dict(saved_plan.threshold)

    return TOPKCollabIterator(
        source, saved_plan.expression, compiled_order_conditions,
        saved_plan.limit, threshold=threshold,
        threshold_refresh_rate=saved_plan.threshold_refresh_rate)
