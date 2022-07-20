from datetime import datetime

from sage.query_engine.types import QueryContext, SavedPlan
from sage.query_engine.expression import Expression
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.iterators.values import ValuesIterator
from sage.query_engine.iterators.limit import LimitIterator
from sage.query_engine.iterators.topk.topk_server import TOPKServerIterator
from sage.query_engine.iterators.topk.patial_topk import PartialTOPKIterator
from sage.query_engine.iterators.topk.rank_filter import RankFilterIterator
from sage.query_engine.iterators.topk.order_conditions import OrderConditions
from sage.query_engine.protobuf.iterators_pb2 import (
    RootTree,
    SavedBagUnionIterator,
    SavedFilterIterator,
    SavedIndexJoinIterator,
    SavedProjectionIterator,
    SavedScanIterator,
    SavedValuesIterator,
    SavedLimitIterator,
    SavedTOPKServerIterator,
    SavedPartialTOPKIterator,
    SavedRankFilterIterator)
from sage.query_engine.protobuf.utils import protoTriple_to_dict


def load(
    saved_plan: SavedPlan, context: QueryContext = {}
) -> PreemptableIterator:
    """
    Load a preemptable physical query execution plan from a saved state.

    Parameters
    ----------
    saved_plan: SavedProtobufPlan
        Saved query execution plan.
    context: QueryContext
        Global variables specific to the execution of the query.

    Returns
    -------
    PreemptableIterator
        The pipeline of iterators used to continue the query execution.
    """
    if isinstance(saved_plan, bytes):  # unpack the plan from the serialized protobuf message
        root = RootTree()
        root.ParseFromString(saved_plan)
        sourceField = root.WhichOneof("source")
        saved_plan = getattr(root, sourceField)
    if type(saved_plan) is SavedFilterIterator:
        return load_filter(saved_plan, context=context)
    if type(saved_plan) is SavedProjectionIterator:
        return load_projection(saved_plan, context=context)
    elif type(saved_plan) is SavedScanIterator:
        return load_scan(saved_plan, context=context)
    elif type(saved_plan) is SavedIndexJoinIterator:
        return load_nlj(saved_plan, context=context)
    elif type(saved_plan) is SavedBagUnionIterator:
        return load_union(saved_plan, context=context)
    elif type(saved_plan) is SavedValuesIterator:
        return load_values(saved_plan, context=context)
    elif type(saved_plan) is SavedLimitIterator:
        return load_limit(saved_plan, context=context)
    elif type(saved_plan) is SavedTOPKServerIterator:
        return load_topk_server(saved_plan, context=context)
    elif type(saved_plan) is SavedPartialTOPKIterator:
        return load_partial_topk(saved_plan, context=context)
    elif type(saved_plan) is SavedRankFilterIterator:
        return load_rank_filter(saved_plan, context=context)
    raise Exception(f"Unknown iterator type '{type(saved_plan)}' when loading controls")


def load_projection(
    saved_plan: SavedProjectionIterator, context: QueryContext = {}
) -> PreemptableIterator:
    """
    Load a ProjectionIterator from a protobuf message.

    Parameters
    ----------
    saved_plan: SavedProjectionIterator
        Saved query execution plan.
    context: QueryContext
        Global variables specific to the execution of the query.

    Returns
    -------
    PreemptableIterator
        The pipeline of iterators used to continue the query execution.
    """
    sourceField = saved_plan.WhichOneof("source")
    source = load(getattr(saved_plan, sourceField), context=context)

    values = saved_plan.values if len(saved_plan.values) > 0 else None

    return ProjectionIterator(source, values)


def load_filter(
    saved_plan: SavedFilterIterator, context: QueryContext = {}
) -> PreemptableIterator:
    """
    Load a FilterIterator from a protobuf message.

    Parameters
    ----------
    saved_plan: SavedProjectionIterator
        Saved query execution plan.
    context: QueryContext
        Global variables specific to the execution of the query.

    Returns
    -------
    PreemptableIterator
        The pipeline of iterators used to continue the query execution.
    """
    sourceField = saved_plan.WhichOneof("source")
    source = load(getattr(saved_plan, sourceField), context=context)

    expression = Expression.parse(saved_plan.expression)

    return FilterIterator(source, expression)


def load_scan(
    saved_plan: SavedScanIterator, context: QueryContext = {}
) -> PreemptableIterator:
    """Load a ScanIterator from a protobuf message.

    Parameters
    ----------
    saved_plan: SavedProjectionIterator
        Saved query execution plan.
    context: QueryContext
        Global variables specific to the execution of the query.

    Returns
    -------
    PreemptableIterator
        The pipeline of iterators used to continue the query execution.
    """
    pattern = protoTriple_to_dict(saved_plan.pattern)

    as_of = None
    if saved_plan.timestamp is not None and saved_plan.timestamp != "":
        as_of = datetime.fromisoformat(saved_plan.timestamp)

    muc = None
    if len(saved_plan.muc) > 0:
        muc = dict(saved_plan.muc)

    mu = None
    if len(saved_plan.mu) > 0:
        mu = dict(saved_plan.mu)

    return ScanIterator(
        pattern, muc=muc, mu=mu, last_read=saved_plan.last_read, as_of=as_of)


def load_values(
    saved_plan: SavedValuesIterator, context: QueryContext = {}
) -> PreemptableIterator:
    """Load a ValuesIterator from a protobuf message.

    Parameters
    ----------
    saved_plan: SavedProjectionIterator
        Saved query execution plan.
    context: QueryContext
        Global variables specific to the execution of the query.

    Returns
    -------
    PreemptableIterator
        The pipeline of iterators used to continue the query execution.
    """
    items = list()
    for item in saved_plan.values:
        items.append(dict(item.bindings))

    muc = None
    if len(saved_plan.muc) > 0:
        muc = dict(saved_plan.muc)

    return ValuesIterator(items, muc=muc, next_item=saved_plan.next_value)


def load_nlj(
    saved_plan: SavedIndexJoinIterator, context: QueryContext = {}
) -> PreemptableIterator:
    """Load a IndexJoinIterator from a protobuf message.

    Parameters
    ----------
    saved_plan: SavedProjectionIterator
        Saved query execution plan.
    context: QueryContext
        Global variables specific to the execution of the query.

    Returns
    -------
    PreemptableIterator
        The pipeline of iterators used to continue the query execution.
    """
    leftField = saved_plan.WhichOneof("left")
    left = load(getattr(saved_plan, leftField), context=context)

    rightField = saved_plan.WhichOneof("right")
    right = load(getattr(saved_plan, rightField), context=context)

    muc = None
    if len(saved_plan.muc) > 0:
        muc = dict(saved_plan.muc)

    return IndexJoinIterator(left, right, muc=muc)


def load_union(
    saved_plan: SavedBagUnionIterator, context: QueryContext = {}
) -> PreemptableIterator:
    """Load a BagUnionIterator from a protobuf message.

    Parameters
    ----------
    saved_plan: SavedProjectionIterator
        Saved query execution plan.
    context: QueryContext
        Global variables specific to the execution of the query.

    Returns
    -------
    PreemptableIterator
        The pipeline of iterators used to continue the query execution.
    """
    leftField = saved_plan.WhichOneof("left")
    left = load(getattr(saved_plan, leftField), context=context)

    rightField = saved_plan.WhichOneof("right")
    right = load(getattr(saved_plan, rightField), context=context)

    return BagUnionIterator(left, right)


def load_limit(
    saved_plan: SavedLimitIterator, context: QueryContext = {}
) -> PreemptableIterator:
    """Load a LimitIterator from a protobuf message.

    Parameters
    ----------
    saved_plan: SavedProjectionIterator
        Saved query execution plan.
    context: QueryContext
        Global variables specific to the execution of the query.

    Returns
    -------
    PreemptableIterator
        The pipeline of iterators used to continue the query execution.
    """
    sourceField = saved_plan.WhichOneof("source")
    source = load(getattr(saved_plan, sourceField), context=context)

    return LimitIterator(source, limit=saved_plan.limit, produced=saved_plan.produced)


def load_topk_server(
    saved_plan: SavedTOPKServerIterator, context: QueryContext = {}
) -> PreemptableIterator:
    """Load a TOPKServerIterator from a protobuf message.

    Parameters
    ----------
    saved_plan: SavedProjectionIterator
        Saved query execution plan.
    context: QueryContext
        Global variables specific to the execution of the query.

    Returns
    -------
    PreemptableIterator
        The pipeline of iterators used to continue the query execution.
    """
    sourceField = saved_plan.WhichOneof("source")
    source = load(getattr(saved_plan, sourceField), context=context)

    expression = OrderConditions.parse(saved_plan.expression)

    topk = list()
    for solution in saved_plan.topk:
        topk.append(dict(solution.bindings))

    return TOPKServerIterator(source, expression, saved_plan.limit, topk=topk)


def load_partial_topk(
    saved_plan: SavedPartialTOPKIterator, context: QueryContext = {}
) -> PreemptableIterator:
    """Load a PartialTOPKIterator from a protobuf message.

    Parameters
    ----------
    saved_plan: SavedPartialTOPKIterator
        Saved query execution plan.
    context: QueryContext
        Global variables specific to the execution of the query.

    Returns
    -------
    PreemptableIterator
        The pipeline of iterators used to continue the query execution.
    """
    sourceField = saved_plan.WhichOneof("source")
    source = load(getattr(saved_plan, sourceField), context=context)

    expression = OrderConditions.parse(saved_plan.expression)

    threshold = None
    if len(saved_plan.threshold) > 0:
        threshold = dict(saved_plan.threshold)

    return PartialTOPKIterator(
        source, expression, saved_plan.limit, threshold=threshold)


def load_rank_filter(
    saved_plan: SavedRankFilterIterator, context: QueryContext = {}
) -> PreemptableIterator:
    """Load a RankFilterIterator from a protobuf message.

    Parameters
    ----------
    saved_plan: SavedRankFilterIterator
        Saved query execution plan.
    context: QueryContext
        Global variables specific to the execution of the query.

    Returns
    -------
    PreemptableIterator
        The pipeline of iterators used to continue the query execution.
    """
    sourceField = saved_plan.WhichOneof("source")
    source = load(getattr(saved_plan, sourceField), context=context)

    expression = OrderConditions.parse(saved_plan.expression)

    return RankFilterIterator(source, expression, is_partial=saved_plan.is_partial)
