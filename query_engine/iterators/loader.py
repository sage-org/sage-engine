# loader.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.projection import ProjectionIterator
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.nlj import IndexJoinIterator, LeftNLJIterator
from query_engine.iterators.filter import FilterIterator
from query_engine.iterators.union import BagUnionIterator
from query_engine.protobuf.utils import protoTriple_to_dict
from query_engine.protobuf.iterators_pb2 import RootTree, SavedProjectionIterator, SavedScanIterator, SavedIndexJoinIterator, SavedBagUnionIterator, SavedFilterIterator


def load(protoMsg, db_connector):
    """Load a preemptable physical query execution plan from a saved state"""
    saved_plan = protoMsg
    if isinstance(protoMsg, bytes):
        root = RootTree()
        root.ParseFromString(protoMsg)
        sourceField = root.WhichOneof('source')
        saved_plan = getattr(root, sourceField)
    if type(saved_plan) is SavedFilterIterator:
        return load_filter(saved_plan, db_connector)
    if type(saved_plan) is SavedProjectionIterator:
        return load_projection(saved_plan, db_connector)
    elif type(saved_plan) is SavedScanIterator:
        return load_scan(saved_plan, db_connector)
    elif type(saved_plan) is SavedIndexJoinIterator:
        return load_nlj(saved_plan, db_connector)
    if type(saved_plan) is SavedBagUnionIterator:
        return load_union(saved_plan, db_connector)
    else:
        raise Exception('Unknown iterator type "%s" when loading controls' % type(saved_plan))


def load_projection(saved_plan, db_connector):
    """Load a ProjectionIterator from a protobuf serialization"""
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), db_connector)
    values = saved_plan.values if len(saved_plan.values) > 0 else None
    return ProjectionIterator(source, values)


def load_filter(saved_plan, db_connector):
    """Load a FilterIterator from a protobuf serialization"""
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), db_connector)
    return FilterIterator(source, saved_plan.expression)


def load_scan(saved_plan, db_connector):
    """Load a ScanIterator from a protobuf serialization"""
    triple = saved_plan.triple
    s, p, o = (triple.subject, triple.predicate, triple.object)
    iterator, card = db_connector.search(s, p, o, offset=saved_plan.offset)
    return ScanIterator(iterator, protoTriple_to_dict(triple), saved_plan.cardinality)


def load_nlj(saved_plan, db_connector):
    """Load a IndexJoinIterator from a protobuf serialization"""
    currentBinding = None
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), db_connector)
    innerTriple = protoTriple_to_dict(saved_plan.inner)
    if len(saved_plan.muc) > 0:
        currentBinding = saved_plan.muc
    iterOffset = saved_plan.offset
    if saved_plan.optional:
        return LeftNLJIterator(source, innerTriple, db_connector, currentBinding=currentBinding, iterOffset=iterOffset)
    return IndexJoinIterator(source, innerTriple, db_connector, currentBinding=currentBinding, iterOffset=iterOffset)


def load_union(saved_plan, db_connector):
    """Load a BagUnionIterator from a protobuf serialization"""
    leftField = saved_plan.WhichOneof('left')
    left = load(getattr(saved_plan, leftField), db_connector)
    rightField = saved_plan.WhichOneof('right')
    right = load(getattr(saved_plan, rightField), db_connector)
    return BagUnionIterator(left, right)
