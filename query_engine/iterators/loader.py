# loader.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.projection import ProjectionIterator
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.nlj import NestedLoopJoinIterator
from query_engine.iterators.union import BagUnionIterator
from query_engine.protobuf.utils import protoTriple_to_dict
from query_engine.protobuf.iterators_pb2 import RootTree, SavedProjectionIterator, SavedScanIterator, SavedNestedLoopJoinIterator, SavedBagUnionIterator


def load(protoMsg, hdtDocument):
    """Load a preemptable physical query execution plan from a saved state"""
    savedPlan = protoMsg
    if isinstance(protoMsg, bytes):
        root = RootTree()
        root.ParseFromString(protoMsg)
        sourceField = root.WhichOneof('source')
        savedPlan = getattr(root, sourceField)
    if type(savedPlan) is SavedProjectionIterator:
        return load_projection(savedPlan, hdtDocument)
    elif type(savedPlan) is SavedScanIterator:
        return load_scan(savedPlan, hdtDocument)
    elif type(savedPlan) is SavedNestedLoopJoinIterator:
        return load_nlj(savedPlan, hdtDocument)
    if type(savedPlan) is SavedBagUnionIterator:
        return load_union(savedPlan, hdtDocument)
    else:
        raise Exception('Unknown iterator type "%s" when loading controls' % type(savedPlan))


def load_projection(savedPlan, hdtDocument):
    """Load a ProjectionIterator from a protobuf serialization"""
    sourceField = savedPlan.WhichOneof('source')
    source = load(getattr(savedPlan, sourceField), hdtDocument)
    return ProjectionIterator(source, savedPlan.values)


def load_scan(savedPlan, hdtDocument):
    """Load a ScanIterator from a protobuf serialization"""
    triple = savedPlan.triple
    s, p, o = (triple.subject, triple.predicate, triple.object)
    iterator, card = hdtDocument.search_triples(s, p, o, offset=int(savedPlan.offset))
    return ScanIterator(iterator, protoTriple_to_dict(triple), savedPlan.cardinality)


def load_nlj(savedPlan, hdtDocument):
    """Load a NestedLoopJoinIterator from a protobuf serialization"""
    currentBinding = None
    sourceField = savedPlan.WhichOneof('source')
    source = load(getattr(savedPlan, sourceField), hdtDocument)
    innerTriple = protoTriple_to_dict(savedPlan.inner)
    if len(savedPlan.muc) > 0:
        currentBinding = savedPlan.muc
    iterOffset = savedPlan.offset
    return NestedLoopJoinIterator(source, innerTriple, hdtDocument, currentBinding=currentBinding, iterOffset=iterOffset)


def load_union(savedPlan, hdtDocument):
    """Load a BagUnionIterator from a protobuf serialization"""
    leftField = savedPlan.WhichOneof('left_source')
    left = load(getattr(savedPlan, leftField), hdtDocument)
    rightField = savedPlan.WhichOneof('right_source')
    right = load(getattr(savedPlan, rightField), hdtDocument)
    return BagUnionIterator(left, right)
