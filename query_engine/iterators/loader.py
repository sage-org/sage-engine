# loader.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.nlj import NestedLoopJoinIterator
from query_engine.protobuf.utils import protoTriple_to_dict
from query_engine.protobuf.iterators_pb2 import TriplePattern, SavedSelectionIterator, SavedScanIterator, SavedNestedLoopJoinIterator


def load(protoMsg, hdtDocument):
    """Load a preemptable physical query execution plan from a saved state"""
    savedPlan = protoMsg
    if isinstance(protoMsg, bytes):
        savedPlan = SavedSelectionIterator()
        savedPlan.ParseFromString(protoMsg)
    if type(savedPlan) is SavedSelectionIterator:
        if savedPlan.HasField('nlj_source'):
            return load(savedPlan.nlj_source, hdtDocument)
        elif savedPlan.HasField('scan_source'):
            return load(savedPlan.scan_source, hdtDocument)
    elif type(savedPlan) is SavedScanIterator:
        return load_scan(savedPlan, hdtDocument)
    elif type(savedPlan) is SavedNestedLoopJoinIterator:
        return load_nlj(savedPlan, hdtDocument)
    else:
        raise Exception('Unknown iterator type "%s" when loading controls' % type(savedPlan))


def load_scan(savedPlan, hdtDocument):
    """Load a ScanIterator from a protobuf serialization"""
    triple = savedPlan.triple
    s, p, o = (triple.subject, triple.predicate, triple.object)
    iterator, card = hdtDocument.search_triples(s, p, o, offset=int(savedPlan.offset))
    return ScanIterator(iterator, protoTriple_to_dict(triple), savedPlan.cardinality)


def load_nlj(savedPlan, hdtDocument):
    """Load a NestedLoopJoinIterator from a protobuf serialization"""
    currentBinding = None
    if savedPlan.HasField('nlj_source'):
        source = load(savedPlan.nlj_source, hdtDocument)
    else:
        source = load(savedPlan.scan_source, hdtDocument)
    innerTriple = protoTriple_to_dict(savedPlan.inner)
    if len(savedPlan.muc) > 0:
        currentBinding = savedPlan.muc
    iterOffset = savedPlan.offset
    return NestedLoopJoinIterator(source, innerTriple, hdtDocument, currentBinding=currentBinding, iterOffset=iterOffset)
