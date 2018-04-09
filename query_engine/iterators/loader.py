# loader.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.nlj import NestedLoopJoinIterator


def load(json_controls, hdtDocument):
    """Load a tree of Stop-and-Go operators from JSON controls"""
    if 'type' not in json_controls:
        raise Exception('Missing field "type" in JSON controls')
    if json_controls['type'] == 'ScanIterator':
        return load_scan(json_controls, hdtDocument)
    elif json_controls['type'] == 'NestedLoopJoinIterator':
        return load_nlj(json_controls, hdtDocument)
    else:
        raise Exception('Unknown iterator type "%s" when loading controls' % json_controls['type'])


def load_scan(json_controls, hdtDocument):
    s, p, o = (json_controls['triple']['subject'], json_controls['triple']['predicate'], json_controls['triple']['object'])
    iterator, c = hdtDocument.search_triples(s, p, o, offset=int(json_controls['offset']))
    return ScanIterator(iterator, json_controls['triple'], json_controls['tripleName'])


def load_nlj(json_controls, hdtDocument):
    source = load(json_controls['source'], hdtDocument)
    innerTriple = json_controls['inner']
    currentBinding = json_controls['mappings']
    iterOffset = int(json_controls['offset'])
    return NestedLoopJoinIterator(source, innerTriple, hdtDocument, currentBinding=currentBinding, iterOffset=iterOffset)
