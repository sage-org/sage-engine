# scan_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from sage.query_engine.sage_engine import SageEngine
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.database.hdt_file_connector import HDTFileConnector

hdtDoc = HDTFileConnector('tests/data/test.hdt')
engine = SageEngine()
triple = {
    'subject': '?s1',
    'predicate': 'http://example.org/p1',
    'object': '?common',
    'graph': 'watdiv100'
}
innerTriple = {
    'subject': '?s2',
    'predicate': 'http://example.org/p2',
    'object': '?common',
    'graph': 'watdiv100'
}


def test_nlj_read():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    join = IndexJoinIterator(scan, innerTriple, hdtDoc)
    (results, saved, done) = engine.execute(join, 10e7)
    assert len(results) == 20
    for res in results:
        assert '?s1' in res and '?s2' in res and '?common' in res
    assert done


def test_nlj_interrupt():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    join = IndexJoinIterator(scan, innerTriple, hdtDoc)
    (results, saved, done) = engine.execute(join, 10e-5)
    assert len(results) <= 20
