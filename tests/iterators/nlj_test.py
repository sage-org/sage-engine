# scan_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.sage_engine import SageEngine
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.nlj import NestedLoopJoinIterator
from datasets.hdt_file_factory import HDTFileFactory

hdtDoc = HDTFileFactory('data/test.hdt')
engine = SageEngine()
triple = {
    'subject': '?s1',
    'predicate': 'http://example.org/p1',
    'object': '?common'
}
innerTriple = {
    'subject': '?s2',
    'predicate': 'http://example.org/p2',
    'object': '?common'
}


def test_nlj_read():
    iterator, card = hdtDoc.search_triples(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    join = NestedLoopJoinIterator(scan, innerTriple, hdtDoc)
    (results, saved, done) = engine.execute(join, 10e7)
    assert len(results) == 20
    for res in results:
        assert '?s1' in res and '?s2' in res and '?common' in res
    assert done


def test_nlj_interrupt():
    iterator, card = hdtDoc.search_triples(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    join = NestedLoopJoinIterator(scan, innerTriple, hdtDoc)
    (results, saved, done) = engine.execute(join, 10e-5)
    assert len(results) < 20
    assert not done
    assert saved.scan_source.triple.subject == triple['subject']
    assert saved.scan_source.triple.predicate == triple['predicate']
    assert saved.scan_source.triple.object == triple['object']
    assert saved.scan_source.cardinality == card
    assert saved.inner.subject == innerTriple['subject']
    assert saved.inner.predicate == innerTriple['predicate']
    assert saved.inner.object == innerTriple['object']
