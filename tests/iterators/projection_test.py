# scan_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.sage_engine import SageEngine
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.projection import ProjectionIterator
from datasets.hdt_file_factory import HDTFileFactory

hdtDoc = HDTFileFactory('data/test.hdt')
engine = SageEngine()
triple = {
    'subject': '?s1',
    'predicate': 'http://example.org/p1',
    'object': '?common'
}


def test_projection_read():
    iterator, card = hdtDoc.search_triples(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    proj = ProjectionIterator(scan, ['?common'])
    (results, saved, done) = engine.execute(proj, 10e7)
    assert len(results) == card
    for res in results:
        assert '?common' in res and '?s1' not in res
    assert done


def test_projection_read():
    iterator, card = hdtDoc.search_triples(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    proj = ProjectionIterator(scan, ['?common'])
    (results, saved, done) = engine.execute(proj, 10e-4)
    assert len(results) < card
    for res in results:
        assert '?common' in res and '?s1' not in res
    assert not done
