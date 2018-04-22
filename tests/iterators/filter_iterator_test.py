# filter_iterator_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.sage_engine import SageEngine
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.filter import FilterIterator
from query_engine.iterators.projection import ProjectionIterator
from query_engine.iterators.loader import load
from database.hdt_file_factory import HDTFileFactory

hdtDoc = HDTFileFactory('data/test.hdt')
engine = SageEngine()
triple = {
    'subject': '?s',
    'predicate': '?p',
    'object': '?o'
}


def test_simple_filter_iterator():
    expression = "($s == URIRef(\"http://example.org/s1\")) and ($p == URIRef(\"http://example.org/p1\"))"
    iterator, card = hdtDoc.search_triples(triple['subject'], triple['predicate'], triple['object'])
    scan = ProjectionIterator(ScanIterator(iterator, triple, card))
    iterator = FilterIterator(scan, expression, set(["?s", "?p"]))
    (results, saved, done) = engine.execute(iterator, 10e7)
    assert len(results) == 100
    for b in results:
        assert b['?s'] == 'http://example.org/s1' and b['?p'] == 'http://example.org/p1'


def test_composed_filter_iterator():
    expression1 = "$s == URIRef(\"http://example.org/s1\")"
    expression2 = "$p == URIRef(\"http://example.org/p1\")"
    iterator, card = hdtDoc.search_triples(triple['subject'], triple['predicate'], triple['object'])
    scan = ProjectionIterator(ScanIterator(iterator, triple, card))
    it1 = FilterIterator(scan, expression1, set(["?s"]))
    it2 = FilterIterator(it1, expression2, set(["?p"]))
    (results, saved, done) = engine.execute(it2, 10e7)
    assert len(results) == 100
    for b in results:
        assert b['?s'] == 'http://example.org/s1' and b['?p'] == 'http://example.org/p1'


def test_filter_iterator_interrupt():
    expression = "($s == URIRef(\"http://example.org/s1\")) and ($p == URIRef(\"http://example.org/p1\"))"
    iterator, card = hdtDoc.search_triples(triple['subject'], triple['predicate'], triple['object'])
    scan = ProjectionIterator(ScanIterator(iterator, triple, card))
    iterator = FilterIterator(scan, expression, set(["?s", "?p"]))
    (results, saved, done) = engine.execute(iterator, 10e-4)
    assert len(results) < 100
    tmp = len(results)
    for b in results:
        assert b['?s'] == 'http://example.org/s1' and b['?p'] == 'http://example.org/p1'
    reloaded = load(saved.SerializeToString(), hdtDoc)
    (results, saved, done) = engine.execute(reloaded, 10e7)
    assert len(results) + tmp == 100
    for b in results:
        assert b['?s'] == 'http://example.org/s1' and b['?p'] == 'http://example.org/p1'


def test_composed_filter_iterator_interrupt():
    expression1 = "$s == URIRef(\"http://example.org/s1\")"
    expression2 = "$p == URIRef(\"http://example.org/p1\")"
    iterator, card = hdtDoc.search_triples(triple['subject'], triple['predicate'], triple['object'])
    scan = ProjectionIterator(ScanIterator(iterator, triple, card))
    it1 = FilterIterator(scan, expression1, set(["?s"]))
    it2 = FilterIterator(it1, expression2, set(["?p"]))
    (results, saved, done) = engine.execute(it2, 10e-4)
    assert len(results) < 100
    tmp = len(results)
    for b in results:
        assert b['?s'] == 'http://example.org/s1' and b['?p'] == 'http://example.org/p1'
    reloaded = load(saved.SerializeToString(), hdtDoc)
    (results, saved, done) = engine.execute(reloaded, 10e7)
    assert len(results) + tmp == 100
    for b in results:
        assert b['?s'] == 'http://example.org/s1' and b['?p'] == 'http://example.org/p1'
