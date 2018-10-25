# filter_iterator_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.sage_engine import SageEngine
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.filter import FilterIterator
from query_engine.iterators.projection import ProjectionIterator
from query_engine.iterators.loader import load
from database.hdt_file_connector import HDTFileConnector
import math

hdtDoc = HDTFileConnector('tests/data/watdiv.10M.hdt')
engine = SageEngine()
triple = {
    'subject': 'http://db.uwaterloo.ca/~galuc/wsdbm/Offer1000',
    'predicate': '?p',
    'object': '?o'
}


def test_simple_filter_iterator():
    expression = "?p = <http://schema.org/eligibleRegion>"
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ProjectionIterator(ScanIterator(iterator, triple, card))
    iterator = FilterIterator(scan, expression)
    (results, saved, done) = engine.execute(iterator, math.inf)
    assert len(results) == 4
    for b in results:
        assert b['?p'] == 'http://schema.org/eligibleRegion'
        assert b['?o'] in [
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country0',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country1',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country4',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
        ]


def test_and_or_filter_iterator():
    expression = "?p = <http://schema.org/eligibleRegion> && (?o = <http://db.uwaterloo.ca/~galuc/wsdbm/Country0> || ?o = <http://db.uwaterloo.ca/~galuc/wsdbm/Country9>)"
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ProjectionIterator(ScanIterator(iterator, triple, card))
    iterator = FilterIterator(scan, expression)
    (results, saved, done) = engine.execute(iterator, math.inf)
    assert len(results) == 2
    for b in results:
        assert b['?p'] == 'http://schema.org/eligibleRegion'
        assert b['?o'] in [
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country0',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
        ]


def test_operation_filter_iterator():
    expression = "10 = 5 * 2"
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ProjectionIterator(ScanIterator(iterator, triple, card))
    iterator = FilterIterator(scan, expression)
    (results, saved, done) = engine.execute(iterator, math.inf)
    assert len(results) == 9


def test_function_filter_iterator():
    expression = '?p = <http://purl.org/goodrelations/price> && isLiteral(?o) && !isNumeric(?o)'
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ProjectionIterator(ScanIterator(iterator, triple, card))
    iterator = FilterIterator(scan, expression)
    (results, saved, done) = engine.execute(iterator, math.inf)
    assert len(results) == 1


def test_filter_iterator_interrupt():
    expression = "?p = <http://schema.org/eligibleRegion>"
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ProjectionIterator(ScanIterator(iterator, triple, card))
    iterator = FilterIterator(scan, expression)
    (results, saved, done) = engine.execute(iterator, 10e-7)
    assert len(results) < 4
    for b in results:
        assert b['?p'] == 'http://schema.org/eligibleRegion'
        assert b['?o'] in [
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country0',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country1',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country4',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
        ]
    tmp = len(results)
    reloaded = load(saved.SerializeToString(), hdtDoc)
    (results, saved, done) = engine.execute(reloaded, 10e7)
    assert len(results) + tmp == 4
    for b in results:
        assert b['?p'] == 'http://schema.org/eligibleRegion'
        assert b['?o'] in [
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country0',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country1',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country4',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
        ]
