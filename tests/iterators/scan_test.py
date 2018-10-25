# scan_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.sage_engine import SageEngine
from query_engine.iterators.scan import ScanIterator
from database.hdt_file_connector import HDTFileConnector

hdtDoc = HDTFileConnector('tests/data/test.hdt')
engine = SageEngine()
triple = {
    'subject': '?s',
    'predicate': '?p',
    'object': '?o'
}


def test_scan_read():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    (results, saved, done) = engine.execute(scan, 10e7)
    assert len(results) == card
    assert done


def test_scan_save_nointerrupt():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    (results, saved, done) = engine.execute(scan, 10e7)


def test_scan_save_interrupt():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    (results, saved, done) = engine.execute(scan, 1e-3)
    assert len(results) < card
    assert not done
