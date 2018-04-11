# scan_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.sage_engine import SageEngine
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.union import BagUnionIterator, RandomBagUnionIterator
from datasets.hdt_file_factory import HDTFileFactory

hdtDoc = HDTFileFactory('data/test.hdt')
engine = SageEngine()
triple1 = {
    'subject': 'http://example.org/s1',
    'predicate': '?p',
    'object': '?o'
}
triple2 = {
    'subject': 'http://example.org/s2',
    'predicate': '?p',
    'object': '?o'
}


def test_bag_union_read():
    iterator1, card1 = hdtDoc.search_triples(triple1['subject'], triple1['predicate'], triple1['object'])
    iterator2, card2 = hdtDoc.search_triples(triple2['subject'], triple2['predicate'], triple2['object'])
    left = ScanIterator(iterator1, triple1, card1)
    right = ScanIterator(iterator2, triple2, card2)
    union = BagUnionIterator(left, right)
    (results, saved, done) = engine.execute(union, 10e7)
    assert len(results) == card1 + card2
    assert done


def test_bag_union_interrupt():
    iterator1, card1 = hdtDoc.search_triples(triple1['subject'], triple1['predicate'], triple1['object'])
    iterator2, card2 = hdtDoc.search_triples(triple2['subject'], triple2['predicate'], triple2['object'])
    left = ScanIterator(iterator1, triple1, card1)
    right = ScanIterator(iterator2, triple2, card2)
    union = BagUnionIterator(left, right)
    (results, saved, done) = engine.execute(union, 10e-4)
    assert len(results) < card1 + card2
    assert not done


def test_random_union_read():
    iterator1, card1 = hdtDoc.search_triples(triple1['subject'], triple1['predicate'], triple1['object'])
    iterator2, card2 = hdtDoc.search_triples(triple2['subject'], triple2['predicate'], triple2['object'])
    left = ScanIterator(iterator1, triple1, card1)
    right = ScanIterator(iterator2, triple2, card2)
    union = RandomBagUnionIterator(left, right)
    (results, saved, done) = engine.execute(union, 10e7)
    assert len(results) == card1 + card2
    assert done


def test_random_union_interrupt():
    iterator1, card1 = hdtDoc.search_triples(triple1['subject'], triple1['predicate'], triple1['object'])
    iterator2, card2 = hdtDoc.search_triples(triple2['subject'], triple2['predicate'], triple2['object'])
    left = ScanIterator(iterator1, triple1, card1)
    right = ScanIterator(iterator2, triple2, card2)
    union = RandomBagUnionIterator(left, right)
    (results, saved, done) = engine.execute(union, 10e-4)
    assert len(results) < card1 + card2
    assert not done
