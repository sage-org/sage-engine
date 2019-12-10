# scan_test.py
# Author: Thomas MINIER - MIT License 2017-2020
import pytest
from sage.query_engine.sage_engine import SageEngine
from sage.query_engine.iterators.scan import ScanIterator
from sage.database.hdt.connector import HDTFileConnector

hdtDoc = HDTFileConnector('tests/data/test.hdt')
engine = SageEngine()
triple = {
    'subject': '?s',
    'predicate': '?p',
    'object': '?o',
    'graph': 'watdiv100'
}


@pytest.mark.asyncio
async def test_scan_read():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    (results, saved, done, _) = await engine.execute(scan, 10e7)
    assert len(results) == card
    assert done


@pytest.mark.asyncio
async def test_scan_save_nointerrupt():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    (results, saved, done, _) = await engine.execute(scan, 10e7)


@pytest.mark.asyncio
async def test_scan_save_interrupt():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    (results, saved, done, _) = await engine.execute(scan, 1e-3)
    assert len(results) <= card
