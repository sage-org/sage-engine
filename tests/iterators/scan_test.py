# scan_test.py
# Author: Thomas MINIER - MIT License 2017-2020
import pytest
from sage.query_engine.sage_engine import SageEngine
from sage.query_engine.iterators.scan import ScanIterator
from sage.database.backends.hdt.connector import HDTFileConnector

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
    context = { 'quantum': 10e7, 'max_results': 10e7 }
    scan = ScanIterator(hdtDoc, triple, context)
    (results, saved, done, _) = await engine.execute(scan, context)
    assert len(results) == scan.__len__()
    assert done


@pytest.mark.asyncio
async def test_scan_save_nointerrupt():
    context = { 'quantum': 10e7, 'max_results': 10e7 }
    scan = ScanIterator(hdtDoc, triple, context)
    (results, saved, done, _) = await engine.execute(scan, context)


@pytest.mark.asyncio
async def test_scan_save_interrupt():
    context = { 'quantum': 10e7, 'max_results': 1e-3 }
    scan = ScanIterator(hdtDoc, triple, context)
    (results, saved, done, _) = await engine.execute(scan, context)
    assert len(results) <= scan.__len__()
