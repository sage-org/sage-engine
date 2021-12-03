# scan_test.py
# Author: Thomas MINIER - MIT License 2017-2020
import pytest
from sage.query_engine.sage_engine import SageEngine
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.database.backends.hdt.connector import HDTFileConnector

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


@pytest.mark.asyncio
async def test_nlj_read():
    context = { 'quantum': 10e7, 'max_results': 10e7 }
    left_scan = ScanIterator(hdtDoc, triple, context)
    right_scan = ScanIterator(hdtDoc, innerTriple, context)
    join = IndexJoinIterator(left_scan, right_scan, context)
    (results, saved, done, _) = await engine.execute(join, context)
    assert len(results) == 20
    for res in results:
        assert '?s1' in res and '?s2' in res and '?common' in res
    assert done


@pytest.mark.asyncio
async def test_nlj_interrupt():
    context = { 'quantum': 10e7, 'max_results': 10e-5 }
    left_scan = ScanIterator(hdtDoc, triple, context)
    right_scan = ScanIterator(hdtDoc, innerTriple, context)
    join = IndexJoinIterator(left_scan, right_scan, context)
    (results, saved, done, _) = await engine.execute(join, context)
    assert len(results) <= 20
