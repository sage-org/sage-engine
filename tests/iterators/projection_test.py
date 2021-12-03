# scan_test.py
# Author: Thomas MINIER - MIT License 2017-2020
import pytest
from sage.query_engine.sage_engine import SageEngine
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.database.backends.hdt.connector import HDTFileConnector

hdtDoc = HDTFileConnector('tests/data/test.hdt')
engine = SageEngine()
triple = {
    'subject': '?s1',
    'predicate': 'http://example.org/p1',
    'object': '?common',
    'graph': 'watdiv100'
}


@pytest.mark.asyncio
async def test_projection_read():
    context = { 'quantum': 10e7, 'max_results': 10e7 }
    scan = ScanIterator(hdtDoc, triple, context)
    proj = ProjectionIterator(scan, context, ['?common'])
    (results, saved, done, _) = await engine.execute(proj, context)
    assert len(results) == scan.__len__()
    for res in results:
        assert '?common' in res and '?s1' not in res
    assert done


@pytest.mark.asyncio
async def test_projection_read_stopped():
    context = { 'quantum': 10e7, 'max_results': 10e-4 }
    scan = ScanIterator(hdtDoc, triple, context)
    proj = ProjectionIterator(scan, context, ['?common'])
    (results, saved, done, _) = await engine.execute(proj, context)
    assert len(results) <= scan.__len__()
    for res in results:
        assert '?common' in res and '?s1' not in res
