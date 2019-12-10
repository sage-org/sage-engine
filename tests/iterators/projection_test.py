# scan_test.py
# Author: Thomas MINIER - MIT License 2017-2020
import pytest
from sage.query_engine.sage_engine import SageEngine
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.database.hdt.connector import HDTFileConnector

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
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    proj = ProjectionIterator(scan, ['?common'])
    (results, saved, done, _) = await engine.execute(proj, 10e7)
    assert len(results) == card
    for res in results:
        assert '?common' in res and '?s1' not in res
    assert done


@pytest.mark.asyncio
async def test_projection_read_stopped():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    proj = ProjectionIterator(scan, ['?common'])
    (results, saved, done, _) = await engine.execute(proj, 10e-4)
    assert len(results) <= card
    for res in results:
        assert '?common' in res and '?s1' not in res
