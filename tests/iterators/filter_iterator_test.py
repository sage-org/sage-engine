# filter_iterator_test.py
# Author: Thomas MINIER - MIT License 2017-2020
import pytest
from sage.query_engine.sage_engine import SageEngine
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.loader import load
from sage.database.hdt.connector import HDTFileConnector
from tests.utils import DummyDataset
import math


hdtDoc = HDTFileConnector('tests/data/watdiv.10M.hdt')
engine = SageEngine()
triple = {
    'subject': 'http://db.uwaterloo.ca/~galuc/wsdbm/Offer1000',
    'predicate': '?p',
    'object': '?o',
    'graph': 'watdiv100'
}


@pytest.mark.asyncio
async def test_simple_filter_iterator():
    context = { 'quantum': 10e7, 'max_results': 10e7 }
    expression = "?p = <http://schema.org/eligibleRegion>"
    scan = ProjectionIterator(ScanIterator(hdtDoc, triple, context), context)
    iterator = FilterIterator(scan, expression, context)
    (results, saved, done, _) = await engine.execute(iterator, context)
    assert len(results) == 4
    for b in results:
        assert b['?p'] == 'http://schema.org/eligibleRegion'
        assert b['?o'] in [
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country0',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country1',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country4',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
        ]


@pytest.mark.asyncio
async def test_and_or_filter_iterator():
    context = { 'quantum': 10e7, 'max_results': 10e7 }
    expression = "?p = <http://schema.org/eligibleRegion> && (?o = <http://db.uwaterloo.ca/~galuc/wsdbm/Country0> || ?o = <http://db.uwaterloo.ca/~galuc/wsdbm/Country9>)"
    scan = ProjectionIterator(ScanIterator(hdtDoc, triple, context), context)
    iterator = FilterIterator(scan, expression, context)
    (results, saved, done, _) = await engine.execute(iterator, context)
    assert len(results) == 2
    for b in results:
        assert b['?p'] == 'http://schema.org/eligibleRegion'
        assert b['?o'] in [
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country0',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
        ]


@pytest.mark.asyncio
async def test_operation_filter_iterator():
    context = { 'quantum': 10e7, 'max_results': 10e7 }
    expression = "10 = 5 * 2"
    scan = ProjectionIterator(ScanIterator(hdtDoc, triple, context), context)
    iterator = FilterIterator(scan, expression, context)
    (results, saved, done, _) = await engine.execute(iterator, context)
    assert len(results) == 9


# @pytest.mark.asyncio
# async def test_function_filter_iterator():
#     context = { 'quantum': 10e7, 'max_results': 10e7 }
#     expression = '?p = <http://purl.org/goodrelations/price> && isLiteral(?o) && !isNumeric(?o)'
#     scan = ProjectionIterator(ScanIterator(hdtDoc, triple, context), context)
#     iterator = FilterIterator(scan, expression, context)
#     (results, saved, done, _) = await engine.execute(iterator, context)
#     assert len(results) == 1


@pytest.mark.asyncio
async def test_filter_iterator_interrupt():
    context = { 'quantum': 10e-7, 'max_results': 10e7 }
    expression = "?p = <http://schema.org/eligibleRegion>"
    scan = ProjectionIterator(ScanIterator(hdtDoc, triple, context), context)
    iterator = FilterIterator(scan, expression, context)
    (results, saved, done, _) = await engine.execute(iterator, context)
    assert len(results) <= 4
    for b in results:
        assert b['?p'] == 'http://schema.org/eligibleRegion'
        assert b['?o'] in [
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country0',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country1',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country4',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
        ]
    tmp = len(results)
    context['quantum'] = 10e7
    reloaded = load(saved.SerializeToString(), DummyDataset(hdtDoc, 'watdiv100'), context)
    (results, saved, done, _) = await engine.execute(reloaded, context)
    assert len(results) + tmp == 4
    for b in results:
        assert b['?p'] == 'http://schema.org/eligibleRegion'
        assert b['?o'] in [
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country0',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country1',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country4',
            'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
        ]
    assert done
