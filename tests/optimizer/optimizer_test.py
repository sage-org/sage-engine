# optimizer_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.optimizer.plan_builder import build_query_plan
from database.hdt_file_connector import HDTFileConnector
from query_engine.iterators.projection import ProjectionIterator
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.nlj import IndexJoinIterator
from query_engine.iterators.union import BagUnionIterator

hdtDoc = HDTFileConnector('tests/data/watdiv.10M.hdt')


def test_empty_patterns():
    bgp = [
        {
            'subject': '?s',
            'predicate': 'http://schema.org/unkwnownPredicate',
            'object': '?o1'
        },
        {
            'subject': '?s',
            'predicate': 'http://schema.org/sillyPredicate',
            'object': '?o2'
        }
    ]
    query = {
        'type': 'bgp',
        'bgp': bgp
    }
    plan, c = build_query_plan(query, hdtDoc)
    assert not plan.has_next()


def test_build_left_linear_plan():
    bgp = [
        {
            'subject': '?s',
            'predicate': 'http://schema.org/eligibleRegion',
            'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
        },
        {
            'subject': '?s',
            'predicate': 'http://purl.org/goodrelations/includes',
            'object': '?includes'
        }
    ]
    query = {
        'type': 'bgp',
        'bgp': bgp
    }
    plan, c = build_query_plan(query, hdtDoc)
    assert type(plan) is ProjectionIterator
    assert type(plan._source) is IndexJoinIterator
    assert plan._source._innerTriple == bgp[1]
    assert type(plan._source._source) is ScanIterator
    assert plan._source._source._triple == bgp[0]


def test_build_union():
    query = {
        'type': 'union',
        'union': [
            [
                {
                    'subject': '?s',
                    'predicate': 'http://schema.org/eligibleRegion',
                    'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
                }
            ],
            [
                {
                    'subject': '?s',
                    'predicate': 'http://purl.org/goodrelations/includes',
                    'object': '?includes'
                }
            ]
        ]
    }
    plan, c = build_query_plan(query, hdtDoc)
    assert type(plan) is BagUnionIterator


def test_load_from_savedPlan():
    bgp = [
        {
            'subject': '?s',
            'predicate': 'http://schema.org/eligibleRegion',
            'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
        },
        {
            'subject': '?s',
            'predicate': 'http://purl.org/goodrelations/includes',
            'object': '?includes'
        }
    ]
    query = {
        'type': 'bgp',
        'bgp': bgp
    }
    plan, c = build_query_plan(query, hdtDoc)
    savedPlan = plan.save()
    plan, c = build_query_plan(None, hdtDoc, savedPlan)
    assert type(plan) is ProjectionIterator
    assert type(plan._source) is IndexJoinIterator
    assert plan._source._innerTriple == bgp[1]
    assert type(plan._source._source) is ScanIterator
    assert plan._source._source._triple == bgp[0]
