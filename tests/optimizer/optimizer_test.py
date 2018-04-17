# optimizer_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from query_engine.optimizer.plan_builder import build_query_plan
from datasets.hdt_file_factory import HDTFileFactory
from query_engine.iterators.projection import ProjectionIterator
from query_engine.iterators.scan import ScanIterator
from query_engine.iterators.nlj import NestedLoopJoinIterator
from query_engine.iterators.union import BagUnionIterator
from query_engine.iterators.utils import EmptyIterator

hdtDoc = HDTFileFactory('data/watdiv.10M.hdt')


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
    plan = build_query_plan(query, hdtDoc)
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
    plan = build_query_plan(query, hdtDoc)
    assert type(plan) is ProjectionIterator
    assert type(plan._source) is NestedLoopJoinIterator
    assert plan._source._innerTriple == bgp[1]
    assert type(plan._source._source) is ScanIterator
    assert plan._source._source._triple == bgp[0]


def test_build_union():
    query = {
        'type': 'union',
        'patterns': [
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
    plan = build_query_plan(query, hdtDoc)
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
    savedPlan = build_query_plan(query, hdtDoc).save()
    plan = build_query_plan(None, hdtDoc, savedPlan)
    assert type(plan) is ProjectionIterator
    assert type(plan._source) is NestedLoopJoinIterator
    assert plan._source._innerTriple == bgp[1]
    assert type(plan._source._source) is ScanIterator
    assert plan._source._source._triple == bgp[0]
