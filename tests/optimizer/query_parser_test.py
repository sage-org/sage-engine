# query_parser_test.py
# Author: Thomas MINIER - MIT License 2017-2018
import pytest
from sage.query_engine.sage_engine import SageEngine
from sage.query_engine.optimizer.query_parser import parse_query
from sage.database.hdt.connector import HDTFileConnector
from tests.utils import DummyDataset
import math


hdtDoc = HDTFileConnector('tests/data/watdiv.10M.hdt')
dataset = DummyDataset(hdtDoc, 'watdiv100')
engine = SageEngine()

queries = [
    ("""
    SELECT * WHERE {
        ?s <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country9> .
        ?s <http://purl.org/goodrelations/includes> ?includes .
        ?s <http://purl.org/goodrelations/validThrough> ?validity .
    }
    """, 2180),
    ("""
    SELECT *
    FROM <http://localhost:8000/sparql/watdiv100>
    WHERE {
        ?s <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country9> .
        ?s <http://purl.org/goodrelations/includes> ?includes .
        ?s <http://purl.org/goodrelations/validThrough> ?validity .
    }
    """, 2180),
    ("""
    SELECT * WHERE {
        {
            ?s <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country9> .
            ?s <http://purl.org/goodrelations/includes> ?includes .
            ?s <http://purl.org/goodrelations/validThrough> ?validity .
        } UNION {
            ?s <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country9> .
            ?s <http://purl.org/goodrelations/includes> ?includes .
            ?s <http://purl.org/goodrelations/validThrough> ?validity .
        }
    }
    """, 2180 * 2),
    ("""
    SELECT * WHERE {
        <http://db.uwaterloo.ca/~galuc/wsdbm/Offer1000> <http://purl.org/goodrelations/price> ?price .
        FILTER(?price = "232")
    }
    """, 1),
    ("""
    SELECT * WHERE {
        <http://db.uwaterloo.ca/~galuc/wsdbm/Offer1000> <http://purl.org/goodrelations/price> ?price .
        FILTER(?price = "232" && 1 + 2 = 3)
    }
    """, 1),
    ("""
    SELECT * WHERE {
        ?s <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country9> .
        GRAPH <http://localhost:8000/sparql/watdiv100> {
            ?s <http://purl.org/goodrelations/includes> ?includes .
            ?s <http://purl.org/goodrelations/validThrough> ?validity .
        }
    }
    """, 2180)]


class TestQueryParser(object):
    @pytest.mark.asyncio
    @pytest.mark.parametrize("query,cardinality", queries)
    async def test_query_parser(self, query, cardinality):
        iterator, cards = parse_query(query, dataset, 'watdiv100', 'http://localhost:8000/sparql/')
        assert len(cards) > 0
        (results, saved, done, _) = await engine.execute(iterator, math.inf)
        assert len(results) == cardinality
        assert done
