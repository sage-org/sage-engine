# bgp_interface_test.py
# Author: Thomas MINIER - MIT License 2017-2018
import pytest
from sage.http_server.server import run_app
from starlette.testclient import TestClient
from tests.http.utils import post_sparql

union_queries = [
    ("""
    SELECT * WHERE {
        {
            <http://db.uwaterloo.ca/~galuc/wsdbm/Offer1000> <http://schema.org/eligibleRegion> ?o .
        } UNION {
            <http://db.uwaterloo.ca/~galuc/wsdbm/Offer1001> <http://schema.org/eligibleRegion> ?o .
        }
    }
    """, 7),
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
    """, 2180 * 2)
]


class TestUnionInterface(object):
    @classmethod
    def setup_class(self):
        self._app = run_app('tests/data/test_config.yaml')
        self._client = TestClient(self._app)

    @classmethod
    def teardown_class(self):
        pass

    @pytest.mark.parametrize("query,cardinality", union_queries)
    def test_union_interface(self, query, cardinality):
        nbResults = 0
        nbCalls = 0
        hasNext = True
        next_link = None
        while hasNext:
            response = post_sparql(self._client, query, next_link, 'http://testserver/sparql/watdiv100')
            assert response.status_code == 200
            response = response.json()
            nbResults += len(response['bindings'])
            hasNext = response['hasNext']
            next_link = response['next']
            nbCalls += 1
        assert nbResults == cardinality
        assert nbCalls >= 1
