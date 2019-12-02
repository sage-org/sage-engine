# bgp_interface_test.py
# Author: Thomas MINIER - MIT License 2017-2018
import pytest
from sage.http_server.server import sage_app
from tests.http.utils import jsonSparql

bgp_queries = [
    ("""
        SELECT * WHERE {
            ?s <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country9> .
            ?s <http://purl.org/goodrelations/includes> ?includes .
            ?s <http://purl.org/goodrelations/validThrough> ?validity .
        }
    """, 2180),
    # ("""
    #     SELECT * WHERE {
    #         ?v0 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country9> .
    #         ?v0 <http://purl.org/goodrelations/includes> ?v1 .
    #         ?v1 <http://schema.org/contentSize> ?v3.
    #     }
    # """, 531),
    # ("""
    #     SELECT * WHERE {
    #         ?s <http://xmlns.com/foaf/age> <http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup3> .
    #         ?s <http://schema.org/nationality> <http://db.uwaterloo.ca/~galuc/wsdbm/Country1> .
    #         ?s <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender1> .
    #     }
    # """, 93),
    # ("""
    #     SELECT * WHERE {
    #         ?s <http://xmlns.com/foaf/age> ?s .
    #     }
    # """, 0),
]


class TestBGPInterface(object):
    @classmethod
    def setup_class(self):
        app = sage_app('tests/data/test_config.yaml')
        app.config['TESTING'] = True
        with app.test_client() as client:
            self.client = client

    @classmethod
    def teardown_class(self):
        pass

    @pytest.mark.parametrize("query,cardinality", bgp_queries)
    def test_bgp_interface(self, query, cardinality):
        nbResults = 0
        nbCalls = 0
        hasNext = True
        next_link = None
        while hasNext:
            response = jsonSparql(self.client, query, next_link, 'http://localhost/sparql/watdiv100')
            nbResults += len(response['bindings'])
            hasNext = response['hasNext']
            next_link = response['next']
            nbCalls += 1
        assert nbResults == cardinality
        assert nbCalls >= 1
