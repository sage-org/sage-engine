# bgp_interface_test.py
# Author: Thomas MINIER - MIT License 2017-2018
import grpc
import pytest
from sage.grpc import service_pb2_grpc
from sage.grpc.grpc_server import get_server
from sage.grpc.service_pb2 import SageQuery

bgp_queries = [
    ("""
        SELECT * WHERE {
            ?s <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country9> .
            ?s <http://purl.org/goodrelations/includes> ?includes .
            ?s <http://purl.org/goodrelations/validThrough> ?validity .
        }
    """, 2180),
    ("""
        SELECT * WHERE {
            ?v0 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country9> .
            ?v0 <http://purl.org/goodrelations/includes> ?v1 .
            ?v1 <http://schema.org/contentSize> ?v3.
        }
    """, 531),
    ("""
        SELECT * WHERE {
            ?s <http://xmlns.com/foaf/age> <http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup3> .
            ?s <http://schema.org/nationality> <http://db.uwaterloo.ca/~galuc/wsdbm/Country1> .
            ?s <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender1> .
        }
    """, 93),
    ("""
        SELECT * WHERE {
            ?s <http://xmlns.com/foaf/age> ?s .
        }
    """, 0)
]


class TestGRPCInterface(object):
    @classmethod
    def setup_class(self):
      self._server = get_server('tests/data/test_config.yaml', workers=1)
      self._server.start()

    @classmethod
    def teardown_class(self):
      self._server.stop(None)

    @pytest.mark.parametrize("query,cardinality", bgp_queries)
    def test_grpc_interface(self, query, cardinality):
      with grpc.insecure_channel('localhost:8000') as channel:
        client = service_pb2_grpc.SageSPARQLStub(channel)
        grpc_query = SageQuery(query = query, default_graph_uri = 'watdiv100')
        nbResults = 0
        nbCalls = 0
        is_done = False
        next_link = None
        while not is_done:
            response = client.Query(grpc_query)
            nbResults += len(response.bindings)
            is_done = response.is_done
            next_link = response.next_link
            nbCalls += 1
            # prepare next query
            grpc_query = SageQuery(query = query, default_graph_uri = 'watdiv100', next_link = next_link)
        assert nbResults == cardinality
        assert nbCalls >= 1
