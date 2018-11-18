# lookup_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from http_server.server import sage_app
from rdflib import Graph

app = sage_app('tests/data/test_config.yaml')
client = app.test_client()


def test_entity_lookup():
    ref_graph = Graph()
    ref_graph.parse("tests/entity/data/city0.ttl", format="ttl")
    # dereference entity City0
    url = "http://localhost:8000/entity/foo/City0"
    deref = client.get(url)
    # load downloaded N-triples into a graph and test if it is isomorphic to the reference graph
    desc_graph = Graph()
    desc_graph.parse(data=deref.data, format="nt")
    assert ref_graph.isomorphic(desc_graph)
