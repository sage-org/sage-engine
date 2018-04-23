# rdf_file_connector_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from database.rdf_file_connector import RDFFileConnector, TripleIndex

db = RDFFileConnector("data/test.ttl", format="ttl")


def test_triple_index():
    index = TripleIndex()
    index.insert((1, 1, 2), (1, 1, 2))
    index.insert((2, 1, 2), (2, 1, 2))
    index.insert((3, 2, 2), (3, 2, 2))
    index.insert((3, 1, 2), (3, 1, 2))
    for v in index.search_pattern((0, 1, 2)):
        assert v in [(1, 1, 2), (2, 1, 2), (3, 1, 2)]

def test_rdf_file():
    # "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer13/Product593"
    iterator, card = db.search_triples(
        None,
        "http://www.w3.org/2000/01/rdf-schema#label",
        '"byplay"')
    for v in iterator:
        print(v)
