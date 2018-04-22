# rdf_file_connector_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from database.rdf_file_connector import RDFFileConnector

db = RDFFileConnector("data/test.ttl", format="ttl")


def test_foo():
    iterator, c = db.search_triples(
        "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer13/Product593",
        "http://www.w3.org/2000/01/rdf-schema#label",
        '"byplay"')
    # for t in iterator:
    #     print(t)
