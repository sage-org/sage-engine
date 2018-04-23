# rdf_file_connector_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from database.rdf_file_connector import RDFFileConnector, TripleIndex

db = RDFFileConnector("data/test.ttl", format="ttl", useCache=True)


def test_triple_index():
    index = TripleIndex()
    index.insert((1, 1, 2), (1, 1, 2))
    index.insert((2, 1, 2), (2, 1, 2))
    index.insert((3, 2, 2), (3, 2, 2))
    index.insert((3, 1, 2), (3, 1, 2))
    for v in index.search_pattern((0, 1, 2)):
        assert v in [(1, 1, 2), (2, 1, 2), (3, 1, 2)]


def test_containment_search_rdf_file():
    expected = ('http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer13/Product593', 'http://www.w3.org/2000/01/rdf-schema#label', '"byplay"')
    iterator, card = db.search_triples(
        "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer13/Product593",
        "http://www.w3.org/2000/01/rdf-schema#label",
        '"byplay"')
    nb = 0
    for v in iterator:
        assert v == expected
        nb += 1
    assert nb == 1


def test_subject_search_rdf_file():
    iterator, card = db.search_triples(
        "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer13/Product593",
        None, None)
    for v in iterator:
        assert v[0] == "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer13/Product593"


def test_predicate_search_rdf_file():
    iterator, card = db.search_triples(
        None, "http://www.w3.org/2000/01/rdf-schema#label", None)
    for v in iterator:
        assert v[1] == "http://www.w3.org/2000/01/rdf-schema#label"


def test_object_search_rdf_file():
    iterator, card = db.search_triples(
        None, "http://www.w3.org/2000/01/rdf-schema#label", None)
    for v in iterator:
        assert v[1] == "http://www.w3.org/2000/01/rdf-schema#label"


def test_mixed_search_rdf_file():
    expected = ('http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer13/Product593', 'http://www.w3.org/2000/01/rdf-schema#label', '"byplay"')
    iterator, card = db.search_triples(
        None,
        "http://www.w3.org/2000/01/rdf-schema#label",
        '"byplay"')
    for v in iterator:
        assert v == expected
