# void_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from database.datasets import DatasetCollection
from database.descriptors import VoidDescriptor, many_void
from rdflib import Graph

datasets = DatasetCollection("data/test_config.yaml")


def test_describe_dataset_void():
    ref_graph = Graph()
    ref_graph.parse("tests/descriptors/data/watdiv100_description.ttl", format="ttl")
    # generate description
    url = "http://localhost:8000/sparql/watdiv100"
    dataset = datasets.get_dataset("watdiv100")
    descriptor = VoidDescriptor(url, dataset)
    desc_graph = Graph()
    desc_graph.parse(data=descriptor.describe("turtle"), format="ttl")
    assert ref_graph.isomorphic(desc_graph)


def test_describe_many_dataset_void():
    ref_graph = Graph()
    ref_graph.parse("tests/descriptors/data/describe_all.ttl", format="ttl")
    # generate description
    url = "http://localhost:8000"
    desc_graph = Graph()
    desc_graph.parse(data=many_void(url, datasets, "turtle"), format="ttl")
    assert ref_graph.isomorphic(desc_graph)
