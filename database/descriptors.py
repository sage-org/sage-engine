# descriptors.py
# Author: Thomas MINIER - MIT License 2017-2018
from abc import ABC, abstractmethod
from rdflib import Graph, BNode, URIRef, Literal, Namespace
from rdflib.namespace import DCTERMS, FOAF, RDF, VOID, XSD

HYDRA = Namespace("http://www.w3.org/ns/hydra/core#")
SAGE = Namespace("http://sage.univ-nantes.fr/owl#")
SD = Namespace("http://www.w3.org/ns/sparql-service-description#")


def bind_prefixes(graph):
    graph.bind("dcterms", "http://purl.org/dc/terms/")
    graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
    graph.bind("hydra", "http://www.w3.org/ns/hydra/core#")
    graph.bind("sage", "http://sage.univ-nantes.fr/owl#")
    graph.bind("sd", "http://www.w3.org/ns/sparql-service-description#")
    graph.bind("void", "http://rdfs.org/ns/void#")


def many_void(root_url, datasets, format):
    """Describe a collection of RDF datasets using VOID + SPARQL Description languages"""
    sage_uri = URIRef(root_url)
    graph_collec = BNode()
    g = Graph()
    bind_prefixes(g)
    # description of the sage endpoint itself
    g.add((sage_uri, RDF["type"], SAGE["SageEndpoint"]))
    g.add((sage_uri, RDF["type"], SD["Service"]))
    g.add((sage_uri, FOAF["homepage"], sage_uri))
    if datasets.name is not None:
        g.add((sage_uri, DCTERMS["title"], Literal(datasets.name)))
    if datasets.maintainer is not None:
        g.add((sage_uri, DCTERMS["maintainer"], Literal(datasets.maintainer)))
    g.add((sage_uri, SD["availableGraphs"], graph_collec))
    g.add((graph_collec, RDF["type"], SD["GraphCollection"]))
    # describe each dataset available
    for d_name, dataset in datasets._datasets.items():
        d_node = BNode()
        u = "{}/sparql/{}".format(root_url, d_name)
        # add relation between datasets collection and the current dataset
        g.add((graph_collec, SD["namedGraph"], d_node))
        g.add((d_node, SD["name"], URIRef(u)))
        g.add((d_node, SD["graph"], URIRef(u)))
        # add all triples from the dataset's description itself
        g += VoidDescriptor(u, dataset)._graph
    return g.serialize(format=format)


class AbstractDescriptor(ABC):
    """A descriptor describes a RDF dataset using a given vocabulary/standard"""

    def __init__(self):
        super(AbstractDescriptor, self).__init__()

    @abstractmethod
    def describe(self, format):
        """Describe the dataset using the given format, and returns the description as a string."""
        pass


class VoidDescriptor(AbstractDescriptor):
    """A descriptor that describes a Sage dataset using the VOID standard"""

    def __init__(self, url, dataset):
        super(VoidDescriptor, self).__init__()
        self._dataset = dataset
        self._dataset_url = URIRef(url)
        self._graph = Graph()
        bind_prefixes(self._graph)
        self.__populate_graph()

    def describe(self, format):
        """
            Describe the dataset using the given format, and returns the description as a string.
            Supported formats: 'xml', 'n3', 'turtle', 'nt', 'pretty-xml', 'trix', 'trig' and 'nquads'
        """
        return self._graph.serialize(format=format)

    def __populate_graph(self):
        """Fill the local triple store with dataset's metadata"""
        d_config = self._dataset.config()
        # main metadata
        self._graph.add((self._dataset_url, RDF["type"], VOID["Dataset"]))
        self._graph.add((self._dataset_url, RDF["type"], SD["Graph"]))
        self._graph.add((self._dataset_url, FOAF["homepage"], self._dataset_url))
        self._graph.add((self._dataset_url, DCTERMS["title"], Literal(d_config["name"])))
        self._graph.add((self._dataset_url, DCTERMS["description"], Literal(d_config["description"])))
        # sage specific metadata (access endpoint, quota, max results per page, etc)
        self._graph.add((self._dataset_url, SD["endpoint"], self._dataset_url))
        self._graph.add((self._dataset_url, HYDRA["entrypoint"], self._dataset_url))
        self._graph.add((self._dataset_url, SAGE["quota"], Literal(self._dataset.quota, datatype=XSD.integer)))
        self._graph.add((self._dataset_url, HYDRA["itemsPerPage"], Literal(self._dataset.maxResults, datatype=XSD.integer)))
        self._graph.add((self._dataset_url, VOID["feature"], SAGE["SageResultsPage"]))
        # HDT statistics
        self._graph.add((self._dataset_url, VOID["triples"], Literal(self._dataset.nb_triples, datatype=XSD.integer)))
        self._graph.add((self._dataset_url, VOID["distinctSubjects"], Literal(self._dataset._connector.nb_subjects, datatype=XSD.integer)))
        self._graph.add((self._dataset_url, VOID["properties"], Literal(self._dataset._connector.nb_predicates, datatype=XSD.integer)))
        self._graph.add((self._dataset_url, VOID["distinctObjects"], Literal(self._dataset._connector.nb_objects, datatype=XSD.integer)))
        if "license" in d_config:
            self._graph.add((self._dataset_url, DCTERMS["license"], URIRef(d_config["license"])))
