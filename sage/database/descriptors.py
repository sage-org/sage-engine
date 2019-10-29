# descriptors.py
# Author: Thomas MINIER - MIT License 2017-2018
from abc import ABC, abstractmethod
from rdflib import Graph, BNode, URIRef, Literal, Namespace
from rdflib.namespace import DCTERMS, FOAF, RDF, RDFS, VOID, XSD
from math import isinf

HYDRA = Namespace("http://www.w3.org/ns/hydra/core#")
SAGE = Namespace("http://sage.univ-nantes.fr/sage-voc#")
SD = Namespace("http://www.w3.org/ns/sparql-service-description#")
W3C_FORMATS = Namespace("http://www.w3.org/ns/formats/")


def bind_prefixes(graph):
    """
        Bind commodity prefixes to a rdflib Graph.
        Generate readable prefixes when serializing the graph to turtle.
    """
    graph.bind("dcterms", "http://purl.org/dc/terms/")
    graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
    graph.bind("hydra", "http://www.w3.org/ns/hydra/core#")
    graph.bind("sage", "http://sage.univ-nantes.fr/sage-voc#")
    graph.bind("sd", "http://www.w3.org/ns/sparql-service-description#")
    graph.bind("void", "http://rdfs.org/ns/void#")


def many_void(endpoint_uri, dataset, format, encoding="utf-8"):
    """
        Describe a collection of RDF dataset using VOID + SPARQL Description languages.
        Supported formats: 'xml', 'json-ld', 'n3', 'turtle', 'nt', 'pretty-xml', 'trix', 'trig' and 'nquads'.

        Args:
            - endpoint_uri [string] - URI used to describe the endpoint
            - dataset [DatasetCollection] - Collection of dataset to describe
            - format [string] - RDF serialization format of the description
            - encoding [string="utf-8"] - String encoding (default to utf-8)
    """
    sage_uri = URIRef(endpoint_uri)
    graph_collec = BNode()
    g = Graph()
    bind_prefixes(g)
    # description of the sage endpoint itself
    g.add((sage_uri, RDF["type"], SAGE["SageEndpoint"]))
    g.add((sage_uri, FOAF["homepage"], sage_uri))
    if dataset.name is not None:
        g.add((sage_uri, DCTERMS["title"], Literal(dataset.name)))
    if dataset.maintainer is not None:
        g.add((sage_uri, DCTERMS["maintainer"], Literal(dataset.maintainer)))
    g.add((sage_uri, SD["availableGraphs"], graph_collec))
    g.add((graph_collec, RDF["type"], SD["GraphCollection"]))
    # describe each dataset available
    for d_name, dataset in dataset._graphs.items():
        d_node = BNode()
        u = "{}/sparql/{}".format(endpoint_uri, d_name)
        # add relation between dataset collection and the current dataset
        g.add((graph_collec, SD["namedGraph"], d_node))
        g.add((d_node, SD["name"], URIRef(u)))
        g.add((d_node, SD["graph"], URIRef(u)))
        # add all triples from the dataset's description itself
        g += VoidDescriptor(u, dataset)._rdf_graph
    return g.serialize(format=format).decode(encoding)


class AbstractDescriptor(ABC):
    """A descriptor describes a RDF dataset using a given vocabulary/standard"""

    def __init__(self):
        super(AbstractDescriptor, self).__init__()

    @abstractmethod
    def describe(self, format, encoding="utf-8"):
        """
            Describe the dataset using the given format, and returns the description as a string.

            Args:
                - format [string] - RDF serialization format of the description
                - encoding [string="utf-8"] - String encoding (default to utf-8)
        """
        pass


class VoidDescriptor(AbstractDescriptor):
    """A descriptor that describes a Sage dataset using the VOID standard"""

    def __init__(self, url, graph):
        super(VoidDescriptor, self).__init__()
        self._graph = graph
        self._graph_url = URIRef(url)
        self._rdf_graph = Graph()
        bind_prefixes(self._rdf_graph)
        self.__populate_graph()

    def describe(self, format, encoding="utf-8"):
        """
            Describe the dataset using the given format, and returns the description as a string.
            Supported formats: 'xml', 'json-ld', 'n3', 'turtle', 'nt', 'pretty-xml', 'trix', 'trig' and 'nquads'.

            Args:
                - format [string] - RDF serialization format of the description
                - encoding [string="utf-8"] - String encoding (default to utf-8)
        """
        return self._rdf_graph.serialize(format=format).decode(encoding)

    def __populate_graph(self):
        """Fill the local triple store with dataset's metadata"""
        # main metadata
        self._rdf_graph.add((self._graph_url, RDF["type"], SAGE["SageDataset"]))
        self._rdf_graph.add((self._graph_url, FOAF["homepage"], self._graph_url))
        self._rdf_graph.add((self._graph_url, DCTERMS["title"], Literal(self._graph.name)))
        self._rdf_graph.add((self._graph_url, DCTERMS["description"], Literal(self._graph.description)))
        # sage specific metadata (access endpoint, quota, max results per page, etc)
        self._rdf_graph.add((self._graph_url, VOID["feature"], W3C_FORMATS["SPARQL_Results_JSON"]))
        self._rdf_graph.add((self._graph_url, VOID["feature"], W3C_FORMATS["SPARQL_Results_XML"]))
        self._rdf_graph.add((self._graph_url, SD["endpoint"], self._graph_url))
        self._rdf_graph.add((self._graph_url, HYDRA["entrypoint"], self._graph_url))
        if isinf(self._graph.quota):
            self._rdf_graph.add((self._graph_url, SAGE["quota"], Literal("Infinity")))
        else:
            self._rdf_graph.add((self._graph_url, SAGE["quota"], Literal(self._graph.quota, datatype=XSD.integer)))
        if isinf(self._graph.max_results):
            self._rdf_graph.add((self._graph_url, HYDRA["itemsPerPage"], Literal("Infinity")))
        else:
            self._rdf_graph.add((self._graph_url, HYDRA["itemsPerPage"], Literal(self._graph.max_results, datatype=XSD.integer)))
        # HDT statistics
        self._rdf_graph.add((self._graph_url, VOID["triples"], Literal(self._graph.nb_triples, datatype=XSD.integer)))
        self._rdf_graph.add((self._graph_url, VOID["distinctSubjects"], Literal(self._graph._connector.nb_subjects, datatype=XSD.integer)))
        self._rdf_graph.add((self._graph_url, VOID["properties"], Literal(self._graph._connector.nb_predicates, datatype=XSD.integer)))
        self._rdf_graph.add((self._graph_url, VOID["distinctObjects"], Literal(self._graph._connector.nb_objects, datatype=XSD.integer)))
        # if "license" in d_config:
        #     self._graph.add((self._graph_url, DCTERMS["license"], URIRef(d_config["license"])))
        # add example queries
        for query in self._graph.example_queries:
            q_node = BNode()
            self._rdf_graph.add((self._graph_url, SAGE["hasExampleQuery"], q_node))
            self._rdf_graph.add((q_node, RDF["type"], SAGE["ExampleQuery"]))
            self._rdf_graph.add((q_node, RDFS["label"], Literal(query["name"])))
            self._rdf_graph.add((q_node, RDF["value"], Literal(query["value"])))
