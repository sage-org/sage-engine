# descriptors.py
# Author: Thomas MINIER - MIT License 2017-2020
from abc import ABC, abstractmethod
from math import isinf

from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, FOAF, RDF, RDFS, VOID, XSD

from sage.database.core.dataset import Dataset
from sage.database.core.graph import Graph as SageGraph

HYDRA = Namespace("http://www.w3.org/ns/hydra/core#")
SAGE = Namespace("http://sage.univ-nantes.fr/sage-voc#")
SD = Namespace("http://www.w3.org/ns/sparql-service-description#")
W3C_FORMATS = Namespace("http://www.w3.org/ns/formats/")


def bind_prefixes(graph: Graph) -> None:
    """Bind commons prefixes to a rdflib Graph.
    
    Generate readable prefixes when serializing the graph to turtle.

    Argument: The rdflib Graph to which prefixes should be added.
    """
    graph.bind("dcterms", "http://purl.org/dc/terms/")
    graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
    graph.bind("hydra", "http://www.w3.org/ns/hydra/core#")
    graph.bind("sage", "http://sage.univ-nantes.fr/sage-voc#")
    graph.bind("sd", "http://www.w3.org/ns/sparql-service-description#")
    graph.bind("void", "http://rdfs.org/ns/void#")


def many_void(endpoint_uri: str, dataset: Dataset, rdf_format: str, encoding: str = "utf-8") -> str:
    """Describe a RDF dataset hosted by a Sage server using the VOID and SPARQL Description languages.
    
    Supported RDF formats: 'xml', 'json-ld', 'n3', 'turtle', 'nt', 'pretty-xml', 'trix', 'trig' and 'nquads'.

    Args:
      * endpoint_uri: URI used to describe the endpoint.
      * dataset: RDF dataset to describe.
      * rdf_format: RDF serialization format for the description.
      * encoding: String encoding (Default to utf-8).

    Returns:
      The description of the RDF dataset, formatted in the given RDF format.
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
    for g_uri, graph in dataset._graphs.items():
        d_node = BNode()
        # add relation between dataset collection and the current dataset
        g.add((graph_collec, SD["namedGraph"], d_node))
        g.add((d_node, SD["name"], URIRef(graph.name)))
        g.add((d_node, SD["graph"], URIRef(g_uri)))
        # add all triples from the dataset's description itself
        g += VoidDescriptor(g_uri, graph)._rdf_graph
    return g.serialize(format=rdf_format).decode(encoding)


class AbstractDescriptor(ABC):
    """A descriptor describes a RDF dataset using a given vocabulary/standard"""

    def __init__(self):
        super(AbstractDescriptor, self).__init__()

    @abstractmethod
    def describe(self, format: str, encoding="utf-8") -> str:
        """Describe the dataset using the given format.

        Supported RDF formats: 'xml', 'json-ld', 'n3', 'turtle', 'nt', 'pretty-xml', 'trix', 'trig' and 'nquads'.

        Args:
          * rdf_format: RDF serialization format for the description.
          * encoding: String encoding (Default to utf-8).

        Returns:
          The description of the RDF dataset, formatted in the given RDF format.
        """
        pass


class VoidDescriptor(AbstractDescriptor):
    """A descriptor that describes a Sage dataset using the VOID standard.
    
    Args:
      * uri: URI of the RDF graph to describe.
      * graph: the RDF Graph to describe.

    Example:
      >>> graph = get_some_graph() # get a RDF graph
      >>> uri = "http://example.org#my-graph"
      >>> desc = VoidDescriptor(uri, graph)
      >>> print(desc.describe("turtle"))
    """

    def __init__(self, uri: str, graph: SageGraph):
        super(VoidDescriptor, self).__init__()
        self._graph = graph
        self._graph_uri = URIRef(uri)
        self._rdf_graph = Graph()
        bind_prefixes(self._rdf_graph)
        self.__populate_graph()

    def describe(self, format: str, encoding="utf-8") -> str:
        """Describe the dataset using the given format.

        Supported RDF formats: 'xml', 'json-ld', 'n3', 'turtle', 'nt', 'pretty-xml', 'trix', 'trig' and 'nquads'.

        Args:
          * rdf_format: RDF serialization format for the description.
          * encoding: String encoding (Default to utf-8).

        Returns:
          The description of the RDF dataset, formatted in the given RDF format.
        """
        return self._rdf_graph.serialize(format=format).decode(encoding)

    def __populate_graph(self) -> None:
        """Fill the local triple store with dataset's metadata"""
        # main metadata
        self._rdf_graph.add((self._graph_uri, RDF["type"], SAGE["SageDataset"]))
        self._rdf_graph.add((self._graph_uri, FOAF["homepage"], self._graph_uri))
        self._rdf_graph.add((self._graph_uri, DCTERMS["title"], Literal(self._graph.name)))
        self._rdf_graph.add((self._graph_uri, DCTERMS["description"], Literal(self._graph.description)))
        # sage specific metadata (access endpoint, quota, max results per page, etc)
        self._rdf_graph.add((self._graph_uri, VOID["feature"], W3C_FORMATS["SPARQL_Results_JSON"]))
        self._rdf_graph.add((self._graph_uri, VOID["feature"], W3C_FORMATS["SPARQL_Results_XML"]))
        self._rdf_graph.add((self._graph_uri, SD["endpoint"], self._graph_uri))
        self._rdf_graph.add((self._graph_uri, HYDRA["entrypoint"], self._graph_uri))
        if isinf(self._graph.quota):
            self._rdf_graph.add((self._graph_uri, SAGE["quota"], Literal("Infinity")))
        else:
            self._rdf_graph.add((self._graph_uri, SAGE["quota"], Literal(self._graph.quota, datatype=XSD.integer)))
        if isinf(self._graph.max_results):
            self._rdf_graph.add((self._graph_uri, HYDRA["itemsPerPage"], Literal("Infinity")))
        else:
            self._rdf_graph.add((self._graph_uri, HYDRA["itemsPerPage"], Literal(self._graph.max_results, datatype=XSD.integer)))
        # HDT statistics
        self._rdf_graph.add((self._graph_uri, VOID["triples"], Literal(self._graph.nb_triples, datatype=XSD.integer)))
        self._rdf_graph.add((self._graph_uri, VOID["distinctSubjects"], Literal(self._graph._connector.nb_subjects, datatype=XSD.integer)))
        self._rdf_graph.add((self._graph_uri, VOID["properties"], Literal(self._graph._connector.nb_predicates, datatype=XSD.integer)))
        self._rdf_graph.add((self._graph_uri, VOID["distinctObjects"], Literal(self._graph._connector.nb_objects, datatype=XSD.integer)))
        # if "license" in d_config:
        #     self._graph.add((self._graph_uri, DCTERMS["license"], URIRef(d_config["license"])))
        # add example queries
        for query in self._graph.example_queries:
            q_node = BNode()
            self._rdf_graph.add((self._graph_uri, SAGE["hasExampleQuery"], q_node))
            self._rdf_graph.add((q_node, RDF["type"], SAGE["ExampleQuery"]))
            self._rdf_graph.add((q_node, RDFS["label"], Literal(query["name"])))
            self._rdf_graph.add((q_node, RDF["value"], Literal(query["value"])))
