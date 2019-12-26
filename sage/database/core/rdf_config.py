# rdf_config.py
# Author: Thomas MINIER - MIT License 2017-2020
import logging
from math import inf

from rdflib import Graph as RGraph
from rdflib.plugins.sparql import prepareQuery

from sage.database.core.dataset import Dataset
from sage.database.core.graph import Graph
from sage.database.import_manager import builtin_backends, import_backend


def load_config(config_file: str, format="ttl") -> Dataset:
    """Parse a SaGe configuration file written in RDF and load the corresponding RDF dataset.

    Args:
      * config_file: Path to the SaGe configuration file (in RDF format) to load.
      * format: Format of the RDF configuration file (ttl, nt, n3). Defaults to Turtle (ttl).
    
    Returns:
      A RDF dataset built according to the input configuration file.
    """
    # load config usinf rdflib
    graph = RGraph()
    graph.parse(config_file, format=format)

    # available backends (populated with sage's native backends)
    backends = builtin_backends()

    # load custom backend (if there is some)
    # TODO

    # get default time quantum & maximum number of results per page
    qres = graph.query("""
    PREFIX sage: <http://sage.univ-nantes.fr/sage-voc#>
    SELECT * WHERE {
        ?server a sage:SageEndpoint; foaf:name ?name.
        OPTIONAL { ?server sage:longDescription ?description }
        OPTIONAL { ?server sage:publicUrl ?url }
        OPTIONAL { ?server sage:quantum ?quantum }
        OPTIONAL { ?server sage:pageSize ?pageSize }
        OPTIONAL { ?server sage:analytics ?analytics }
        OPTIONAL {
            ?server sage:defaultQuery ?query.
            ?query a sage:ExampleQuery;
                sage:targetGraph ?queryGraphName;
                foaf:name ?queryName;
                rdf:value ?queryValue.
        }
    }""")
    if len(qres) != 1:
        raise SyntaxError("A valid SaGe RDF configuration file must contains exactly one sage:SageEndpoint.")
    for row in qres:
        # load dataset basic informations
        dataset_name = str(row.name)
        public_url = str(row.url)
        analytics = str(row.analytics)
        if row.query is not None:
            default_query = {
                "dataset_name": str(row.queryGraphName),
                "name": str(row.queryName),
                "value": str(row.queryValue)
            }
        else:
            default_query = None

        if row.description is not None:
            with open(str(row.description), "r") as file:
                dataset_description = file.read()
        else:
            dataset_description = "A RDF dataset hosted by a SaGe server"

        # get default time quantum & maximum number of results per page
        if row.quantum is not None:
            value = row.quantum.toPython()
            if value == 'inf':
                logging.warning("You are using SaGe with an infinite time quantum. Be sure to configure the Worker timeout of Gunicorn accordingly, otherwise long-running queries might be terminated.")
                quantum = inf
            else:
                quantum = value
        else:
            quantum = 75
        if row.pageSize is not None and row.pageSize.toPython() != 'inf':
            max_results = row.pageSize.toPython()
        else:
            logging.warning("You are using SaGe without limitations on the number of results sent per page. This is fine, but be carefull as very large page of results can have unexpected serialization time.")
            max_results = inf
        break

    # prepare the query used to fetch backend informations per graph
    backend_query = prepareQuery("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX sage: <http://sage.univ-nantes.fr/sage-voc#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT ?name ?paramName ?paramValue WHERE {
        ?backend a sage:Backend;
                foaf:name ?name;
                sage:param ?param.
        ?param foaf:name ?paramName;
               rdf:value ?paramValue.
    }""")

    # build all RDF graphs found in the configuration file
    graphs = dict()
    qres = graph.query("""
    PREFIX sage: <http://sage.univ-nantes.fr/sage-voc#>
    SELECT * WHERE {
        ?server a sage:SageEndpoint;
                sage:graph ?graph.
        ?graph  a sage:SageGraph;
                foaf:name ?name ;
                sage:backend ?backend.
        OPTIONAL { ?graph dcterms:description ?desc . }
        OPTIONAL { ?graph sage:quantum ?quantum . }
        OPTIONAL { ?graph sage:pageSize ?pageSize . }
    }""")
    for row in qres:
        # load basic information about the graph
        if row.name is None:
            raise SyntaxError("A valid SaGe RDF graph must have a name (declared using foaf:name)!")
        g_name = row.name
        g_description = row.desc if row.desc is not None else "Unnamed RDF graph with id {}".format(g_name)
        g_quantum = row.quantum if row.quantum is not None else quantum
        g_max_results = row.pageSize if row.pageSize is not None else max_results

        # load default queries for this graph
        # TODO
        g_queries = list()
        # g_queries = g_config["queries"] if "queries" in g_config else list()

        # load the backend for this graph
        g_connector = None
        backend_config = dict()
        backend_name = None
        # fetch backend config. parameters first
        backend_res = graph.query(backend_query, initBindings = { "backend": row.backend })
        if len(backend_res) == 0:
            logging.error(f"Graph with name '{g_name}' has a backend declared with an invalid syntax. Please check your configuration file using the documentation.")
        else:
            for b_row in backend_res:
                backend_name = str(b_row.name)
                backend_config[str(b_row.paramName)] = str(b_row.paramValue)
            # load the graph connector using available backends
            if backend_name in backends:
                g_connector = backends[backend_name](backend_config)
            else:
                logging.error(f"Impossible to find the backend with name {backend_name}, declared for the RDF Graph {g_name}")
                continue
            # build the graph and register it
            graphs[g_name] = Graph(g_name, g_description, g_connector, quantum=g_quantum, max_results=g_max_results, default_queries=g_queries)
            logging.info("RDF Graph '{}' (backend: {}) successfully loaded".format(g_name, backend_name))

    return Dataset(dataset_name, dataset_description, graphs, public_url=public_url, default_query=default_query, analytics=analytics)
