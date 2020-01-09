# yaml_config.py
# Author: Thomas MINIER - MIT License 2017-2020
import logging
from math import inf
from uuid import uuid4

from yaml import FullLoader, load

from sage.database.core.dataset import Dataset
from sage.database.core.graph import Graph
from sage.database.import_manager import builtin_backends, import_backend
from sage.database.statefull.hashmap_manager import HashMapManager


def load_config(config_file: str) -> Dataset:
    """Parse a SaGe configuration file written in YAML and load the corresponding RDF dataset.

    Args:
      * config_file: Path to the SaGe configuration file (in YAML format) to load.
    
    Returns:
      A RDF dataset built according to the input configuration file.
    """
    config = load(open(config_file), Loader=FullLoader)

    # available backends (populated with sage's native backends)
    backends = builtin_backends()
    # build custom backend (if there is some)
    if 'backends' in config and len(config['backends']) > 0:
        for b in config['backends']:
            if 'name' not in b or 'path' not in b or 'connector' not in b or 'required' not in b:
                raise SyntaxError('Invalid backend declared. Each custom backend must be declared with properties "name", "path", "connector" and "required"')
            backends[b['name']] = import_backend(b['name'], b['path'], b['connector'], b['required'])

    # load dataset basic informations
    dataset_name = config["name"]
    public_url = config["public_url"] if "public_url" in config else None
    default_query = config["default_query"] if "default_query" in config else None
    analytics = config["google_analytics"] if "google_analytics" in config else None
    if "long_description" in config:
        with open(config["long_description"], "r") as file:
            dataset_description = file.read()
    else:
        dataset_description = "A RDF dataset hosted by a SaGe server"

    # load the mode of the server: stateless or statefull
    if 'stateless' in config:
        is_stateless = config['stateless']
    else:
        is_stateless = True

    # if statefull, load the saved plan storage backend to use
    statefull_manager = None
    if not is_stateless:
        # TODO allow use of custom backend for saved plans
        # same kind of usage than custom DB backends
        statefull_manager = HashMapManager()

    # get default time quantum & maximum number of results per page
    if 'quota' in config:
        if config['quota'] == 'inf':
            logging.warning("You are using SaGe with an infinite time quantum. Be sure to configure the Worker timeout of Gunicorn accordingly, otherwise long-running queries might be terminated.")
            quantum = inf
        else:
            quantum = config['quota']
    else:
        quantum = 75
    if 'max_results' in config and config['max_results'] != 'inf':
        max_results = config['max_results']
    else:
        logging.warning("You are using SaGe without limitations on the number of results sent per page. This is fine, but be carefull as very large page of results can have unexpected serialization time.")
        max_results = inf

    # build all RDF graphs found in the configuration file
    graphs = dict()
    if "graphs" not in config:
        raise SyntaxError("Np RDF graphs found in the configuration file. Please refers to the documentation to see how to declare RDF graphs in a SaGe YAML configuration file.")
    for g_config in config["graphs"]:
        if "uri" not in g_config:
            raise SyntaxError(f"Error: the RDF Graph {g_config} has no URI declared!")
        # load basic information about the graph
        g_uri = g_config["uri"]
        g_name = g_config["name"] if "name" in g_config else str(uuid4())
        g_description = g_config["description"] if "description" in g_config else f"Unnamed RDF graph with id {g_name}"
        g_quantum = g_config["quota"] if "quota" in g_config else quantum
        g_max_results = g_config["max_results"] if "max_results" in g_config else max_results
        g_queries = g_config["queries"] if "queries" in g_config else list()

        # load the graph connector using available backends
        if "backend" in g_config and g_config["backend"] in backends:
            g_connector = backends[g_config["backend"]](g_config)
        else:
            logging.error(f"Impossible to find the backend with name {g_config['backend']}, declared for the RDF Graph {g_name}")
            continue

        # build the graph and register it using its URI
        graphs[g_uri] = Graph(g_uri, g_name, g_description, g_connector, quantum=g_quantum, max_results=g_max_results, default_queries=g_queries)
        logging.info(f"RDF Graph '{g_name}' (backend: {g_config['backend']}) successfully loaded")

    return Dataset(dataset_name, dataset_description, graphs, public_url=public_url, default_query=default_query, analytics=analytics, stateless=is_stateless, statefull_manager=statefull_manager)
