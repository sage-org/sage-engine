# datasets.py
# Author: Thomas MINIER - MIT License 2017-2018
from yaml import load
from sage.database.import_manager import import_backend, hdt_backend
from math import inf
from urllib.parse import quote_plus


def load_config(config_file="config.yaml"):
    """Load config file to initialize fragment factories.
    A config file is a Python file, loaded as a module.

    Example config file:
    # config.yaml
    name: My LDF server
    maintainer: chuck Norris <me@gmail.com>

    datasets:
    -
        name: DBpedia-2016-04
        description: DBpedia dataset, version 2016-04
        backend: hdt-file
        file: /home/chuck-norris/dbpedia-2016-04.hdt
    -
        name: Chuck-Norris-facts
        description: Best Chuck Norris facts ever
        backend: rdf-file
        format: nt
        file: /home/chuck-norris/facts.nt
    """
    config = load(open(config_file))
    # available backends (populated with sage's native backends)
    backends = {
        'hdt-file': hdt_backend()
    }
    # build custom backend (if there is some)
    if 'backends' in config and len(config['backends']) > 0:
        for b in config['backends']:
            if 'name' not in b or 'path' not in b or 'connector' not in b or 'required' not in b:
                raise SyntaxError('Invalid backend declared. Each custom backend must be declared with properties "name", "path", "connector" and "required"')
            backends[b['name']] = import_backend(b['name'], b['path'], b['connector'], b['required'])
    # set page size, i.e. the number of triples per page
    quota = config['quota'] if 'quota' in config else 75
    max_results = config['max_results'] if 'max_results' in config else inf
    config['quota'] = quota
    for c in config["datasets"]:
        if 'quota' not in c:
            c['quota'] = quota
        if 'max_results' not in c:
            c['max_results'] = max_results
        if 'publish' not in c:
            c['publish'] = False
        if 'queries' not in c:
            c['queries'] = []
    # build RDF graphs
    graphs = {c["name"]: Graph(c, backends) for c in config["datasets"]}
    return (config, graphs, backends)


class Graph(object):
    """A RDF Graph with a dedicated backend"""

    def __init__(self, config, backends):
        super(Graph, self).__init__()
        self._config = config
        # build database connector
        if self._config['backend'] in backends:
            self._connector = backends[self._config['backend']](self._config)
        else:
            raise SyntaxError('Unknown backend {} encountered'.format(self._config['backend']))
        # format preset queries
        for query in self.example_queries:
            query['@id'] = quote_plus(query['name'])
            if 'description' not in query:
                query['description'] = query['name']
            if 'publish' not in query:
                query['publish'] = False

    def config(self):
        return self._config

    @property
    def quota(self):
        return self._config['quota']

    @property
    def max_results(self):
        return self._config['max_results']

    @property
    def nb_triples(self):
        return self._connector.nb_triples

    @property
    def example_queries(self):
        return self._config['queries']

    def connector(self):
        """Get the underlying DatabaseConnector for this dataset"""
        return self._connector

    def search(self, subject, predicate, obj, last_read=None):
        """
            Get an iterator over all RDF triples matching a triple pattern.
            Args:
                - subject [string] - Subject of the triple pattern
                - predicate [string] - Preicate of the triple pattern
                - object [string] - Object of the triple pattern
                - last_read ``[string=None]`` ``optional`` -  OFFSET ID used to resume scan
            Returns:
                A Python iterator over RDF triples matching the given triples pattern
        """
        return self._connector.search(subject, predicate, obj, last_read)

    def describe(self, url):
        """Describe the RDF Dataset in JSON-LD format"""
        return {
            "@context": {
                "schema": "http://schema.org/",
                "void": "http://rdfs.org/ns/void#",
                'sage': 'http://sage.univ-nantes.fr/sage-voc#'
            },
            "@type": "http://schema.org/Dataset",
            "schema:url": url,
            "schema:name": self._config["name"],
            "schema:description": self._config["description"],
            "void:triples": self.nb_triples,
            "void:distinctSubjects": self._connector.nb_subjects if self._connector.nb_subjects is not None else "unknown",
            "void:properties": self._connector.nb_predicates if self._connector.nb_predicates is not None else "unknown",
            "void:distinctObjects": self._connector.nb_objects if self._connector.nb_objects is not None else "unknown",
            "sage:timeQuota": self.quota,
            "sage:maxResults": self.max_results if self.max_results is not inf else 'inf'
        }

    def get_query(self, q_id):
        """Get an example SPARQL query associated with the graph, or None if it was not found"""
        for query in self.example_queries:
            if query['@id'] == q_id:
                return query
        return None


class Dataset(object):
    """A collection of RDF graphs"""

    def __init__(self, config_file):
        super(Dataset, self).__init__()
        self._config_file = config_file
        (self._config, self._datasets, self._backends) = load_config(self._config_file)
        if "long_description" in self._config:
            with open(self._config["long_description"], "r") as file:
                self._long_description = file.read()
        else:
            self._long_description = ""

    @property
    def name(self):
        return self._config["name"] if "name" in self._config else None

    @property
    def default_query(self):
        default = {
            "name": "",
            "value": ""
        }
        return self._config["default_query"] if "default_query" in self._config else default

    @property
    def long_description(self):
        return self._long_description

    @property
    def public_url(self):
        return self._config["public_url"] if "public_url" in self._config else None

    @property
    def maintainer(self):
        return self._config["maintainer"] if "maintainer" in self._config else None

    def describe(self, url):
        """Gives a generator over dataset descriptions"""
        for name, dataset in self._datasets.items():
            yield dataset.describe(url)

    def get_graph(self, dataset_name):
        """Get a dataset given its name"""
        dataset = self._datasets[dataset_name] if dataset_name in self._datasets else None
        return dataset

    def has_graph(self, dataset_name):
        """Test fi a graph exist"""
        return dataset_name in self._datasets
