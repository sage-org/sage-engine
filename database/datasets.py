# datasets.py
# Author: Thomas MINIER - MIT License 2017-2018
from yaml import load
from database.rdf_file_connector import RDFFileConnector
from database.hdt_file_connector import HDTFileConnector
from math import inf

DB_CONNECTORS = {
    'rdf-file': RDFFileConnector,
    'hdt-file': HDTFileConnector
}


class Dataset(object):
    """A RDF Dataset with a dedicated backend"""

    def __init__(self, config):
        super(Dataset, self).__init__()
        self._config = config
        connectorClass = DB_CONNECTORS[self._config['backend']] if self._config['backend'] in DB_CONNECTORS else None
        self._connector = connectorClass.from_config(self._config)

    def config(self):
        return self._config

    @property
    def quota(self):
        return self._config['quota']

    @property
    def maxResults(self):
        return self._config['maxResults']

    @property
    def nb_triples(self):
        return self._connector.nb_triples

    @property
    def example_queries(self):
        return self._config['queries']

    def connector(self):
        """Get the underlying DatabaseConnector for this dataset"""
        return self._connector

    def search_triples(self, subject, predicate, obj, limit=0, offset=0):
        """
            Get an iterator over all RDF triples matching a triple pattern.
            Args:
                - subject [string] - Subject of the triple pattern
                - predicate [string] - Preicate of the triple pattern
                - object [string] - Object of the triple pattern
                - limit [int=0] - (Optional) LIMIT modifier, i.e., maximum number of RDF triples to read
                - offset [int=0] - (Optional) OFFSET modifier, i.e., number of RDF triples to skip
            Returns:
                A Python iterator over RDF triples matching the given triples pattern
        """
        return self._connector.search_triples(subject, predicate, obj, limit, offset)

    def describe(self, url):
        """Describe the Dataset API using the Hydra spec"""
        description = {
            "@type": "Class",
            "@id": "{}/{}".format(url, self._config["name"]),
            "title": self._config["name"],
            "description": self._config["description"],
            "stats": {
                "size": self.nb_triples,
                "nb_subjects": self._connector.nb_subjects if self._connector.nb_subjects is not None else 'unknown',
                "nb_predicates": self._connector.nb_predicates if self._connector.nb_predicates is not None else 'unknown',
                "nb_objects": self._connector.nb_objects if self._connector.nb_objects is not None else 'unknown'
            },
            "timeQuota": self.quota,
            "maxResults": self.maxResults if self.maxResults is not inf else 'inf',
            "examples": self.example_queries,
            "supportedOperation": [
                {
                    "@type": "Operation",
                    "title": "Evaluate a SPARQL query",
                    "method": "POST"
                }
            ],
            "supportedProperty": [
                {
                    "@type": "SupportedProperty",
                    "property": "#query",
                    "required": True,
                    "readable": True,
                    "writable": True
                },
                {
                    "@type": "SupportedProperty",
                    "property": "#next",
                    "required": False,
                    "readable": True,
                    "writable": True
                }
            ]
        }
        return description


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
    # set page size, i.e. the number of triples per page
    quota = config['quota'] if 'quota' in config else 75
    maxResults = config['maxResults'] if 'maxResults' in config else inf
    config['quota'] = quota
    for c in config["datasets"]:
        if 'quota' not in c:
            c['quota'] = quota
        if 'maxResults' not in c:
            c['maxResults'] = maxResults
        if 'queries' not in c:
            c['queries'] = []
    # build datasets
    datasets = {c["name"]: Dataset(c) for c in config["datasets"]}
    return (config, datasets)


class DatasetCollection(object):
    """A collection of RDF datasets, served as a Singleton"""

    def __init__(self, config_file):
        super(DatasetCollection, self).__init__()
        self._config_file = config_file
        (self._config, self._datasets) = load_config(self._config_file)
        if "longDescription" in self._config:
            with open(self._config["longDescription"], "r") as file:
                self._long_description = file.read()
        else:
            self._long_description = ""

    @property
    def name(self):
        return self._config["name"] if "name" in self._config else None

    @property
    def long_description(self):
        return self._long_description

    @property
    def public_url(self):
        return self._config["publicUrl"] if "publicUrl" in self._config else None

    @property
    def maintainer(self):
        return self._config["maintainer"] if "maintainer" in self._config else None

    def describe(self, url):
        """Gives a generator over dataset descriptions"""
        for name, dataset in self._datasets.items():
            yield dataset.describe(url)

    def get_dataset(self, dataset_name):
        """Get a dataset given its name"""
        dataset = self._datasets[dataset_name] if dataset_name in self._datasets else None
        return dataset
