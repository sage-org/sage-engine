# datasets.py
# Author: Thomas MINIER - MIT License 2017-2018
from yaml import load
# from datasets.raw_file_factory import RawFileFactory
from datasets.hdt_file_factory import HDTFileFactory
# from datasets.hdt_server_factory import HDTServerFactory

factories = {
    # 'rdf-file': RawFileFactory,
    # 'hdt-server': HDTServerFactory,
    'hdt-file': HDTFileFactory
}


class Dataset(object):
    """A RDF Dataset with a dedicated backend"""
    def __init__(self, config):
        super(Dataset, self).__init__()
        self._config = config
        factory = factories[self._config['backend']] if self._config['backend'] in factories else None
        self._factory = factory.from_config(self._config)

    def config(self):
        return self._config

    def page_size(self):
        return self._config["pageSize"]

    def deadline(self):
        return self._config['deadline']

    def factory(self):
        return self._factory

    def search_triples(self, subject, predicate, obj, limit=0, offset=0):
        return self._factory.search_triples(subject, predicate, obj, limit, offset)


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
    page_size = config['pageSize'] if 'pageSize' in config else 100
    deadline = config['quota'] if 'quota' in config else 20
    config['pageSize'] = page_size
    config['deadline'] = deadline
    for c in config["datasets"]:
        if 'pageSize' not in c:
            c["pageSize"] = page_size
        if 'deadline' not in c:
            c['deadline'] = deadline
    # build datasets
    datasets = {c["name"]: Dataset(c) for c in config["datasets"]}
    return (config, datasets)


class DatasetCollection(object):
    """A collection of RDF datasets, served as a Singleton"""
    def __init__(self, config_file):
        super(DatasetCollection, self).__init__()
        self._config_file = config_file
        (self._config, self._datasets) = load_config(self._config_file)

    def get_dataset(self, dataset_name):
        """Get a dataset given its name"""
        dataset = self._datasets[dataset_name] if dataset_name in self._datasets else None
        return dataset
