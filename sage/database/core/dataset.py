# dataset.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.database.core.yaml_config import load_config

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
