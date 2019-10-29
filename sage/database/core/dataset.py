# dataset.py
# Author: Thomas MINIER - MIT License 2017-2019
# from sage.database.core.yaml_config import load_config

class Dataset(object):
    """A collection of RDF graphs"""

    def __init__(self, name, description, graphs, public_url=None, default_query=None, analytics=None):
        super(Dataset, self).__init__()
        self._name = name
        self._desciption = description
        self._graphs = graphs
        self._public_url = public_url
        self._default_query = default_query
        self._analytics = analytics

    @property
    def name(self):
        return self._name

    @property
    def default_query(self):
        default = {
            "name": "",
            "value": ""
        }
        return self._default_query if self._default_query is not None else default

    @property
    def long_description(self):
        return self._desciption

    @property
    def public_url(self):
        return self._public_url

    @property
    def analytics(self):
        return self._analytics

    @property
    def maintainer(self):
        # DEPRECATED
        return None

    def describe(self, url):
        """Gives a generator over dataset descriptions"""
        for name, dataset in self._graphs.items():
            yield dataset.describe(url)

    def get_graph(self, graph_uri):
        """Get a RDF graph given its URI, otherwise returns None"""
        return self._graphs[graph_uri] if graph_uri in self._graphs else None

    def has_graph(self, graph_uri):
        """Test if a RDF graph exists in the RDF dataset"""
        return graph_uri in self._graphs
