# dataset.py
# Author: Thomas MINIER - MIT License 2017-2020


class Dataset(object):
    """A collection of RDF graphs"""

    def __init__(self, name, description, graphs, public_url=None, default_query=None, analytics=None, stateless=True, statefull_manager=None):
        super(Dataset, self).__init__()
        self._name = name
        self._desciption = description
        self._graphs = graphs
        self._public_url = public_url
        self._default_query = default_query
        self._analytics = analytics
        self._stateless = stateless
        self._statefull_manager = statefull_manager
        # open the statefull manager (if needed)
        if (not self._stateless) and self._statefull_manager is not None:
            self._statefull_manager.open()

    @property
    def name(self):
        return self._name

    @property
    def is_stateless(self):
        return self._stateless

    @property
    def statefull_manager(self):
        return self._statefull_manager

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
