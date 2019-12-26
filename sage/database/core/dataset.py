# dataset.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Iterable, Optional

from sage.database.core.graph import Graph
from sage.database.statefull.statefull_manager import StatefullManager


class Dataset(object):
    """A collection of RDF graphs.

    Args:
      * name: Name of the RDF dataset.
      * description: Description of the RDF dataset.
      * graphs: RDF Graphs of the dataset.
      * public_url: (Optional) URL that host the SaGe server
      * default_query: (Optional) A default query that can be executed against this dataset.
      * analytics: Google analytics credentials.
      * stateless: True if the dataset is queried in sateless mode, False if its is queried in statefull mode.
      * statefull_manager: StatefullManager used to store saved plan (required in statefull mode).
    """

    def __init__(self, name: str, description: str, graphs: Dict[str, Graph], public_url: Optional[str] = None, default_query: Optional[str] = None, analytics=None, stateless=True, statefull_manager: Optional[StatefullManager] = None):
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
    def name(self) -> str:
        return self._name

    @property
    def is_stateless(self) -> bool:
        return self._stateless

    @property
    def statefull_manager(self) -> StatefullManager:
        return self._statefull_manager

    @property
    def default_query(self):
        default = {
            "name": "",
            "value": ""
        }
        return self._default_query if self._default_query is not None else default

    @property
    def long_description(self) -> str:
        return self._desciption

    @property
    def public_url(self) -> str:
        return self._public_url

    @property
    def analytics(self):
        return self._analytics

    @property
    def maintainer(self):
        # DEPRECATED
        return None

    def describe(self, url: str) -> Iterable[Dict[str, str]]:
        """Get a generator over dataset descriptions.

        Args:
          * url: Public URL of the dataset.

        Yields:
          Dataset descriptions as dictionnaries.
        """
        for name, graph in self._graphs.items():
            yield graph.describe(url)

    def get_graph(self, graph_uri: str) -> Optional[Graph]:
        """Get a RDF graph given its URI, otherwise returns None.
          
        Args:
          * graph_uri: URI of the RDF graph to access.

        Returns:
          The RDF Graph associated with the URUI or None if it was not found.
        """
        return self._graphs[graph_uri] if graph_uri in self._graphs else None

    def has_graph(self, graph_uri: str) -> bool:
        """Test if a RDF graph exists in the RDF dataset.
        
        Args:
          * graph_uri: URI of the RDF graph to access.

        Returns:
          True if the RDF graph exists in the RDF dataset, False otherwise.
        """
        return graph_uri in self._graphs
