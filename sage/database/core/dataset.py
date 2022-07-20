from __future__ import annotations

import yaml
import logging

from os import environ
from math import inf
from uuid import uuid4
from typing import Dict, Iterable, Optional

from sage.singleton import Singleton
from sage.database.core.graph import Graph
from sage.database.backends.import_manager import builtin_backends


class Dataset(metaclass=Singleton):
    """
    A collection of RDF graphs built from the YAML configuration file.
    """

    def __init__(self) -> None:
        super(Dataset, self).__init__()

        if "SAGE_CONFIG_FILE" not in environ:
            raise Exception("No YAML configuration file provided...")
        config = yaml.safe_load(open(environ["SAGE_CONFIG_FILE"]))

        self._name = config["name"]
        self._public_url = config.setdefault("public_url", None)
        self._default_query = config.setdefault("default_query", None)
        self._stateless = config.setdefault("stateless", True)

        self._desciption = "A RDF dataset hosted by a SaGe server"
        if "long_description" in config:
            with open(config["long_description"], "r") as file:
                self._desciption = file.read()

        backends = builtin_backends()

        if "quota" not in config:
            self._quantum = inf
            logging.warning(
                "You are using SaGe with an infinite time quantum. "
                "Be sure to configure the Worker timeout of Gunicorn accordingly, "
                "otherwise long-running queries might be terminated.")
        else:
            self._quantum = config["quota"]

        if "max_results" not in config:
            self._max_results = inf
            logging.warning(
                "You are using SaGe without limitations on the number of results "
                "sent per page. This is fine, but be carefull as very large page "
                "of results can have unexpected serialization time.")
        else:
            self._max_results = config["max_results"]

        if "max_limit" not in config:
            self._max_limit = inf
            logging.warning(
                "You are using SaGe without restrictions on the limit K for "
                "SPARQL TOP-K queries. This is fine, but be carefull as a very "
                "large K can drastically increase the time to suspend and "
                "resume a TOPKIterator.")
        else:
            self._max_limit = config["max_limit"]

        self._graphs = dict()
        if "graphs" not in config:
            raise SyntaxError(
                "No RDF graphs found in the configuration file. "
                "Please refers to the documentation to see how to declare RDF "
                "graphs in a SaGe YAML configuration file.")
        for graph in config["graphs"]:
            if "uri" not in graph:
                raise SyntaxError(f"The RDF Graph {graph} has no declared URI!")
            g_uri = graph["uri"]
            g_name = graph.setdefault("name", str(uuid4()))
            g_description = graph.setdefault("description", f"RDF Graph {g_name}")
            g_queries = graph.setdefault("queries", [])

            if "backend" in graph and graph["backend"] in backends:
                g_connector = backends[graph["backend"]](graph)
            else:
                logging.error(
                    f"Impossible to find the backend {graph['backend']}, "
                    f"declared for the RDF Graph {g_name}")
                continue

            self._graphs[g_uri] = Graph(
                g_uri, g_name, g_description, g_connector,
                default_queries=g_queries)

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_stateless(self) -> bool:
        return self._stateless

    @property
    def default_query(self):
        default = {"name": "", "value": ""}
        return self._default_query if self._default_query is not None else default

    @property
    def long_description(self) -> str:
        return self._desciption

    @property
    def public_url(self) -> str:
        return self._public_url

    @property
    def quota(self) -> int:
        return self._quantum

    @property
    def max_results(self) -> int:
        return self._max_results

    @property
    def max_limit(self) -> int:
        return self._max_limit

    def describe(self, url: str) -> Iterable[Dict[str, str]]:
        """
        Get a generator over dataset descriptions.

        Parameters
        ----------
        url: str
            Public URL of the dataset.

        Yields
        ------
          Dataset descriptions as dictionnaries.
        """
        for name, graph in self._graphs.items():
            yield graph.describe(url)

    def get_graph(self, graph_uri: str) -> Optional[Graph]:
        """
        Get a RDF graph given its URI, otherwise returns None.

        Parameters
        ----------
        graph_uri: str
            URI of the RDF graph to access.

        Returns
        -------
          The RDF Graph associated with the URUI or None if it was not found.
        """
        return self._graphs[graph_uri] if graph_uri in self._graphs else None

    def has_graph(self, graph_uri: str) -> bool:
        """
        Test if a RDF graph exists in the RDF dataset.

        Parameters
        ----------
        graph_uri: str
            URI of the RDF graph to access.

        Returns
        -------
          True if the RDF graph exists in the RDF dataset, False otherwise.
        """
        return graph_uri in self._graphs
