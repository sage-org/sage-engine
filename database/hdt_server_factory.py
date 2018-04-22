# fragment_factory.py
# Author: Thomas MINIER - MIT License 2017-2018
from database.fragment_factory import FragmentFactory
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from rdflib import Graph
from requests import Session


class HDTServerFactory(FragmentFactory):
    """A HDTServerFactory builds LDF fragments from a remote HDT server.
    HDT-server: https://github.com/MaestroGraph/HDT-Server
    Example live server: http://hdt.lod.labs.vu.nl with 2 graphs (Wikidata and LOD-a-lot)
    """

    def __init__(self, url, graph, triplesPerPage=100):
        # use a Session for HTTP polling
        self._session = Session()
        self._session.headers.update({
            'user-agent': 'YALDF (Yet Another LDF Server)/1.0.0',
            'accept_encoding': 'gzip, deflate',
            'accept': 'application/n-triples'
        })
        self._url = url
        self._parsed_url = urlparse(url)
        self._graph = graph
        self._triplesPerPage = triplesPerPage
        self._baseQueryParams = dict(graph=self._graph, page_size=self._triplesPerPage)

    def get_triples(self, subject, predicate, obj, page=1):
        graph = Graph()
        # build query params (subject, predicate, object, graph and page)
        queryParams = dict(page=page)
        if subject is not None:
            queryParams["subject"] = "<%s>" % subject
        if predicate is not None:
            queryParams["predicate"] = "<%s>" % predicate
        if obj is not None:
            queryParams["object"] = "%s" % obj if obj.startswith('"') else "<%s>" % obj
        queryParams.update(self._baseQueryParams)
        # build URLs used to fetch triples and cardinality
        tripleURL = urlunparse((self._parsed_url.scheme, self._parsed_url.netloc, "/triple",
                               self._parsed_url.params, urlencode(queryParams), self._parsed_url.fragment))
        countURL = urlunparse((self._parsed_url.scheme, self._parsed_url.netloc, "/triple/count",
                              self._parsed_url.params, urlencode(queryParams), self._parsed_url.fragment))
        # make HTTP requests to fetch triples and cardinality
        # TODO add safeguard when a request fails
        tripleRequest = self._session.get(tripleURL)
        countRequest = self._session.get(countURL, headers={'accept': 'application/json'})
        # load data fetched throught HTTP requests
        cardinality = int(countRequest.content)
        # WARNING: http://hdt.lod.labs.vu.nl returns invalid URI, like <AO-00023>, which breaks rdflib parser
        graph.parse(data=tripleRequest.content, format="nt")
        return (graph, cardinality)

    def from_config(config):
        """Build a HDTServerFactory from a config file"""
        # TODO add safeguard
        return HDTServerFactory(config['url'], config['graph'], config['pageSize'])
