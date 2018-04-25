# hdt_server_connector.py
# Author: Thomas MINIER - MIT License 2017-2018
from database.db_connector import DatabaseConnector
from database.db_iterator import DBIterator
from database.utils import ArrayTripleIterator
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from rdflib import Graph
from requests import Session
from math import ceil, modf


class HDTServerIterator(DBIterator):
    """A HDTServerIterator iterates over a remote collection of RDF triples, hosted by a HDT-Server"""
    def __init__(self, pattern, session, parsedUrl, qParams, startPage=1, limit=0, offset=0):
        super(HDTServerIterator, self).__init__(pattern, limit, offset)
        self._session = session
        self._parsed_url = parsedUrl
        self._qParams = qParams
        self._nextPage = startPage + 1
        self._bufferedtriples, self._isDone = self.__fetchPage(startPage, offset)
        self._nbReads = 0

    def __fetchPage(self, page, offset=0):
        g = Graph()
        self._qParams["page"] = page
        tripleURL = urlunparse((self._parsed_url.scheme, self._parsed_url.netloc, "/triple",
                               self._parsed_url.params, urlencode(self._qParams), self._parsed_url.fragment))
        tripleRequest = self._session.get(tripleURL)
        content = tripleRequest.content
        if len(content) == 0:
            return [], True
        # WARNING: http://hdt.lod.labs.vu.nl returns invalid URI, like <AO-00023>, which breaks rdflib parser
        g.parse(data=content, format="nt")
        triples = map(lambda t: (t[0].n3(), t[1].n3(), t[2].n3()), g.triples((None, None, None)))
        return sorted(list(triples))[offset:], False

    def has_next(self):
        return not self._isDone

    def next(self):
        if self.limit > 0 and self._nbReads >= self.limit:
            raise StopIteration()
        elif len(self._bufferedtriples) > 0:
            self._nbReads += 1
            return self._bufferedtriples.pop(0)
        elif self._isDone:
            raise StopIteration()
        self._bufferedtriples, self._isDone = self.__fetchPage(self._nextPage)
        self._nextPage += 1
        return self.next()


class HDTServerConnector(DatabaseConnector):
    """A HDTServerConnector evaluates triple patterns using a remote HDT server
    HDT-server: https://github.com/MaestroGraph/HDT-Server
    Example live server: http://hdt.lod.labs.vu.nl with 2 graphs (Wikidata and LOD-a-lot)
    """

    def __init__(self, url, graph, pageSize=100):
        super(HDTServerConnector, self).__init__()
        # use a Session for HTTP polling
        self._session = Session()
        self._session.headers.update({
            'user-agent': 'SaGe query engine/HDTServerConnector/1.0.0',
            'accept-encoding': 'gzip, deflate',
            'accept': 'application/n-triples'
        })
        self._url = url
        self._parsed_url = urlparse(url)
        self._graph = graph
        self._pageSize = pageSize
        self._baseQueryParams = dict(graph=self._graph, page_size=self._pageSize)

    def search_triples(self, subject, predicate, obj, limit=0, offset=0):
        page = ceil(offset / self._pageSize) if offset > 0 else 1
        offset = offset - ((page - 1) * self._pageSize) if offset > 0 and modf(offset / self._pageSize) != 0 else 0
        # build query params (subject, predicate, object, graph and page)
        queryParams = dict(page=page)
        if subject is not None:
            queryParams["subject"] = "<{}>".format(subject)
        if predicate is not None:
            queryParams["predicate"] = "<{}>".format(predicate)
        if obj is not None:
            queryParams["object"] = obj if obj.startswith('"') else "<{}>".format(obj)
        queryParams.update(self._baseQueryParams)
        # build URLs used to fetch cardinality
        countURL = urlunparse((self._parsed_url.scheme, self._parsed_url.netloc, "/triple/count",
                              self._parsed_url.params, urlencode(queryParams), self._parsed_url.fragment))
        countRequest = self._session.get(countURL, headers={'accept': 'application/json'})
        cardinality = int(countRequest.content)
        pattern = {"subject": subject, "predicate": predicate, "object": obj}
        it = HDTServerIterator(pattern, self._session, self._parsed_url, queryParams, startPage=page, limit=limit, offset=offset)
        return (it, cardinality)

    def from_config(config):
        """Build a HDTServerConnector from a config file"""
        pageSize = config['pageSize'] if 'pageSize' in config else 100
        return HDTServerConnector(config['url'], config['graph'], pageSize=pageSize)
