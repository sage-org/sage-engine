# grpc_client.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Iterable, Optional

import grpc

from sage.grpc import service_pb2_grpc
from sage.grpc.service_pb2 import SageQuery


class SageClient(object):
  """A SaGe client allows to execute SPARQL queries using a SaGe server deployed with gRPC.

    Args:
      * url: URL of the SaGe gRPC server.
      * options: An optional list of key-value pairs (channel args in gRPC Core runtime) to configure the channel.
      * credentials: A, optional ChannelCredentials instance. If set, the client will use a secure_channel for communicating with the server. Otherwise, it will use an insecure_channel.
      * compression: An optional value indicating the compression method to be used over the lifetime of the channel. This is an EXPERIMENTAL option.
    
    Example:
      >>> with SageClient("localhost:8000") as client:
      >>>   sparql_query = "SELECT * WHERE { ?s ?p ?o }"
      >>>   for bindings in client.query(sparql_query, "http://example.org#DBpedia")
      >>>     print(bindings)
  """
  def __init__(self, url: str, options: Optional[Dict[str, str]] = None, credentials: Optional[grpc.ChannelCredentials] = None, compression: Optional[str] = None):
    super(SageClient).__init__()
    self._url = url
    self._credentials = credentials
    if self._credentials is None:
      self._channel = grpc.insecure_channel(self._url, options=options, compression=compression)
    else:
      self._channel = grpc.secure_channel(self._url, self._credentials, options=options, compression=compression)
  
  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    self.close()
  
  def __del__(self):
    self.close()
  
  def close(self) -> None:
    self._channel.close()
  
  def query(self, sparql_query: str, default_graph_uri: str) -> Iterable[Dict[str, str]]:
    """
      Execute a SPARQL query using a SaGe gRPC-server.

      Args:
        * sparql_query: SPARQL query to execute.
        * default_graph_uri: URI of the default RDF Graph to query.

      Yields:
        Set of solution mappings

      Example:
        >>> sparql_query = "SELECT * WHERE { ?s ?p ?o }"
        >>> for bindings in client.query(sparql_query, "http://example.org#DBpedia")
        >>>   print(bindings)
    """
    client = service_pb2_grpc.SageSPARQLStub(self._channel)
    grpc_query = SageQuery(query = query, default_graph_uri = default_graph_uri)
    is_done = False
    next_link = None
    while not is_done:
      response = client.Query(grpc_query)
      # prepare next query
      is_done = response.is_done
      next_link = response.next_link
      grpc_query = SageQuery(query = query, default_graph_uri = default_graph_uri, next_link = next_link)
      # yield solution mappings in dict format
      for binding in response.bindings:
        results = dict()
        for mu in binding.values:
          results[mu.variable] = mu.value
        yield results
      
