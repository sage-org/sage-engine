# utils.py
# Author: Thomas MINIER - MIT License 2017-2020
from sage.http_server.server import SagePostQuery


def post_sparql(client, query, next_link, graph_uri):
    """Execute a POST SPARQL query using FastAPI TestClient"""
    headers = {
        "Accept": "application/json"
    }
    query = SagePostQuery(
        query = query.strip(),
        defaultGraph = graph_uri,
        next = next_link
    )
    res = client.post('/sparql', json=query.dict(), headers=headers)
    return res
