# utils.py
# Author: Thomas MINIER - MIT License 2017-2018
from werkzeug.datastructures import Headers


def jsonSparql(client, query, next_link, graph_uri):
    headers = Headers()
    headers.add('Accept', 'application/json')
    data = {
        "query": query.strip(),
        "defaultGraph": graph_uri
    }
    if next_link is not None:
        data["next"] = next_link
    res = client.post('/sparql', json=data, headers=headers)
    return res.get_json()
