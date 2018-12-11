# utils.py
# Author: Thomas MINIER - MIT License 2017-2018
from json import dumps, loads
from werkzeug.datastructures import Headers


def jsonSparql(app, query, next_link, graph_uri):
    headers = Headers()
    headers.add('Accept', 'application/json')
    data = {
        "query": query,
        "defaultGraph": graph_uri
    }
    if next_link is not None:
        data["next"] = next_link
    res = app.post('/sparql', data=dumps(data), content_type='application/json', headers=headers)
    return loads(res.data)
