# utils.py
# Author: Thomas MINIER - MIT License 2017-2018
from json import dumps, loads


def jsonPost(app, url, data):
    res = app.post(url, data=dumps(data), content_type='application/json')
    return loads(res.data)


def jsonSparql(app, query, next_link, graph_uri):
    data = {
        "query": query,
        "defaultGraph": graph_uri
    }
    if next_link is not None:
        data["next"] = next_link
    res = app.post('/sparql', data=dumps(data), content_type='application/json')
    return loads(res.data)
