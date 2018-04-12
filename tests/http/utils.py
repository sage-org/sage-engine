# utils.py
# Author: Thomas MINIER - MIT License 2017-2018
from json import dumps, loads


def jsonPost(app, url, data):
    res = app.post(url, data=dumps(data), content_type='application/json')
    return loads(res.data)
