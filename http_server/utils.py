# utils.py
# Author: Thomas MINIER - MIT License 2017-2018
from collections import OrderedDict
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from json import dumps
from time import time


def sort_qparams(v):
    """Sort query params as subject, predicate, object, page, as the current ldf-client require about this particular order..."""
    if v[0] == 'subject':
        return 0
    elif v[0] == 'predicate':
        return 1
    elif v[0] == 'object':
        return 2
    elif v[0] == 'page':
        return 3
    return 4


def secure_url(url):
    """Secure potentially ill formatted urls"""
    (scheme, netloc, path, params, query, fragment) = urlparse(url)
    qparams = OrderedDict(sorted(parse_qs(query).items(), key=sort_qparams))
    query = urlencode(qparams, doseq=True)
    return urlunparse((scheme, netloc, path, params, query, fragment)).replace("%7E", "~")


def format_marshmallow_errors(errors):
    """Format mashmallow validation errors in string format"""
    res = "ERROR 400 Bad Request: Errors when validating query schema"
    return dumps(errors, indent=2)
