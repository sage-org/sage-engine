# utils.py
# Author: Thomas MINIER - MIT License 2017-2018
from flask import Response, url_for
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from base64 import b64encode, b64decode
from json import dumps
from xml.etree import ElementTree


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
    qparams = parse_qs(query)
    query = urlencode(qparams, doseq=True)
    return urlunparse((scheme, netloc, path, params, query, fragment)).replace("%7E", "~")


def format_graph_uri(uri, server_url):
    """Format a GRAPH IRI if its belong to the same server than the current one"""
    if not server_url.endswith('/'):
        server_url += '/'
    if uri.startswith(server_url):
        index = uri.index(server_url)
        return uri[index + len(server_url):]
    return '_:UnkownGraph'


def format_marshmallow_errors(errors):
    """Format mashmallow validation errors in string format"""
    return dumps(errors, indent=2)


def encode_saved_plan(savedPlan):
    if savedPlan is None:
        return None
    bytes = savedPlan.SerializeToString()
    return b64encode(bytes).decode('utf-8')


def decode_saved_plan(bytes):
    return b64decode(bytes) if bytes is not None else None


def sage_http_error(text, status=400):
    content = """
        <!DOCTYPE html>
        <html lang="en" dir="ltr">
          <head>
            <meta charset="utf-8">
            <title>SaGe server Error</title>
          </head>
          <body>
            <h1>SaGe server error</h1>
            <p>{}</p>
          </body>
        </html>
    """.format(text)
    return Response(content, status=status, content_type="text/html")


def generate_sitemap(dataset, last_mod):
    """Generate a XML sitemap from the datasets & queries hosted on the server"""
    root = ElementTree.Element("urlset", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")
    # add basic navigation links
    home_xml = ElementTree.SubElement(root, "url")
    ElementTree.SubElement(home_xml, "loc").text = url_for('index', _external=True)
    ElementTree.SubElement(home_xml, "lastmod").text = last_mod

    # add server VoID
    gvoid_xml = ElementTree.SubElement(root, "url")
    ElementTree.SubElement(gvoid_xml, "loc").text = url_for('void-interface.void_all', _external=True)
    ElementTree.SubElement(gvoid_xml, "lastmod").text = last_mod

    # add dataset and queries
    for graph_name, graph in dataset._datasets.items():
        # add dataset homepage
        graph_xml = ElementTree.SubElement(root, "url")
        ElementTree.SubElement(graph_xml, "loc").text = url_for('sparql-interface.sparql_query', graph_name=graph_name, _external=True)
        ElementTree.SubElement(graph_xml, "lastmod").text = last_mod

        # add dataset VoID
        void_xml = ElementTree.SubElement(root, "url")
        ElementTree.SubElement(void_xml, "loc").text = url_for('void-interface.void_dataset', graph_name=graph_name, _external=True)
        ElementTree.SubElement(void_xml, "lastmod").text = last_mod

        # add dataset queries
        for query in graph.example_queries:
            if query["publish"]:
                query_xml = ElementTree.SubElement(root, "url")
                # location
                ElementTree.SubElement(query_xml, "loc").text = url_for('publish-query-interface.publish_query', graph_name=graph_name, query_name=query["@id"], _external=True)
                # last_mod
                ElementTree.SubElement(query_xml, "lastmod").text = last_mod
    return ElementTree.tostring(root, encoding="utf8", method="xml").decode("utf-8")
