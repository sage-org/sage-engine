# responses.py
# Author: Thomas MINIER - MIT License 2017-2018
from sage.http_server.protobuf.sage_response_pb2 import Binding, BindingBag, SageStatistics, SageResponse
from sage.query_engine.formatters import sparql_xml, binding_to_json
from json import dumps
from xml.etree import ElementTree


def stream_json_list(iterator):
    """A generator for streaming a list of JSON results in Flask API"""
    try:
        # get first result
        prev_binding = next(iterator)
        for b in iterator:
            yield dumps(prev_binding, separators=(',', ':')) + ','
            prev_binding = b
        # Now yield the last iteration without comma but with the closing brackets
        yield dumps(prev_binding, separators=(',', ':'))
    except StopIteration:
        # StopIteration here means the length was zero, so yield a valid releases doc and stop
        pass


def protobuf(bindings, next_page, stats):
    sageResponse = SageResponse()
    sageResponse.hasNext = next_page is not None
    if next_page is not None:
        sageResponse.next = next_page
    # register bindings
    for binding in bindings:
        bag = BindingBag()
        for k, v in binding:
            b = Binding()
            b.variable = v
            b.value = v
            bag.bindings.append(b)
        sageResponse.bindings.append(bag)
    # register statistics
    stats = SageStatistics()
    stats.importTime = stats["import"]
    stats.exportTime = stats["export"]
    sageResponse.stats.CopyFrom(stats)
    return sageResponse.SerializeToString()


def skolemize_one(value, url):
    return "{}/bnode#{}".format(url, value[2:]) if value.startswith("_:") else value


def skolemize(bindings, url):
    """Skolemize blank nodes"""
    for b in bindings:
        r = dict()
        for key, value in b.items():
            r[key] = skolemize_one(value, url)
        yield r


def ntriples_streaming(triples):
    for s, p, o in triples:
        subj = "<{}>".format(s) if not s.startswith("\"") else s
        pred = "<{}>".format(p)
        obj = "<{}>".format(o) if not o.startswith("\"") else o
        yield "{} {} {} .\n".format(subj, pred, obj)


def w3c_json_streaming(bindings, next_link, stats, url):
    """Creates a page of SaGe results in the W3C SPARQL JSON results format, compatible with Flask streaming API"""
    hasNext = "true" if next_link is not None else "false"
    vars = list(map(lambda x: x[1:], bindings[0].keys()))
    # generate headers
    yield "{\"head\":{\"vars\":["
    yield ",".join(map(lambda x: "\"{}\"".format(x), vars))
    yield "],\"pageSize\":{},\"hasNext\":{},".format(len(bindings), hasNext)
    if next_link is not None:
        yield "\"next\":\"{}\",".format(next_link)
    yield "\"stats\":" + dumps(stats, separators=(',', ':')) + "},\"results\":{\"bindings\":["
    # generate results
    b_iter = map(binding_to_json, skolemize(bindings, url))
    yield from stream_json_list(b_iter)
    yield "]}}"


def raw_json_streaming(bindings, next_link, stats, url):
    """Creates a page of SaGe results in a simple, non-standard JSON format, compatible with Flask streaming API"""
    hasNext = "true" if next_link is not None else "false"
    yield "{\"bindings\":["
    b_iter = skolemize(bindings, url)
    yield from stream_json_list(b_iter)
    yield "],\"pageSize\":{},\"hasNext\":{},".format(len(bindings), hasNext)
    if next_link is not None:
        yield "\"next\":\"{}\",".format(next_link)
    else:
        yield "\"next\":null,"
    yield "\"stats\":" + dumps(stats, separators=(',', ':')) + "}"


def w3c_xml(bindings, next_link, stats):
    page = sparql_xml(bindings)
    head = page.find("head")
    controls = ElementTree.SubElement(head, "controls")
    hasNext_node = ElementTree.SubElement(controls, "hasNext")
    hasNext_node.text = str(next_link is not None)
    next_node = ElementTree.SubElement(controls, "next")
    next_node.text = next_link
    # TODO include stats
    return ElementTree.tostring(page, encoding="utf-8").decode("utf-8")
