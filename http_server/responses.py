# responses.py
# Author: Thomas MINIER - MIT License 2017-2018
from http_server.protobuf.sage_response_pb2 import Binding, BindingBag, SageStatistics, SageResponse
from query_engine.formatters import sparql_json, sparql_xml
from json import dumps
from xml.etree import ElementTree


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


def deskolemize(bindings, url):
    """Deskolemize blank nodes"""
    for b in bindings:
        r = dict()
        for key, value in b.items():
            r[key] = "{}/bnode#{}".format(url, value[2:]) if value.startswith("_:") else value
        yield r


def w3c_json(bindings, next_link, stats):
    page = sparql_json(bindings)
    page["head"]["controls"] = dict()
    page["head"]["controls"]["hasNext"] = next_link is not None
    page["head"]["controls"]["next"] = next_link
    page["head"]["stats"] = stats
    return page


def raw_json(bindings, next_link, stats, url):
    res = {
        "bindings": list(deskolemize(bindings, url)),
        "pageSize": len(bindings),
        "hasNext": next_link is not None,
        "next": next_link,
        "stats": stats
    }
    return res


def raw_json_streaming(bindings, next_link, stats, url):
    """Return Sage results in a simple, non-standard JSON format, using Flask streaming API"""
    hasNext = "true" if next_link is not None else "false"
    yield "{\"bindings\":["
    b_iter = deskolemize(bindings, url)
    try:
        # get first result
        prev_binding = next(b_iter)
        for b in b_iter:
            yield dumps(prev_binding, separators=(',', ':')) + ','
            prev_binding = b
        # Now yield the last iteration without comma but with the closing brackets
        yield dumps(prev_binding, separators=(',', ':'))
    except StopIteration:
        # the case where there is no bindings
        pass
    finally:
        # StopIteration here means the length was zero, so yield a valid releases doc and stop
        yield "],\"pageSize\":{},\"hasNext\":{},\"next\":\"{}\",".format(len(bindings), hasNext, next_link)
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
