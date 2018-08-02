# responses.py
# Author: Thomas MINIER - MIT License 2017-2018
from http_server.protobuf.sage_response_pb2 import Binding, BindingBag, SageStatistics, SageResponse
from query_engine.formatters import sparql_json, sparql_xml
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


def json(bindings, next_link, stats):
    page = sparql_json(bindings)
    page["head"]["controls"] = dict()
    page["head"]["controls"]["hasNext"] = next_link is not None
    page["head"]["controls"]["next"] = next_link
    page["head"]["stats"] = stats
    return page


def raw_json(bindings, next_link, stats):
    res = {
        "bindings": bindings,
        "pageSize": len(bindings),
        "hasNext": next_link is not None,
        "next": next_link,
        "stats": stats
    }
    return res


def xml(bindings, next_link, stats):
    page = sparql_xml(bindings)
    head = page.find("head")
    controls = ElementTree.SubElement(head, "controls")
    hasNext_node = ElementTree.SubElement(controls, "hasNext")
    hasNext_node.text = str(next_link is not None)
    next_node = ElementTree.SubElement(controls, "next")
    next_node.text = next_link
    # TODO include stats
    return ElementTree.tostring(page, encoding="utf-8").decode("utf-8")
