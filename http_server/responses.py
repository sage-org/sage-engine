# responses.py
# Author: Thomas MINIER - MIT License 2017-2018
from http_server.protobuf.sage_response_pb2 import Binding, BindingBag, SageStatistics, SageResponse


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


def json(bindings, page_size, next_link, stats):
    res = {
        "bindings": bindings,
        "pageSize": page_size,
        "hasNext": next_link is not None,
        "next": next_link,
        "stats": stats
    }
    return res
