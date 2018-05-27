# responses.py
# Author: Thomas MINIER - MIT License 2017-2018
from http_server.protobuf.sage_response_pb2 import Binding, BindingBag, SageStatistics, SageResponse


def protobuf(bindings, nextPage, stats):
    sageResponse = SageResponse()
    sageResponse.hasNext = nextPage is not None
    if nextPage is not None:
        sageResponse.next = nextPage
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


def json(bindings, pageSize, nextLink, stats):
    res = {
        "bindings": bindings,
        "pageSize": pageSize,
        "hasNext": nextLink is not None,
        "next": nextLink,
        "stats": stats
    }
    return res
