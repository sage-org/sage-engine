# utils.py
# Author: Thomas MINIER - MIT License 2017-2018


def pyDict_to_protoDict(source, target):
    """Copy a python dict into a Protobuf map<K,V>"""
    for key in source:
        target[key] = source[key]


def protoTriple_to_dict(triple):
    return {
        'subject': triple.subject,
        'predicate': triple.predicate,
        'object': triple.object,
        'graph': triple.graph
    }
