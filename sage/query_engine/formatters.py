# formatters.py
# Author: Thomas MINIER - MIT License 2017-2020
from xml.etree import ElementTree
from typing import List, Dict, Optional, Tuple

def get_binding_type(value: str) -> Tuple[str, str, Optional[str], Optional[str]]:
    # literal case
    if value.startswith("\""):
        extra_label, extra_value = None, None
        if "\"^^<http" in value:
            index = value.rfind("\"^^<http")
            extra_label, extra_value = "datatype", value[index + 4:len(value) - 1]
            value = value[0:index + 1]
        elif "\"^^http" in value:
            index = value.rfind("\"^^http")
            extra_label, extra_value = "datatype", value[index + 3:]
            value = value[0:index + 1]
        elif "\"@" in value:
            index = value.rfind("\"@")
            extra_label, extra_value = "xml:lang", value[index + 2:]
            value = value[0:index + 1]
        return value[1:len(value) - 1], "literal", extra_label, extra_value
    else:
        # as the dataset is blank-node free, all other values are uris
        return value, "uri", None, None


def binding_to_json(binding: Dict[str, str]) -> dict:
    """Format a set of solutions bindings in the W3C SPARQL JSON format"""
    json_binding = dict()
    for variable, value in binding.items():
        variable = variable[1:]
        json_binding[variable] = dict()
        value, type, extra_label, extra_value = get_binding_type(value.strip())
        json_binding[variable]["value"] = value
        json_binding[variable]["type"] = type
        if extra_label is not None:
            json_binding[variable][extra_label] = extra_value
    return json_binding
