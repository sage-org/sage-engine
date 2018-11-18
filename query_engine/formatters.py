# formatters.py
# Author: Thomas MINIER - MIT License 2017-2018
from xml.etree import ElementTree


def get_binding_type(value):
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


def binding_to_json(binding):
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


def sparql_xml(bindings_list):
    """Formats a set of bindings into SPARQL results in JSON formats."""
    def convert_binding(b, root):
        result_node = ElementTree.SubElement(root, "result")
        for variable, value in b.items():
            v_name = variable[1:]
            b_node = ElementTree.SubElement(result_node, "binding", name=v_name)
            value, type, extra_label, extra_value = get_binding_type(value.strip())
            if type == "uri":
                uri_node = ElementTree.SubElement(b_node, "uri")
                uri_node.text = value
            elif type == "literal":
                literal_node = literal_node = ElementTree.SubElement(b_node, "literal")
                literal_node.text = value
                if extra_label is not None:
                    literal_node.set(extra_label, extra_value)
        return result_node

    vars = list(map(lambda x: x[1:], bindings_list[0].keys()))
    root = ElementTree.Element("sparql", xmlns="http://www.w3.org/2005/sparql-results#")
    # build head
    head = ElementTree.SubElement(root, "head")
    for variable in vars:
        ElementTree.SubElement(head, "variable", name=variable)
    # build results
    results = ElementTree.SubElement(root, "results")
    for binding in bindings_list:
        convert_binding(binding, results)
    return root
