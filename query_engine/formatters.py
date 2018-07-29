# formatters.py
# Author: Thomas MINIER - MIT License 2017-2018


def get_binding_type(value):
    # literal case
    if value.startswith("\""):
        extra_label, extra_value = None, None
        if "\"^^<http" in value:
            index = value.rfind("\"^^<http")
            extra_label, extra_value = "datatype", value[index + 4:len(value) - 1]
        elif "\"^^http" in value:
            index = value.rfind("\"^^http")
            extra_label, extra_value = "datatype", value[index + 3:]
        elif "\"@" in value:
            index = value.rfind("\"@")
            extra_label, extra_value = "xml:lang", value[index + 2:]
        return "literal", extra_label, extra_value
    else:
        # as the dataset is blank-node free, all other values are uris
        return "uri", None, None


def sparql_json(bindings_list):
    """Formats a set of bindings into SPARQL results in JSON formats."""
    def binding_transformer(b):
        binding = dict()
        for variable, value in b.items():
            variable = variable[1:]
            binding[variable] = dict()
            binding[variable]["value"] = value
            type, extra_label, extra_value = get_binding_type(value.strip())
            binding[variable]["type"] = type
            if extra_label is not None:
                binding[variable][extra_label] = extra_value
        return binding

    page = dict()
    page["head"] = dict()
    page["head"]["vars"] = list(map(lambda x: x[1:], bindings_list[0].keys()))
    page["results"] = dict()
    page["results"]["bindings"] = list(map(binding_transformer, bindings_list))
    page["results"]["size"] = len(bindings_list)
    return page
