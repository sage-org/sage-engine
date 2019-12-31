# responses.py
# Author: Thomas MINIER - MIT License 2017-2020
from json import dumps
from typing import Dict, Iterable, List, Optional, Tuple
from xml.etree import ElementTree


def analyze_term(value: str) -> Tuple[str, str, Optional[str], Optional[str]]:
    """Analyze a RDF term and extract various information about it.

    Argument: The RDF term to analyze.

    Returns: A tuple (`value`, `type`, `extra_label`, `extra_value`) where:
      * `value` is the term value.
      * `type` is the type of the term (literal or uri).
      * `extra_label` is the type of an extra element for this term (datatype or xml:lang).
      * `extra_value` is the value of an extra element for this term.
    
    Example:
      >>> analyze_term("<http://example.org#Anna>")
      ("<http://example.org#Anna>", "uri", None, None)
      >>> analyze_term('"Anna"')
      ('"Anna"', "literal", None, None)
      >>> analyze_term('"Anna"@en')
      ('"Anna"', "literal", "xml:lang", "en")
      >>> analyze_term('"Anna"^^<http://datatype.org#string>')
      ('"Anna"', "literal", "datatype", "<http://datatype.org#string>")
    """
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


def stream_json_list(iterator: Iterable[Dict[str, str]]) -> Iterable[str]:
    """A generator for streaming a list of JSON results in an HTTP response.
    
    Argument: An iterator which yields solutions bindings.

    Yields: Solution bindings as string-encoded JSON.
    """
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


def skolemize_one(bnode: str, url: str) -> str:
    """Skolemize a blank node.

    If the input value is not a Blank node, then do nothing.

    Args:
      * value: RDF Blank node to skolemize.
      * url: Prefix URL used for skolemization.

    Returns:
      The skolemized blank node, or the value itself if it was not a blank node.
    """
    return f"{url}/bnode#{bnode[2:]}" if bnode.startswith("_:") else bnode


def skolemize(bindings: Iterable[Dict[str, str]], url: str) -> Iterable[Dict[str, str]]:
    """Skolemize blank nodes in a list of solution bindings.
    
    Args:
      * bindings: An iterable which yields set of solution bindings to process.
      * url: Prefix URL used for skolemization.

    Yields:
      Solution bindings, where blank nodes have been skolemized using the input URL.
    """
    for b in bindings:
        r = dict()
        for key, value in b.items():
            r[key] = skolemize_one(value, url)
        yield r


def ntriples_streaming(triples: Iterable[Tuple[str, str, str]]) -> Iterable[str]:
    """Serialize RDF triples in N-Triples string format in a iterable-fashion.

    Argument: An iterable which yields RDF triples to process.
    
    Yields: RDF triples in a string format, encoded in the N-Triples format.
    """
    for s, p, o in triples:
        subj = f"<{s}>" if not s.startswith("\"") else s
        pred = f"<{p}>"
        obj = f"<{o}>" if not o.startswith("\"") else o
        yield f"{subj} {pred} {obj} .\n"


def binding_to_json(binding: Dict[str, str]) -> dict:
    """Format a set of solutions bindings in the W3C SPARQL JSON format.
    
    Argument: A set of solution bindings.

    Returns: The input set of solution bindings, encoded in the W3C SPARQL JSON format.
    """
    json_binding = dict()
    for variable, value in binding.items():
        variable = variable[1:]
        json_binding[variable] = dict()
        value, type, extra_label, extra_value = analyze_term(value.strip())
        json_binding[variable]["value"] = value
        json_binding[variable]["type"] = type
        if extra_label is not None:
            json_binding[variable][extra_label] = extra_value
    return json_binding


def w3c_json_streaming(bindings: Iterable[Dict[str, str]], next_link: Optional[str], stats: dict, skol_url: str) -> Iterable[str]:
    """Yield a page of SaGe results in the W3C SPARQL JSON results format, so it can be sent in an HTTP response.
    
    Args:
      * bindings: An iterable which yields set of solution bindings.
      * next_link: Link to a SaGe saved plan. Use `None` if there is no one, i.e., the query execution has completed during the quantum.
      * stats: Statistics about query execution.
      * skol_url: URL used for the skolemization of blank nodes.
    
    Yields:
      A page of SaGe results in the W3C SPARQL JSON results format.
    """
    hasNext = "true" if next_link is not None else "false"
    vars = list(map(lambda x: x[1:], bindings[0].keys()))
    # generate headers
    yield "{\"head\":{\"vars\":["
    yield ",".join(map(lambda x: f"\"{x}\"", vars))
    yield f"],\"pageSize\":{len(bindings)},\"hasNext\":{hasNext},"
    if next_link is not None:
        yield f"\"next\":\"{next_link}\","
    yield "\"stats\":" + dumps(stats, separators=(',', ':')) + "},\"results\":{\"bindings\":["
    # generate results
    b_iter = map(binding_to_json, skolemize(bindings, skol_url))
    yield from stream_json_list(b_iter)
    yield "]}}"


def raw_json_streaming(bindings: Iterable[Dict[str, str]], next_link: Optional[str], stats: dict, skol_url: str) -> Iterable[str]:
    """Yield a page of SaGe results in a non-standard JSON format, so it can be sent in an HTTP response.
    
    Args:
      * bindings: An iterable which yields set of solution bindings.
      * next_link: Link to a SaGe saved plan. Use `None` if there is no one, i.e., the query execution has completed during the quantum.
      * stats: Statistics about query execution.
      * skol_url: URL used for the skolemization of blank nodes.
    
    Yields:
      A page of SaGe results in the W3C SPARQL JSON results format.
    """
    hasNext = "true" if next_link is not None else "false"
    yield "{\"bindings\":["
    b_iter = skolemize(bindings, skol_url)
    yield from stream_json_list(b_iter)
    yield f"],\"pageSize\":{len(bindings)},\"hasNext\":{hasNext},"
    if next_link is not None:
        yield f"\"next\":\"{next_link}\","
    else:
        yield "\"next\":null,"
    yield "\"stats\":" + dumps(stats, separators=(',', ':')) + "}"


def bindings_to_w3c_xml(bindings: Iterable[Dict[str, str]], skol_url: str) -> ElementTree.Element:
    """Formats a set of bindings into SPARQL results in the W3C SPARQL XML format.
    
    Args:
      * bindings: An iterable which yields set of solution bindings.
      * skol_url: URL used for the skolemization of blank nodes.

    Returns: The input set of solution bindings, encoded in the W3C SPARQL XML format.
    """
    def convert_binding(b, root):
        result_node = ElementTree.SubElement(root, "result")
        for variable, value in b.items():
            v_name = variable[1:]
            b_node = ElementTree.SubElement(result_node, "binding", name=v_name)
            value, type, extra_label, extra_value = analyze_term(value.strip())
            if type == "uri":
                uri_node = ElementTree.SubElement(b_node, "uri")
                uri_node.text = value
            elif type == "literal":
                literal_node = literal_node = ElementTree.SubElement(b_node, "literal")
                literal_node.text = value
                if extra_label is not None:
                    literal_node.set(extra_label, extra_value)
        return result_node

    vars = list(map(lambda x: x[1:], bindings[0].keys()))
    root = ElementTree.Element("sparql", xmlns="http://www.w3.org/2005/sparql-results#")
    # build head
    head = ElementTree.SubElement(root, "head")
    for variable in vars:
        ElementTree.SubElement(head, "variable", name=variable)
    # build results
    results = ElementTree.SubElement(root, "results")
    for binding in skolemize(bindings, skol_url):
        convert_binding(binding, results)
    return root


def w3c_xml(bindings: Iterable[Dict[str, str]], next_link: Optional[str], stats: dict, skol_url: str) -> Iterable[str]:
    """Yield a page of SaGe results in the W3C SPARQL XML results format, so it can be sent in an HTTP response.
    
    Args:
      * bindings: An iterable which yields set of solution bindings.
      * next_link: Link to a SaGe saved plan. Use `None` if there is no one, i.e., the query execution has completed during the quantum.
      * stats: Statistics about query execution.
      * skol_url: URL used for the skolemization of blank nodes.
    
    Yields:
      A page of SaGe results in the W3C SPARQL JSON results format.
    """
    page = bindings_to_w3c_xml(bindings, skol_url)
    head = page.find("head")
    controls = ElementTree.SubElement(head, "controls")
    hasNext_node = ElementTree.SubElement(controls, "hasNext")
    hasNext_node.text = str(next_link is not None)
    next_node = ElementTree.SubElement(controls, "next")
    next_node.text = next_link
    # TODO include stats
    return ElementTree.tostring(page, encoding="utf-8").decode("utf-8")
