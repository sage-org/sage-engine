from json import dumps
from typing import Dict, Iterable, Optional, Tuple, Any
from xml.etree import ElementTree


def analyze_term(value: str) -> Tuple[str, str, Optional[str], Optional[str]]:
    """
    Analyzes a RDF term and extracts various information about it.

    Parameters
    ----------
    value: str
        The RDF term to analyze.

    Returns
    -------
    Tuple[str, str, None | str, None | str]
        A tuple (`value`, `type`, `extra_label`, `extra_value`) where:
            - `value` is the term value.
            - `type` is the type of the term (literal or uri).
            - `extra_label` is the type of an extra element for this term (datatype or xml:lang).
            - `extra_value` is the value of an extra element for this term.

    Example
    -------
    >>> analyze_term("<http://example.org#Anna>")
        ("<http://example.org#Anna>", "uri", None, None)
    >>> analyze_term('"Anna"')
        ('"Anna"', "literal", None, None)
    >>> analyze_term('"Anna"@en')
        ('"Anna"', "literal", "xml:lang", "en")
    >>> analyze_term('"Anna"^^<http://datatype.org#string>')
        ('"Anna"', "literal", "datatype", "<http://datatype.org#string>")
    """
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
    return value, "uri", None, None


def stream_json_list(iterator: Iterable[Dict[str, str]]) -> Iterable[str]:
    """
    A generator that streams a list of JSON results in an HTTP response.

    Parameters
    ----------
    Iterable[Dict[str, str]]
        An iterator which yields solutions mappings.

    Yields
    ------
    Iterable[str]
        A solution mappings as a string-encoded JSON.
    """
    try:
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
    """
    Skolemizes a blank node.

    If the input value is not a Blank node, then do nothing.

    Parameters
    ----------
    value: str
        RDF Blank node to skolemize.
    url: str
        Prefix URL used for skolemization.

    Returns
    -------
    str
        The skolemized blank node, or the value itself if it was not a blank node.
    """
    return f"{url}/bnode#{bnode[2:]}" if bnode.startswith("_:") else bnode


def skolemize(solutions: Iterable[Dict[str, str]], url: str) -> Iterable[Dict[str, str]]:
    """
    Skolemizes blank nodes in a list of solution mappings.

    Parameters
    ----------
    solutions: Iterable[Dict[str, str]]
        An iterable which yields set of solution bindings to process.
    url: str
        Prefix URL used for skolemization.

    Yields
    ------
    Iterable[Dict[str, str]]
        Solutions mappings where blank nodes have been skolemized using
        the input URL.
    """
    for solution in solutions:
        skol_solution = dict()
        for key, value in solution.items():
            skol_solution[key] = skolemize_one(value, url)
        yield skol_solution


def ntriples_streaming(triples: Iterable[Tuple[str, str, str]]) -> Iterable[str]:
    """
    Serializes RDF triples into N-Triples in an iterable-fashion.

    Parameters
    ----------
    Iterable[Tuple[str, str, str]]
        An iterable which yields RDF triples to process.

    Yields
    ------
    Iterable[str]
        RDF triples encoded as N-Triples.
    """
    for s, p, o in triples:
        subj = f"<{s}>" if not s.startswith("\"") else s
        pred = f"<{p}>"
        obj = f"<{o}>" if not o.startswith("\"") else o
        yield f"{subj} {pred} {obj} .\n"


def mappings_to_json(solution: Dict[str, str]) -> Dict[str, Any]:
    """
    Formats a solution mappings in the W3C SPARQL JSON format.

    Parameters
    ----------
    Dict[str, str]
        A solution mappings.

    Returns
    -------
    Dict[str, Any]
        A solution mappings formated using the W3C SPARQL JSON format.
    """
    json_mappings = dict()
    for variable, value in solution.items():
        json_mappings[variable[1:]] = dict()
        value, type, extra_label, extra_value = analyze_term(value.strip())
        json_mappings[variable[1:]]["value"] = value
        json_mappings[variable[1:]]["type"] = type
        if extra_label is not None:
            json_mappings[variable[1:]][extra_label] = extra_value
    return json_mappings


def w3c_json_streaming(
    solutions: Iterable[Dict[str, str]], next_link: Optional[str],
    stats: Dict[str, Any], skol_url: str
) -> Iterable[str]:
    """
    Yields a page of SaGe results in the W3C SPARQL JSON results format,
    so it can be sent in an HTTP response.

    Parameters
    ----------
    solutions: Iterable[Dict[str, str]]
        An iterable which yields set of solution mappings.
    next_link: None | str
        A link to a SaGe saved plan. Use `None` if there is no one, i.e., the
        query execution has completed during the quantum.
    stats: Dict[str, Any]
        Statistics about query execution.
    skol_url: str
        URL used for the skolemization of blank nodes.

    Yields
    ------
    Iterable[str]
        A page of SaGe results in the W3C SPARQL JSON results format.
    """
    hasNext = "true" if next_link is not None else "false"
    vars = list(map(lambda x: x[1:], solutions[0].keys()))
    # generate headers
    yield "{\"head\":{\"vars\":["
    yield ",".join(map(lambda x: f"\"{x}\"", vars))
    yield f"],\"pageSize\":{len(solutions)},\"hasNext\":{hasNext},"
    if next_link is not None:
        yield f"\"next\":\"{next_link}\","
    yield "\"stats\":" + dumps(stats, separators=(',', ':')) + "},\"results\":{\"bindings\":["
    # generate results
    b_iter = map(mappings_to_json, skolemize(solutions, skol_url))
    yield from stream_json_list(b_iter)
    yield "]}}"


def raw_json_streaming(
    solutions: Iterable[Dict[str, str]], next_link: Optional[str],
    stats: Dict[str, Any], skol_url: str
) -> Iterable[str]:
    """
    Yields a page of SaGe results in a non-standard JSON format, so it can be
    sent in an HTTP response.

    Parameters
    ----------
    solutions: Iterable[Dict[str, str]]
        An iterable which yields set of solution bindings.
    next_link: None | str
        Link to a SaGe saved plan. Use `None` if there is no one, i.e., the
        query execution has completed during the quantum.
    stats: Dict[str, Any]
        Statistics about query execution.
    skol_url: str
        URL used for the skolemization of blank nodes.

    Yields
    ------
    Iterable[str]
        A page of SaGe results in the W3C SPARQL JSON results format.
    """
    hasNext = "true" if next_link is not None else "false"
    yield "{\"bindings\":["
    b_iter = skolemize(solutions, skol_url)
    yield from stream_json_list(b_iter)
    yield f"],\"pageSize\":{len(solutions)},\"hasNext\":{hasNext},"
    if next_link is not None:
        yield f"\"next\":\"{next_link}\","
    else:
        yield "\"next\":null,"
    yield "\"stats\":" + dumps(stats, separators=(',', ':')) + "}"


def solutions_to_w3c_xml(
    solutions: Iterable[Dict[str, str]], skol_url: str
) -> ElementTree.Element:
    """
    Formats query solutions into SPARQL results in the W3C SPARQL XML format.

    Parameters
    ----------
    solutions: Iterable[Dict[str, str]]
        An iterable which yields set of solution bindings.
    skol_url: str
        URL used for the skolemization of blank nodes.

    Returns
    -------
    ElementTree.Element
        The input set of solution mappings, encoded in the W3C SPARQL XML format.
    """
    def convert_solution(solution, root):
        result_node = ElementTree.SubElement(root, "result")
        for variable, value in solution.items():
            b_node = ElementTree.SubElement(result_node, "binding", name=variable[1:])
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

    vars = list(map(lambda x: x[1:], solutions[0].keys()))
    root = ElementTree.Element("sparql", xmlns="http://www.w3.org/2005/sparql-results#")
    # build head
    head = ElementTree.SubElement(root, "head")
    for variable in vars:
        ElementTree.SubElement(head, "variable", name=variable)
    # build results
    results = ElementTree.SubElement(root, "results")
    for solution in skolemize(solutions, skol_url):
        convert_solution(solution, results)
    return root


def w3c_xml(
    solutions: Iterable[Dict[str, str]], next_link: Optional[str],
    stats: Dict[str, Any], skol_url: str
) -> Iterable[str]:
    """
    Yields a page of SaGe results in the W3C SPARQL XML results format, so it
    can be sent in an HTTP response.

    Parameters
    ----------
    solutions: Iterable[Dict[str, str]]
        An iterable which yields set of solution bindings.
    next_link: None | str
        Link to a SaGe saved plan. Use `None` if there is no one, i.e., the query execution has completed during the quantum.
    stats: Dict[str, Any]
        Statistics about query execution.
    skol_url: str
        URL used for the skolemization of blank nodes.

    Yields
    ------
    Iterable[str]
        A page of SaGe results in the W3C SPARQL JSON results format.
    """
    page = solutions_to_w3c_xml(solutions, skol_url)
    head = page.find("head")
    controls = ElementTree.SubElement(head, "controls")
    hasNext_node = ElementTree.SubElement(controls, "hasNext")
    hasNext_node.text = str(next_link is not None)
    next_node = ElementTree.SubElement(controls, "next")
    next_node.text = next_link
    # TODO include stats
    return ElementTree.tostring(page, encoding="utf-8").decode("utf-8")
