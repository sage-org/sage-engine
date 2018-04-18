# compile_filter_test.py
# Author: Thomas MINIER - MIT License 2017-2018
import pytest
from query_engine.filter.compiler import compile_filter

xsd_number = "\"120\"^^http://www.w3.org/2001/XMLSchema#integer"
literal_120_int = '120'

simple_operations = [
    # tests all simple binary operands
    ("==", ['?s', xsd_number], '$s == {}'.format(literal_120_int), set(["?s"])),
    ("!=", ['?s', xsd_number], '$s != {}'.format(literal_120_int), set(["?s"])),
    ("<", ['?s', xsd_number], '$s < {}'.format(literal_120_int), set(["?s"])),
    ("<=", ['?s', xsd_number], '$s <= {}'.format(literal_120_int), set(["?s"])),
    (">", ['?s', xsd_number], '$s > {}'.format(literal_120_int), set(["?s"])),
    (">=", ['?s', xsd_number], '$s >= {}'.format(literal_120_int), set(["?s"])),
    # check for substitution
    ("==", [xsd_number, "?s"], '{} == $s'.format(literal_120_int), set(["?s"])),
    ("!=", [xsd_number, "?s"], '{} != $s'.format(literal_120_int), set(["?s"])),
    ("<", [xsd_number, "?s"], '{} < $s'.format(literal_120_int), set(["?s"])),
    ("<=", [xsd_number, "?s"], '{} <= $s'.format(literal_120_int), set(["?s"])),
    (">", [xsd_number, "?s"], '{} > $s'.format(literal_120_int), set(["?s"])),
    (">=", [xsd_number, "?s"], '{} >= $s'.format(literal_120_int), set(["?s"])),
    # check all couples of arguments
    ("==", ["?s1", "?s2"], '$s1 == $s2', set(["?s1", "?s2"])),
    ("==", [xsd_number, "?s2"], '{} == $s2'.format(literal_120_int), set(["?s2"])),
    ("==", ["?s1", xsd_number], '$s1 == {}'.format(literal_120_int), set(["?s1"])),
    ("==", [xsd_number, xsd_number], '{} == {}'.format(literal_120_int, literal_120_int), set()),
    # test complex XML datatype
    ("==", ["\"120\"", "?s2"], 'Literal("120", datatype=None, lang=None) == $s2', set(["?s2"])),
    ("==", ["http://example.org#toto", "?s2"], 'URIRef("http://example.org#toto") == $s2', set(["?s2"])),
    ("==", ["\"2008-06-20T00:00:00\"^^http://www.w3.org/2001/XMLSchema#dateTime", "?s2"], 'Literal("2008-06-20T00:00:00", datatype="http://www.w3.org/2001/XMLSchema#dateTime", lang=None) == $s2', set(["?s2"])),
    # test SPARQL functions
    ("bound", ["?s"], "sparql_bound($s)", set(["?s"])),
    ("sameterm", ["?s"], "sameTerm($s)", set(["?s"])),
    ("in", ["?s", ["{}".format(xsd_number), "http://example.org#1"]], "($s in [{}, URIRef(\"http://example.org#1\")])".format(literal_120_int), set(["?s"])),
    ("notin", ["?s", ["{}".format(xsd_number), "http://example.org#1"]], "($s not in [{}, URIRef(\"http://example.org#1\")])".format(literal_120_int), set(["?s"])),
    ("isiri", ["?s"], "isIRI($s)", set(["?s"])),
    ("isliteral", ["?s"], "isLiteral($s)", set(["?s"])),
    ("isnumeric", ["?s"], "isinstance($s, Number)", set(["?s"])),
    ("lang", ["?s"], "sparql_lang($s)", set(["?s"])),
    ("datatype", ["?s"], "sparql_datatype($s)", set(["?s"])),
    ("struuid", ["?s"], "sparql_STRUUID()", set()),
    ("uuid", ["?s"], "sparql_UUID()", set()),
]


@pytest.mark.parametrize("op,args,expected,expectedVars", simple_operations)
def test_simple_operations_filter(op, args, expected, expectedVars):
    filter = {
        "type": "operation",
        "operator": op,
        "args": args
    }
    compiled, vars = compile_filter(filter)
    assert compiled == expected
    assert vars == expectedVars


def test_logical_operations():
    and_filter = {
        "type": "operation",
        "operator": "&&",
        "args": [
            {
                "type": "operation",
                "operator": "==",
                "args": ['?s1', "\"120\"^^http://www.w3.org/2001/XMLSchema#integer"]
            },
            {
                "type": "operation",
                "operator": "<=",
                "args": ['?s2', "\"120\"^^http://www.w3.org/2001/XMLSchema#integer"]
            }
        ]
    }
    compiled, vars = compile_filter(and_filter)
    assert compiled == "($s1 == {}) and ($s2 <= {})".format(literal_120_int, literal_120_int)
    assert vars == set(["?s1", "?s2"])


def test_complex_compile():
    complex_filter = {
        "type": "operation",
        "operator": "&&",
        "args": [
            {
                "type": "operation",
                "operator": "<",
                "args": [
                    "?simProperty1",
                    {
                        "type": "operation",
                        "operator": "+",
                        "args": [
                            "?origProperty1",
                            "\"120\"^^http://www.w3.org/2001/XMLSchema#integer"
                        ]
                    }
                ]
            },
            {
                "type": "operation",
                "operator": ">",
                "args": [
                    "?simProperty1",
                    {
                        "type": "operation",
                        "operator": "-",
                        "args": [
                            "?origProperty1",
                            "\"120\"^^http://www.w3.org/2001/XMLSchema#integer"
                        ]
                    }
                ]
            }
        ]
    }
    compiled, vars = compile_filter(complex_filter)
    assert compiled == '($simProperty1 < ($origProperty1 + {})) and ($simProperty1 > ($origProperty1 - {}))'.format(literal_120_int, literal_120_int)
    assert vars == set(["?simProperty1", "?origProperty1"])
