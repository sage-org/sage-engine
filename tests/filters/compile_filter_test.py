# compile_filter_test.py
# Author: Thomas MINIER - MIT License 2017-2018
import pytest
from query_engine.filter.compiler import compile_filter

simple_operations = [
    # tests all simple binary operands
    ("==", ['?s', "\"120\"^^http://www.w3.org/2001/XMLSchema#integer"], '$s == 120', set(["?s"])),
    ("!=", ['?s', "\"120\"^^http://www.w3.org/2001/XMLSchema#integer"], '$s != 120', set(["?s"])),
    ("<", ['?s', "\"120\"^^http://www.w3.org/2001/XMLSchema#integer"], '$s < 120', set(["?s"])),
    ("<=", ['?s', "\"120\"^^http://www.w3.org/2001/XMLSchema#integer"], '$s <= 120', set(["?s"])),
    (">", ['?s', "\"120\"^^http://www.w3.org/2001/XMLSchema#integer"], '$s > 120', set(["?s"])),
    (">=", ['?s', "\"120\"^^http://www.w3.org/2001/XMLSchema#integer"], '$s >= 120', set(["?s"])),
    # check for substitution
    ("==", ["\"120\"^^http://www.w3.org/2001/XMLSchema#integer", "?s"], '120 == $s', set(["?s"])),
    ("!=", ["\"120\"^^http://www.w3.org/2001/XMLSchema#integer", "?s"], '120 != $s', set(["?s"])),
    ("<", ["\"120\"^^http://www.w3.org/2001/XMLSchema#integer", "?s"], '120 < $s', set(["?s"])),
    ("<=", ["\"120\"^^http://www.w3.org/2001/XMLSchema#integer", "?s"], '120 <= $s', set(["?s"])),
    (">", ["\"120\"^^http://www.w3.org/2001/XMLSchema#integer", "?s"], '120 > $s', set(["?s"])),
    (">=", ["\"120\"^^http://www.w3.org/2001/XMLSchema#integer", "?s"], '120 >= $s', set(["?s"])),
    # check all couples of arguments
    ("==", ["?s1", "?s2"], '$s1 == $s2', set(["?s1", "?s2"])),
    ("==", ["\"120\"^^http://www.w3.org/2001/XMLSchema#integer", "?s2"], '120 == $s2', set(["?s2"])),
    ("==", ["?s1", "\"120\"^^http://www.w3.org/2001/XMLSchema#integer"], '$s1 == 120', set(["?s1"])),
    ("==", ["\"120\"^^http://www.w3.org/2001/XMLSchema#integer", "\"120\"^^http://www.w3.org/2001/XMLSchema#integer"], '120 == 120', set())
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
    assert compiled == "($s1 == 120) and ($s2 <= 120)"
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
    assert compiled == '($simProperty1 < ($origProperty1 + 120)) and ($simProperty1 > ($origProperty1 - 120))'
    assert vars == set(["?simProperty1", "?origProperty1"])
