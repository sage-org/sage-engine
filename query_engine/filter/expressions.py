# expression.py
# Author: Thomas MINIER - MIT License 2017-2018
from string import Template


def unary_expression(templateString):
    """High-order function used to build a unary expression"""
    def expression(value):
        return templateString.format(value)
    return expression


def binary_expression(templateString):
    """High-order function used to build a binary expression"""
    def expression(left, right):
        return templateString.format(left, right)
    return expression


add_expr = binary_expression("({} + {})")

minus_expr = binary_expression("({} - {})")

mul_expr = binary_expression("({} * {})")

div_expr = binary_expression("({} / {})")

and_expr = binary_expression("({}) and ({})")

or_expr = binary_expression("({}) or ({})")

eq_expr = binary_expression("{} == {}")

neq_expr = binary_expression("{} != {}")

less_expr = binary_expression("{} < {}")

leq_expr = binary_expression("{} <= {}")

gt_expr = binary_expression("{} > {}")

gteq_expr = binary_expression("{} >= {}")

bind_expr = unary_expression("sparql_bind({})")

sameTerm_expr = unary_expression("sameTerm({})")

in_expr = unary_expression("({} in {})")

not_in_expr = binary_expression("({} not in {})")

is_iri_expr = unary_expression("isIRI({})")

is_literal_expr = unary_expression("isLiteral({})")

is_numeric_expr = unary_expression("isinstance({}, Number)")

lang_expr = unary_expression("sparql_lang({})")

datatype_expr = unary_expression("sparql_datatype({})")

STRUUID_expr = unary_expression("sparql_STRUUID({})")

UUID_expr = unary_expression("sparql_UUID({})")
