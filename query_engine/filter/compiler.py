# expr_builder.py
# Author: Thomas MINIER - MIT License 2017-2018
import query_engine.filter.expressions as expressions
from query_engine.filter.utils import compile_literal


def fargs_to_unary_expression(fargs, expr):
    return (expr(fargs[0][0]), fargs[0][1])


def fargs_to_empty_expression(expr):
    return (expr(None), set())


def fargs_to_binary_expression(fargs, expr):
    print(fargs)
    return (expr(fargs[0][0], fargs[1][0]), fargs[0][1] | fargs[1][1])


def compile_filter(f):
    """
        Compile a filter clause from a serialized filter into a python template expression.

        Args:
            - f [dict] a serialized filter of the form {"type": "...", "operation": "...", "args": [...]}
        Returns:
            A tuple (expression, vars) where:
                - expression (type=string) is a python template string
                - vars (type=set of string) is the set of all SPARQL variables in the compiled expression
    """
    if type(f) is str or type(f) is list:
        value = compile_literal(f)
        if value.startswith('?'):
            return ('$' + value[1:], set([value]))
        return (value, set())
    fargs = list(map(compile_filter, f['args']))
    if f['operator'] == '+':
        return fargs_to_binary_expression(fargs, expressions.add_expr)
    elif f['operator'] == '-':
        return fargs_to_binary_expression(fargs, expressions.minus_expr)
    elif f['operator'] == '==':
        return fargs_to_binary_expression(fargs, expressions.eq_expr)
    elif f['operator'] == '!=':
        return fargs_to_binary_expression(fargs, expressions.neq_expr)
    elif f['operator'] == '<':
        return fargs_to_binary_expression(fargs, expressions.less_expr)
    elif f['operator'] == '>':
        return fargs_to_binary_expression(fargs, expressions.gt_expr)
    elif f['operator'] == '<=':
        return fargs_to_binary_expression(fargs, expressions.leq_expr)
    elif f['operator'] == '>=':
        return fargs_to_binary_expression(fargs, expressions.gteq_expr)
    elif f['operator'] == '&&':
        return fargs_to_binary_expression(fargs, expressions.and_expr)
    elif f['operator'] == '||':
        return fargs_to_binary_expression(fargs, expressions.or_expr)
    elif f['operator'] == '!':
        return fargs_to_unary_expression(fargs, expressions.not_expr)
    elif f['operator'] == 'bound':
        return fargs_to_unary_expression(fargs, expressions.bound_expr)
    elif f['operator'] == 'sameterm':
        return fargs_to_unary_expression(fargs, expressions.sameTerm_expr)
    elif f['operator'] == 'in':
        return fargs_to_binary_expression(fargs, expressions.in_expr)
    elif f['operator'] == 'notin':
        return fargs_to_binary_expression(fargs, expressions.not_in_expr)
    elif f['operator'] == 'isiri':
        return fargs_to_unary_expression(fargs, expressions.is_iri_expr)
    elif f['operator'] == 'isliteral':
        return fargs_to_unary_expression(fargs, expressions.is_literal_expr)
    elif f['operator'] == 'isnumeric':
        return fargs_to_unary_expression(fargs, expressions.is_numeric_expr)
    elif f['operator'] == 'lang':
        return fargs_to_unary_expression(fargs, expressions.lang_expr)
    elif f['operator'] == 'datatype':
        return fargs_to_unary_expression(fargs, expressions.datatype_expr)
    elif f['operator'] == 'struuid':
        return fargs_to_empty_expression(expressions.STRUUID_expr)
    elif f['operator'] == 'uuid':
        return fargs_to_empty_expression(expressions.UUID_expr)
    else:
        raise Exception("Unsupported filter type found during filter compilation: '{}'".format(f['operator']))
