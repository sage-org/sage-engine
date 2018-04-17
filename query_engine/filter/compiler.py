# expr_builder.py
# Author: Thomas MINIER - MIT License 2017-2018
import query_engine.filter.expressions as expressions
from query_engine.filter.utils import string_to_rdf


def fargs_to_expression(fargs, expr):
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
    if type(f) is str:
        value = string_to_rdf(f)
        if value.startswith('?'):
            return ('$' + value[1:], set([value]))
        return (value, set())
    fargs = list(map(compile_filter, f['args']))
    if f['operator'] == '+':
        return fargs_to_expression(fargs, expressions.add_expr)
    elif f['operator'] == '-':
        return fargs_to_expression(fargs, expressions.minus_expr)
    elif f['operator'] == '==':
        return fargs_to_expression(fargs, expressions.eq_expr)
    elif f['operator'] == '!=':
        return fargs_to_expression(fargs, expressions.neq_expr)
    elif f['operator'] == '<':
        return fargs_to_expression(fargs, expressions.less_expr)
    elif f['operator'] == '>':
        return fargs_to_expression(fargs, expressions.gt_expr)
    elif f['operator'] == '<=':
        return fargs_to_expression(fargs, expressions.leq_expr)
    elif f['operator'] == '>=':
        return fargs_to_expression(fargs, expressions.gteq_expr)
    elif f['operator'] == '&&':
        return fargs_to_expression(fargs, expressions.and_expr)
    elif f['operator'] == '||':
        return fargs_to_expression(fargs, expressions.or_expr)
    else:
        raise Exception("Unsupported filter type found during filter compilation: '{}'".format(f['operator']))
