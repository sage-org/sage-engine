# utils.py
# Author: Thomas MINIER - MIT License 2017-2018


def compile_literal(literal):
    """Turn a string into a RDFlib URI or Literal.
    Returns the original string if the string is a SPARQL variable"""
    if literal.startswith('?'):
        return literal
    elif literal.startswith('http'):
        return "URIRef(\"{}\")".format(literal)
    elif literal.startswith('"'):
        splited_literal = literal.split('"')
        value = splited_literal[1]
        right_part = splited_literal[2] if len(splited_literal) >= 2 else None
        datatype = "None"
        lang = "None"
        if right_part is not None and right_part.startswith("^^"):
            datatype = right_part.split("^^")[1].strip("<>")
            if datatype == "http://www.w3.org/2001/XMLSchema#integer":
                return str(int(value))
            elif datatype == "http://www.w3.org/2001/XMLSchema#double" or datatype == "http://www.w3.org/2001/XMLSchema#float":
                return str(float(value))
            datatype = "\"{}\"".format(datatype)
        elif right_part is not None and right_part.startswith("@"):
            lang = "\"{}\"".format(right_part.split("@")[1])
        return "Literal(\"{}\", datatype={}, lang={})".format(value, datatype, lang)
    return "'{}'".format(literal)
