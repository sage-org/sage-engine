from pyparsing import ParseException
from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate

from sage.query_engine.optimizer.logical.plan_visitor import Node


class Parser():

    @staticmethod
    def parse(query: str) -> Node:
        try:
            return translateQuery(parseQuery(query)).algebra
        except ParseException:
            return translateUpdate(parseUpdate(query))
