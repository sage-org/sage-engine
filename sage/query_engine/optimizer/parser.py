from pyparsing import ParseException
from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate

from sage.query_engine.types import RDFLibNode


class Parser():

    @staticmethod
    def parse(query: str) -> RDFLibNode:
        try:
            return translateQuery(parseQuery(query)).algebra
        except ParseException:
            raise Exception("SPARQL UPDATE queries are not working with this version of SaGe.")
            return translateUpdate(parseUpdate(query))
