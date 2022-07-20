from typing import Dict, Any, Union, Tuple

from rdflib.term import BNode, Literal, URIRef, Variable
from rdflib.plugins.sparql.parserutils import CompValue, Expr
from sage.query_engine.protobuf.iterators_pb2 import (
    RootTree,
    SavedBagUnionIterator,
    SavedFilterIterator,
    SavedIndexJoinIterator,
    SavedProjectionIterator,
    SavedScanIterator,
    SavedValuesIterator,
    SavedLimitIterator,
    SavedTOPKServerIterator,
    SavedPartialTOPKIterator,
    SavedRankFilterIterator)

SavedPlan = Union[
    RootTree,
    SavedBagUnionIterator,
    SavedFilterIterator,
    SavedIndexJoinIterator,
    SavedProjectionIterator,
    SavedScanIterator,
    SavedValuesIterator,
    SavedLimitIterator,
    SavedTOPKServerIterator,
    SavedPartialTOPKIterator,
    SavedRankFilterIterator]

RDFLibTerm = Union[BNode, Literal, URIRef, Variable]
RDFLibTriplePattern = Tuple[RDFLibTerm, RDFLibTerm, RDFLibTerm]
RDFLibNode = Union[CompValue, Expr, RDFLibTerm, RDFLibTriplePattern]
RDFLibExpr = Expr
RDFLibOperator = CompValue
RDFLibMappings = Dict[Variable, RDFLibTerm]

Mappings = Dict[str, str]
TriplePattern = Dict[str, str]
QueryContext = Dict[str, Any]
