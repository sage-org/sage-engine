# topk.py
# Author: Pascal Molli - MIT License 2017-2020
from time import time
from typing import Dict, List, Optional

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedOneTopkIterator, mapping
from sage.query_engine.iterators.utils import find_in_mappings
from sage.query_engine.iterators.filter import to_rdflib_term

from sage.query_engine.protobuf.utils import pyDict_to_protoDict

from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery

from rdflib.plugins.sparql.parser import OrderClause
#>>> OrderClause.parseString("order by ?s")
#>>> ok you can parse, but get a parseTree, algebra expression

#from sage.query_engine.optimizer.query_parser import parse_filter_expr

import sys


class OneTopkIterator(PreemptableIterator):
    """A OneTopkIterator evaluates a SPARQL Orderby + limit k query.

    Args:
      * source: Previous iterator in the pipeline.
      * olist: list of variable to sort.
      * context: Information about the query execution.
    """

    def __init__(self, source: PreemptableIterator, context: dict, expr, topk = None):
        super(OneTopkIterator, self).__init__()
        self._source = source
        self._rawexpr=expr

        #print(f'expr:{expr}')
        compiled_expr = parseQuery(f"SELECT * WHERE {{?s ?p ?o}} order by {expr}")
        compiled_expr = translateQuery(compiled_expr)
        self._prologue = compiled_expr.prologue
        self._expr = compiled_expr.algebra.p.p.expr
        # print(self._expr)
        # if topk is None:
        #     self._topk=[{'?s': 'http://db.uwaterloo.ca/~galuc/wsdbm/Offer34327', '?p': 'http://purl.org/goodrelations/price', '?o': '"503"'},
        #     {'?s': 'http://db.uwaterloo.ca/~galuc/wsdbm/Offer34327', '?p': 'http://purl.org/goodrelations/serialNumber', '?o': '"17519453"'}]
        # else:
        #     self._topk=topk
        self._topk=None
        if topk is not None:
            print(f'topk is set:{topk}')
            self._topk=topk

    def __repr__(self) -> str:
        return f"<OneTopkIterator {self._rawexpr} {self._topk} FROM {self._source}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "onetopk"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        b=self._source.has_next()
        return b

    def next_stage(self, mappings: Dict[str, str]):
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        self._source.next_stage(mappings)

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        if not self.has_next():
            return None
        mappings = await self._source.next()
        if mappings is None:
            return None
        return self.updatetopk(mappings)

    def updatetopk(self,mappings: Dict[str,str]):
        if self._topk is None:
            return mappings
        for e in self._expr:
            try:
                if e.order=='DESC':
                    if to_rdflib_term(mappings['?' + e.expr]) > to_rdflib_term(self._topk['?' + e.expr]):
                        #print(f"onetopk:saved")
                        return None
                else:
                    if to_rdflib_term(mappings['?' + e.expr]) < to_rdflib_term(self._topk['?' + e.expr]):
                        #print(f"onetopk:saved")
                        return None
            except:
                print(f'error with mapping:{mappings} topk:{self._topk}')
                print("Unexpected error:", sys.exc_info()[0])
                raise
        return mappings

    def save(self) -> SavedOneTopkIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_topk = SavedOneTopkIterator()
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_topk, source_field).CopyFrom(self._source.save())
        saved_topk.expr=self._rawexpr
        if self._topk is not None:
            pyDict_to_protoDict(self._topk, saved_topk.topk)
        return saved_topk
