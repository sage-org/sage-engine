# topk.py
# Author: Pascal Molli - MIT License 2017-2020
from time import time
from typing import Dict, List, Optional

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedTopkIterator, mapping
from sage.query_engine.iterators.utils import find_in_mappings
from sage.query_engine.iterators.filter import to_rdflib_term

from sage.query_engine.protobuf.utils import pyDict_to_protoDict

from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery

from rdflib.plugins.sparql.parser import OrderClause
#>>> OrderClause.parseString("order by ?s")
#>>> ok you can parse, but get a parseTree, algebra expression

#from sage.query_engine.optimizer.query_parser import parse_filter_expr

def order_to_string(expr):
    res=""
    for i in expr:
        if i.order is None:
            # res.append(f'{parse_filter_expr(i.expr)}')
            res+=f'?{i.expr} '
        else:
            res+=f'{i.order}(?{i.expr})  '
    #print(f"res:{res}")
    return res

class TopkIterator(PreemptableIterator):
    """A TopkIterator evaluates a SPARQL Orderby + limit k query.

    Args:
      * source: Previous iterator in the pipeline.
      * olist: list of variable to sort.
      * context: Information about the query execution.
    """

    def __init__(self, source: PreemptableIterator, context: dict, expr, length=0, topk = None):
        super(TopkIterator, self).__init__()
        self._source = source
        self._length=length
        self._rawexpr=expr

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
        if topk is not None:
            self._topk=topk
        else:
            self._topk=[]

    def __repr__(self) -> str:
        return f"<TopkIterator {self._rawexpr} {self._topk} FROM {self._source}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "topk"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        b=self._source.has_next() or len(self._topk)>0
#        if not b:
#            print(f'topk {self._topk}')
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
            if len(self._topk)>0:
                if len(self._topk)>self._length:
                    self.computetopk()
                #print(f'popped {len(self._topk)}')
                top=self._topk[0]
                self._topk=self._topk[1:]
                return top
            return None
        self._topk.append(mappings)
#        self.updatetopk(mappings)
        return None

    def computetopk(self):
        for e in reversed(self._expr):
            reverse = bool(e.order and e.order == 'DESC')
            self._topk = sorted(self._topk, key=lambda x: to_rdflib_term(x['?'+e.expr]),reverse=reverse)
        if len(self._topk)>self._length:
             del self._topk[self._length:]

    def save(self) -> SavedTopkIterator:
        """Save and serialize the iterator as a Protobuf message"""
        self.computetopk()
        saved_topk = SavedTopkIterator()
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_topk, source_field).CopyFrom(self._source.save())
        saved_topk.expr=self._rawexpr
        saved_topk.length=self._length

        for i in self._topk:
            mu=saved_topk.topk.add()
            pyDict_to_protoDict(i, mu.mu)

        return saved_topk
