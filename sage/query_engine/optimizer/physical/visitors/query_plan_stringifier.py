from typing import Dict, Any

from sage.query_engine.optimizer.physical.plan_visitor import PhysicalPlanVisitor
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator


class QueryPlanStringifier(PhysicalPlanVisitor):

    def visit_projection(self, node: PreemptableIterator, context: Dict[str, Any] = {}) -> str:
        projection = ' '.join(node._projection)
        if node._source.serialized_name().startswith('topk'):
            topk = node._source
            where_clause = self.visit(topk._source)
            order_conditions = f"ORDER BY {topk._order_conditions} LIMIT {topk._limit}"
        else:
            where_clause = self.visit(node._source)
            order_conditions = ""
        return f"SELECT {projection} WHERE {{\n{where_clause}\n}} {order_conditions}"

    def visit_filter(self, node: PreemptableIterator, context: Dict[str, Any] = {}) -> str:
        return f"{self.visit(node._source)}\n\tFILTER ({node._expression})."

    def visit_join(self, node: PreemptableIterator, context: Dict[str, Any] = {}) -> str:
        return f"{self.visit(node._left)}\n{self.visit(node._right)}"

    def visit_values(self, node: PreemptableIterator, context: Dict[str, Any] = {}) -> str:
        variables = ' '.join(node._values[0].keys())
        solution_mappings = list()
        for mappings in node._values:
            solution_mappings.append(f"({ ' '.join(mappings.values()) })")
        return f"\tVALUES ({variables}) {{ { ' '.join(solution_mappings) } }}."

    def visit_scan(self, node: PreemptableIterator, context: Dict[str, Any] = {}) -> str:
        if node._pattern["subject"].startswith('http'):
            subject = f'<{node._pattern["subject"]}>'
        else:
            subject = node._pattern["subject"]
        if node._pattern["predicate"].startswith('http'):
            predicate = f'<{node._pattern["predicate"]}>'
        else:
            predicate = node._pattern["predicate"]
        if node._pattern["object"].startswith('http'):
            object = f'<{node._pattern["object"]}>'
        else:
            object = node._pattern["object"]
        return f"\t{subject} {predicate} {object}."

    def visit_limit(self, node: PreemptableIterator, context: Dict[str, Any] = {}) -> str:
        return f"{self.visit(node._source, context=context)} LIMIT {node._limit}"
