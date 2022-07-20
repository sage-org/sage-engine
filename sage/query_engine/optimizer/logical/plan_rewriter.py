from typing import List
from rdflib.plugins.sparql.algebra import Filter

from sage.query_engine.types import RDFLibExpr, RDFLibOperator, QueryContext
from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor


class PlanRewriter(LogicalPlanVisitor):
    """
    This class is used:
        (1) to merge BGP and ToMultiSet nodes together.
        (2) to split AND expressions into many FILTER clauses.

    First example; merging two BGPs:
        - from: Project__{p: Join__{p1: BGP__{...}, p2: BGP__{...}}}
        - to: Project__{p: BGP__{...}}

    Second example; splitting an AND expression:
        - from:
            Project__{
                p: Filter__{
                    expr: ConditionalAndExpression__{
                        expr: RelationalExpression__{expr: ?v1, op: '=', other: 'string1'},
                        other: RelationalExpression__{expr: ?v2, op: '=', other: 'string2'}
                    },
                    p: BGP__{...}
                }
            }
        - to:
            Project__{
                p: Filter__{
                    expr: RelationalExpression__{expr: ?v1, op: '=', other: 'string1'},
                    p: Filter__{
                        expr: RelationalExpression__{expr: ?v2, op: '=', other: 'string2'},
                        p: BGP__{...}
                    }
                }
            }
    """

    def merge_bgps(self, node: RDFLibOperator) -> RDFLibOperator:
        if node.p1.name == "BGP" or node.p1.name == "ToMultiSet":
            if node.p2.name == "BGP" or node.p2.name == "ToMultiSet":
                bgp = {"triples": [], "values_clauses": []}
                if node.p1.name == "ToMultiSet":
                    bgp["values_clauses"].append(node.p1.p)
                else:
                    bgp["triples"].extend(node.p1.triples)
                    bgp["values_clauses"].extend(node.p1.values_clauses)
                if node.p2.name == "ToMultiSet":
                    bgp["values_clauses"].append(node.p2.p)
                else:
                    bgp["triples"].extend(node.p2.triples)
                    bgp["values_clauses"].extend(node.p2.values_clauses)
                return RDFLibOperator("BGP", **bgp)
        return node

    def visit_select_query(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> RDFLibOperator:
        node.p = self.visit(node.p, context=context)
        if node.p.name == "Join":
            node.p = self.merge_bgps(node.p)
        return node

    def visit_limit_k(
        self, node: RDFLibOperator, context: QueryContext
    ) -> RDFLibOperator:
        node.p = self.visit(node.p, context=context)
        return node

    def visit_orderby(
        self, node: RDFLibOperator, context: QueryContext
    ) -> RDFLibOperator:
        node.p = self.visit(node.p, context=context)
        return node

    def visit_projection(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> RDFLibOperator:
        node.p = self.visit(node.p, context=context)
        if node.p.name == "Join":
            node.p = self.merge_bgps(node.p)
        return node

    def visit_to_multiset(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> RDFLibOperator:
        node.p = self.visit(node.p, context=context)
        return node

    def visit_values(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> RDFLibOperator:
        return node

    def visit_filter(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> RDFLibOperator:
        sub_expressions = self.visit(node.expr, context=context)
        node.expr = sub_expressions.pop()
        while len(sub_expressions) > 0:
            node.p = Filter(sub_expressions.pop(), node.p)
        node.p = self.visit(node.p, context=context)
        if node.p.name == "Join":
            node.p = self.merge_bgps(node.p)
        return node

    def visit_join(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> RDFLibOperator:
        node.p1 = self.visit(node.p1, context=context)
        if node.p1.name == "Join":
            node.p1 = self.merge_bgps(node.p1)
        node.p2 = self.visit(node.p2, context=context)
        if node.p2.name == "Join":
            node.p2 = self.merge_bgps(node.p2)
        return node

    def visit_union(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> RDFLibOperator:
        node.p1 = self.visit(node.p1, context=context)
        if node.p1.name == "Join":
            node.p1 = self.merge_bgps(node.p1)
        node.p2 = self.visit(node.p2, context=context)
        if node.p2.name == "Join":
            node.p2 = self.merge_bgps(node.p2)
        return node

    def visit_bgp(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> RDFLibOperator:
        node.values_clauses = []
        return node

    def visit_expression(
        self, node: RDFLibExpr, context: QueryContext = {}
    ) -> List[RDFLibExpr]:
        if node.name == "ConditionalAndExpression":
            return self.visit_conditional_and_expression(node, context=context)
        return [node]

    def visit_conditional_and_expression(
        self, node: RDFLibExpr, context: QueryContext = {}
    ) -> List[RDFLibExpr]:
        operands = self.visit(node.expr, context=context)
        for other in node.other:
            operands.extend(self.visit(other, context=context))
        return operands
