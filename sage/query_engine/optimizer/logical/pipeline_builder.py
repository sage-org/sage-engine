import sage.query_engine.optimizer.utils as utils

from sage.query_engine.types import RDFLibOperator, RDFLibTriplePattern, QueryContext
from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor
from sage.query_engine.expression import Expression
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.limit import LimitIterator
from sage.query_engine.iterators.topk.topk_factory import TOPKFactory
from sage.query_engine.iterators.topk.order_conditions import OrderConditions
from sage.query_engine.iterators.topk.rank_filter import RankFilterIterator
from sage.query_engine.optimizer.logical.join_ordering import JoinOrderingFactory
# from sage.query_engine.iterators.update.delete import DeleteIterator
# from sage.query_engine.iterators.update.if_exists import IfExistsIterator
# from sage.query_engine.iterators.update.insert import InsertIterator
# from sage.query_engine.iterators.update.serializable import SerializableUpdateIterator
# from sage.query_engine.iterators.update.update_sequence import UpdateSequenceIterator


class PipelineBuilder(LogicalPlanVisitor):
    """
    Transforms an RDFLib syntax tree into a pipeline of iterators.

    NOTE: Support for SPARQL UPDATE queries is out of date...
    """

    def visit_select_query(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> PreemptableIterator:
        return self.visit(node.p, context=context)

    def visit_limit_k(
        self, node: RDFLibOperator, context: QueryContext
    ) -> PreemptableIterator:
        if node.p.p.name == "OrderBy":  # ORDER-BY + LIMIT-K = TOP-K
            iterator = self.visit(node.p.p.p, context=context)
            expression = OrderConditions.from_rdflib(node.p.p.expr)

            if context.setdefault("early_pruning", False):
                for partial_expression, is_partial in expression.decompose():
                    iterator = RankFilterIterator(
                        iterator, partial_expression, is_partial=is_partial)

            iterator = TOPKFactory.create(context, iterator, expression, node.length)
            iterator = ProjectionIterator(iterator, [f"?{v}" for v in node.p.PV])
        else:
            iterator = self.visit(node.p, context=context)
            iterator = LimitIterator(iterator, limit=node.length)
        return iterator

    def visit_projection(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> PreemptableIterator:
        iterator = self.visit(node.p, context=context)
        projection = [f"?{v}" for v in node.PV]
        return ProjectionIterator(iterator, projection)

    def visit_join(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> PreemptableIterator:
        left_iterator = self.visit(node.p1, context=context)
        right_iterator = self.visit(node.p2, context=context)
        return IndexJoinIterator(left_iterator, right_iterator)

    def visit_union(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> PreemptableIterator:
        left_iterator = self.visit(node.p1, context=context)
        right_iterator = self.visit(node.p2, context=context)
        return BagUnionIterator(left_iterator, right_iterator)

    def visit_filter(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> PreemptableIterator:
        iterator = self.visit(node.p, context=context)
        expression = Expression(node.expr)
        return FilterIterator(iterator, expression)

    def visit_bgp(
        self, node: RDFLibOperator, context: QueryContext = {}
    ) -> PreemptableIterator:
        iterators = []
        for triple_pattern in node.triples:
            iterators.append(self.visit(triple_pattern, context=context))

        # computes the join ordering
        iterator = JoinOrderingFactory.create(context).compute(iterators)

        # transforms VALUES into FILTERS and adds them on top of the pipeline
        for values_clause in node.values_clauses:
            expressions = []
            for mappings in values_clause.res:
                for key, value in mappings.items():
                    expressions.append(f"{key.n3()} = {value.n3()}")
            expression = Expression.parse(" || ".join(expressions))
            iterator = FilterIterator(iterator, expression)

        return iterator

    def visit_scan(
        self, node: RDFLibTriplePattern, context: QueryContext = {}
    ) -> PreemptableIterator:
        default_graph_uri = context.get("default_graph_uri")
        snapshot = context.get("snapshot")

        triple_pattern = utils.format_triple_pattern(node, graph=default_graph_uri)

        return ScanIterator(triple_pattern, as_of=snapshot)

    # def visit_insert(
    #     self, node: RDFLibOperator, context: QueryContext = {}
    # ) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
    #     quads = utils.get_quads_from_update(node, self._default_graph)
    #     return InsertIterator(quads, Dataset()), []

    # def visit_delete(
    #     self, node: RDFLibOperator, context: QueryContext = {}
    # ) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
    #     quads = utils.get_quads_from_update(node, self._default_graph)
    #     return DeleteIterator(quads, Dataset()), []

    # def visit_modify(
    #     self, node: RDFLibOperator, context: QueryContext = {}
    # ) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
    #     consistency_level = "serializable"
    #     if node.where.name == "Join":
    #         if node.where.p1.name == "BGP" and len(node.where.p1.triples) == 0:
    #             bgp = node.where.p2
    #         elif node.where.p2.name == "BGP" and len(node.where.p2.triples) == 0:
    #             bgp = node.where.p1
    #     if consistency_level == "serializable":
    #         source, cardinalities = self.visit(bgp, context=context)
    #         delete_templates = list()
    #         insert_templates = list()
    #         if node.delete is not None:
    #             delete_templates = utils.get_quads_from_update(
    #                 node.delete, self._default_graph)
    #         if node.insert is not None:
    #             insert_templates = utils.get_quads_from_update(
    #                 node.insert, self._default_graph)
    #         iterator = SerializableUpdateIterator(
    #             source, delete_templates, insert_templates, Dataset())
    #         return iterator, cardinalities
    #     else:
    #         # Build the IF EXISTS style query from an UPDATE query with bounded
    #         # RDF triples in the WHERE, INSERT and DELETE clause.
    #         # - Assert that all RDF triples from the WHERE clause are bounded
    #         for triple_pattern in node.where.triples:
    #             if utils.fully_bounded(triple_pattern):
    #                 continue
    #             raise UnsupportedSPARQL(
    #                 "The SaGe server only supports INSERT/DELETE DATA queries")
    #         delete_templates = list()
    #         insert_templates = list()
    #         if node.delete is not None:
    #             delete_templates = utils.get_quads_from_update(
    #                 node.delete, self._default_graph)
    #         if node.insert is not None:
    #             insert_templates = utils.get_quads_from_update(
    #                 node.insert, self._default_graph)
    #         triples = list(utils.localize_triples(
    #             node.where.triples, [self._default_graph]))
    #         if_exists_op = IfExistsIterator(triples, Dataset(), context["start_timestamp"])
    #         delete_op = DeleteIterator(delete_templates, Dataset())
    #         insert_op = DeleteIterator(insert_templates, Dataset())
    #         iterator = UpdateSequenceIterator(if_exists_op, delete_op, insert_op)
    #         return iterator, []
