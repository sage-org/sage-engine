import sage.query_engine.optimizer.utils as utils

from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
from rdflib.plugins.sparql.parserutils import CompValue

from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor, TriplePattern
from sage.database.core.dataset import Dataset
from sage.query_engine.exceptions import UnsupportedSPARQL
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.values import ValuesIterator
from sage.query_engine.iterators.limit import LimitIterator
from sage.query_engine.iterators.topk import TOPKIterator
from sage.query_engine.iterators.topk_collab import TOPKCollabIterator
from sage.query_engine.iterators.utils import EmptyIterator
from sage.query_engine.update.delete import DeleteOperator
from sage.query_engine.update.if_exists import IfExistsOperator
from sage.query_engine.update.insert import InsertOperator
from sage.query_engine.update.serializable import SerializableUpdate
from sage.query_engine.update.update_sequence import UpdateSequenceOperator


class PipelineBuilder(LogicalPlanVisitor):

    def __init__(self, dataset: Dataset, default_graph: str, as_of: Optional[datetime] = None):
        super().__init__()
        self._dataset = dataset
        self._default_graph = default_graph
        self._as_of = as_of

    def __find_connected_pattern__(
        self, variables: List[str], iterators: List[ScanIterator]
    ) -> int:
        for index, iterator in enumerate(iterators):
            pattern_variables = utils.get_vars(iterator._pattern)
            if len(variables & pattern_variables) > 0:
                return index
        return -1

    def __build_ascending_cardinalities_tree__(
        self, scan_iterators: List[ScanIterator]
    ) -> PreemptableIterator:
        print('building ascending cardinalities tree')
        scan_iterators = sorted(
            scan_iterators,
            key=lambda it: (it._cardinality, it._pattern['predicate']))
        pipeline = scan_iterators.pop(0)
        variables = utils.get_vars(pipeline._pattern)
        while len(scan_iterators) > 0:
            next = self.__find_connected_pattern__(variables, scan_iterators)
            if next >= 0:
                scan_iterator = scan_iterators.pop(next)
            else:
                scan_iterator = scan_iterators.pop(0)
            variables = variables | utils.get_vars(scan_iterator._pattern)
            pipeline = IndexJoinIterator(pipeline, scan_iterator)
        return pipeline

    def __build_naive_tree__(
        self, scan_iterators: List[ScanIterator]
    ) -> PreemptableIterator:
        print('building naive tree')
        pipeline = scan_iterators.pop(0)
        while len(scan_iterators) > 0:
            pipeline = IndexJoinIterator(pipeline, scan_iterators.pop(0))
        return pipeline

    def visit_select_query(self, node: CompValue, context: Dict[str, Any] = {}) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        return self.visit(node.p, context=context)

    def visit_limit_k(self, node: CompValue, context: Dict[str, Any]) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        if node.p.p.name == 'OrderBy':
            topk_source, cardinalities = self.visit(node.p.p.p, context=context)
            order_conditions = " ".join([orderCond.repr for orderCond in node.p.p.expr])
            if context['topk_strategy'] == 'FullServer':
                topk_iterator = TOPKIterator(
                    topk_source, order_conditions, node.p.p.expr, limit=node.length)
            else:
                threshold_refresh_rate = float(context['topk_strategy'].split('-')[1])
                topk_iterator = TOPKCollabIterator(
                    topk_source, order_conditions, node.p.p.expr, limit=node.length,
                    threshold_refresh_rate=threshold_refresh_rate)
            projected_variables = list(map(lambda t: '?' + str(t), node.p.PV))
            iterator = ProjectionIterator(topk_iterator, projected_variables)
        else:
            child, cardinalities = self.visit(node.p, context=context)
            iterator = LimitIterator(child, limit=node.length)
        return iterator, cardinalities

    def visit_projection(self, node: CompValue, context: Dict[str, Any] = {}) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        projected_variables = list(map(lambda t: '?' + str(t), node.PV))
        child, cardinalities = self.visit(node.p, context=context)
        return ProjectionIterator(child, projected_variables), cardinalities

    def visit_join(self, node: CompValue, context: Dict[str, Any] = {}) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        left_child, left_cardinalities = self.visit(node.p1, context=context)
        right_child, right_cardinalities = self.visit(node.p2, context=context)
        cardinalities = left_cardinalities + right_cardinalities
        return IndexJoinIterator(left_child, right_child), cardinalities

    def visit_union(self, node: CompValue, context: Dict[str, Any] = {}) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        left_child, left_cardinalities = self.visit(node.p1, context=context)
        right_child, right_cardinalities = self.visit(node.p2, context=context)
        cardinalities = left_cardinalities + right_cardinalities
        return BagUnionIterator(left_child, right_child), cardinalities

    def visit_filter(self, node: CompValue, context: Dict[str, Any] = {}) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        child, cardinalities = self.visit(node.p, context=context)
        return FilterIterator(child, node.expr.repr, node.expr.vars, node.expr), cardinalities

    def visit_to_multiset(self, node: CompValue, context: Dict[str, Any] = {}) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        return self.visit(node.p, context=context)

    def visit_values(self, node: CompValue, context: Dict[str, Any] = {}) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        return ValuesIterator(utils.format_solution_mappings(node.res)), []

    def visit_bgp(self, node: CompValue, context: Dict[str, Any] = {}) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        scan_iterators = list()
        cardinalities = list()
        for triple_pattern in node.triples:
            child, cardinality = self.visit(triple_pattern, context=context)
            scan_iterators.append(child)
            cardinalities.extend(cardinality)
        if context.get('force_order'):
            iterator = self.__build_naive_tree__(scan_iterators)
        else:
            iterator = self.__build_ascending_cardinalities_tree__(scan_iterators)
        for values in node.mappings:
            iterator = IndexJoinIterator(ValuesIterator(utils.format_solution_mappings(values.res)), iterator)
        return iterator, cardinalities

    def visit_scan(self, node: TriplePattern, context: Dict[str, Any] = {}) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        triple_pattern = utils.format_triple_pattern(node, graph=self._default_graph)
        if self._dataset.has_graph(triple_pattern['graph']):
            iterator = ScanIterator(self._dataset.get_graph(triple_pattern['graph']), triple_pattern, as_of=self._as_of)
        else:
            iterator = EmptyIterator()
        cardinality = {'pattern': triple_pattern, 'cardinality': iterator._cardinality}
        return iterator, [cardinality]

    def visit_insert(self, node: CompValue, context: Dict[str, Any] = {}) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        quads = utils.get_quads_from_update(node, self._default_graph)
        return InsertOperator(quads, self._dataset), []

    def visit_delete(self, node: CompValue, context: Dict[str, Any] = {}) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        quads = utils.get_quads_from_update(node, self._default_graph)
        return DeleteOperator(quads, self._dataset), []

    def visit_modify(self, node: CompValue, context: Dict[str, Any] = {}) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        consistency_level = "serializable"
        if node.where.name == 'Join':
            if node.where.p1.name == 'BGP' and len(node.where.p1.triples) == 0:
                bgp = node.where.p2
            elif node.where.p2.name == 'BGP' and len(node.where.p2.triples) == 0:
                bgp = node.where.p1
        if consistency_level == "serializable":
            source, cardinalities = self.visit(bgp, context=context)
            delete_templates = list()
            insert_templates = list()
            if node.delete is not None:
                delete_templates = utils.get_quads_from_update(
                    node.delete, self._default_graph)
            if node.insert is not None:
                insert_templates = utils.get_quads_from_update(
                    node.insert, self._default_graph)
            iterator = SerializableUpdate(
                self._dataset, source, delete_templates, insert_templates)
            return iterator, cardinalities
        else:
            # Build the IF EXISTS style query from an UPDATE query with bounded
            # RDF triples in the WHERE, INSERT and DELETE clause.
            # - Assert that all RDF triples from the WHERE clause are bounded
            for triple_pattern in node.where.triples:
                if utils.fully_bounded(triple_pattern):
                    continue
                raise UnsupportedSPARQL(
                    "The SaGe server only supports INSERT/DELETE DATA queries")
            delete_templates = list()
            insert_templates = list()
            if node.delete is not None:
                delete_templates = utils.get_quads_from_update(
                    node.delete, self._default_graph)
            if node.insert is not None:
                insert_templates = utils.get_quads_from_update(
                    node.insert, self._default_graph)
            triples = list(utils.localize_triples(
                node.where.triples, [self._default_graph]))
            if_exists_op = IfExistsOperator(triples, self._dataset, self._as_of)
            delete_op = DeleteOperator(delete_templates, self._dataset)
            insert_op = DeleteOperator(insert_templates, self._dataset)
            iterator = UpdateSequenceOperator(if_exists_op, delete_op, insert_op)
            return iterator, []


# import sage.query_engine.optimizer.utils as utils
#
# from datetime import datetime
# from typing import Optional, List, Dict, Any, Tuple
# from rdflib.plugins.sparql.parserutils import CompValue
#
# from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor, TriplePattern
# from sage.database.core.dataset import Dataset
# from sage.query_engine.exceptions import UnsupportedSPARQL
# from sage.query_engine.iterators.filter import FilterIterator
# from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
# from sage.query_engine.iterators.projection import ProjectionIterator
# from sage.query_engine.iterators.union import BagUnionIterator
# from sage.query_engine.iterators.nlj import IndexJoinIterator
# from sage.query_engine.iterators.scan import ScanIterator
# from sage.query_engine.iterators.values import ValuesIterator
# from sage.query_engine.iterators.utils import EmptyIterator
# from sage.query_engine.update.delete import DeleteOperator
# from sage.query_engine.update.if_exists import IfExistsOperator
# from sage.query_engine.update.insert import InsertOperator
# from sage.query_engine.update.serializable import SerializableUpdate
# from sage.query_engine.update.update_sequence import UpdateSequenceOperator
#
#
# class PipelineBuilder(LogicalPlanVisitor):
#
#     def __init__(
#         self, dataset: Dataset, default_graph: str,
#         as_of: Optional[datetime] = None
#     ):
#         super().__init__()
#         self._dataset = dataset
#         self._default_graph = default_graph
#         self._as_of = as_of
#
#     def __find_connected_pattern__(
#         self, variables: List[str], iterators: List[ScanIterator]
#     ) -> int:
#         for index, iterator in enumerate(iterators):
#             pattern_variables = utils.get_vars(iterator._pattern)
#             if len(variables & pattern_variables) > 0:
#                 return index
#         return -1
#
#     def __build_ascending_cardinalities_tree__(
#         self, scan_iterators: List[ScanIterator]
#     ) -> PreemptableIterator:
#         print('building ascending cardinalities tree')
#         scan_iterators = sorted(
#             scan_iterators,
#             key=lambda it: (it._cardinality, it._pattern['predicate']))
#         pipeline = scan_iterators.pop(0)
#         variables = utils.get_vars(pipeline._pattern)
#         while len(scan_iterators) > 0:
#             next = self.__find_connected_pattern__(variables, scan_iterators)
#             if next >= 0:
#                 scan_iterator = scan_iterators.pop(next)
#             else:
#                 scan_iterator = scan_iterators.pop(0)
#             variables = variables | utils.get_vars(scan_iterator._pattern)
#             pipeline = IndexJoinIterator(pipeline, scan_iterator)
#         return pipeline
#
#     def __build_naive_tree__(
#         self, scan_iterators: List[ScanIterator]
#     ) -> PreemptableIterator:
#         print('building naive tree')
#         pipeline = scan_iterators.pop(0)
#         while len(scan_iterators) > 0:
#             pipeline = IndexJoinIterator(pipeline, scan_iterators.pop(0))
#         return pipeline
#
#     def visit_select_query(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
#         return self.visit(node.p)
#
#     def visit_projection(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
#         projected_variables = list(map(lambda t: '?' + str(t), node.PV))
#         child, cardinalities = self.visit(node.p)
#         return ProjectionIterator(child, projected_variables), cardinalities
#
#     def visit_join(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
#         left_child, left_cardinalities = self.visit(node.p1)
#         right_child, right_cardinalities = self.visit(node.p2)
#         cardinalities = left_cardinalities + right_cardinalities
#         return IndexJoinIterator(left_child, right_child), cardinalities
#
#     def visit_union(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
#         left_child, left_cardinalities = self.visit(node.p1)
#         right_child, right_cardinalities = self.visit(node.p2)
#         cardinalities = left_cardinalities + right_cardinalities
#         return BagUnionIterator(left_child, right_child), cardinalities
#
#     def visit_filter(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
#         child, cardinalities = self.visit(node.p)
#         return FilterIterator(child, node.expr.repr, node.expr), cardinalities
#
#     def visit_to_multiset(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
#         return self.visit(node.p)
#
#     def visit_values(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
#         return ValuesIterator(utils.format_solution_mappings(node.res)), []
#
#     def visit_bgp(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
#         scan_iterators = list()
#         cardinalities = list()
#         for triple_pattern in node.triples:
#             child, cardinality = self.visit(triple_pattern)
#             scan_iterators.append(child)
#             cardinalities.extend(cardinality)
#         if self._dataset.force_order:
#             iterator = self.__build_naive_tree__(scan_iterators)
#         else:
#             iterator = self.__build_ascending_cardinalities_tree__(scan_iterators)
#         for values in node.mappings:
#             iterator = IndexJoinIterator(ValuesIterator(utils.format_solution_mappings(values.res)), iterator)
#         return iterator, cardinalities
#
#     def visit_scan(self, node: TriplePattern) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
#         triple_pattern = utils.format_triple_pattern(node, graph=self._default_graph)
#         if self._dataset.has_graph(triple_pattern['graph']):
#             iterator = ScanIterator(
#                 self._dataset.get_graph(triple_pattern['graph']),
#                 triple_pattern, as_of=self._as_of)
#         else:
#             iterator = EmptyIterator()
#         cardinality = {'pattern': triple_pattern, 'cardinality': iterator._cardinality}
#         return iterator, [cardinality]
#
#     def visit_insert(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
#         quads = utils.get_quads_from_update(node, self._default_graph)
#         return InsertOperator(quads, self._dataset), []
#
#     def visit_delete(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
#         quads = utils.get_quads_from_update(node, self._default_graph)
#         return DeleteOperator(quads, self._dataset), []
#
#     def visit_modify(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
#         consistency_level = "serializable"
#         if node.where.name == 'Join':
#             if node.where.p1.name == 'BGP' and len(node.where.p1.triples) == 0:
#                 bgp = node.where.p2
#             elif node.where.p2.name == 'BGP' and len(node.where.p2.triples) == 0:
#                 bgp = node.where.p1
#         if consistency_level == "serializable":
#             source, cardinalities = self.visit(bgp)
#             delete_templates = list()
#             insert_templates = list()
#             if node.delete is not None:
#                 delete_templates = utils.get_quads_from_update(
#                     node.delete, self._default_graph)
#             if node.insert is not None:
#                 insert_templates = utils.get_quads_from_update(
#                     node.insert, self._default_graph)
#             iterator = SerializableUpdate(
#                 self._dataset, source, delete_templates, insert_templates)
#             return iterator, cardinalities
#         else:
#             # Build the IF EXISTS style query from an UPDATE query with bounded
#             # RDF triples in the WHERE, INSERT and DELETE clause.
#             # - Assert that all RDF triples from the WHERE clause are bounded
#             for triple_pattern in node.where.triples:
#                 if utils.fully_bounded(triple_pattern):
#                     continue
#                 raise UnsupportedSPARQL(
#                     "The SaGe server only supports INSERT/DELETE DATA queries")
#             delete_templates = list()
#             insert_templates = list()
#             if node.delete is not None:
#                 delete_templates = utils.get_quads_from_update(
#                     node.delete, self._default_graph)
#             if node.insert is not None:
#                 insert_templates = utils.get_quads_from_update(
#                     node.insert, self._default_graph)
#             triples = list(utils.localize_triples(
#                 node.where.triples, [self._default_graph]))
#             if_exists_op = IfExistsOperator(triples, self._dataset, self._as_of)
#             delete_op = DeleteOperator(delete_templates, self._dataset)
#             insert_op = DeleteOperator(insert_templates, self._dataset)
#             iterator = UpdateSequenceOperator(if_exists_op, delete_op, insert_op)
#             return iterator, []
