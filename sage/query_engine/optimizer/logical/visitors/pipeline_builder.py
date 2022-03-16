import sage.query_engine.optimizer.utils as utils

from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from rdflib.plugins.sparql.parserutils import CompValue, Expr

from sage.query_engine.optimizer.logical.plan_visitor import LogicalPlanVisitor, RDFTerm, TriplePattern
from sage.database.core.dataset import Dataset
from sage.query_engine.exceptions import UnsupportedSPARQL
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.values import ValuesIterator
from sage.query_engine.iterators.utils import EmptyIterator
from sage.query_engine.update.delete import DeleteOperator
from sage.query_engine.update.if_exists import IfExistsOperator
from sage.query_engine.update.insert import InsertOperator
from sage.query_engine.update.serializable import SerializableUpdate
from sage.query_engine.update.update_sequence import UpdateSequenceOperator


class ExpressionStringifier(LogicalPlanVisitor):

    def __init__(self):
        super().__init__()

    def visit_rdfterm(self, node: RDFTerm) -> str:
        return utils.format_term(node)

    def visit_conditional_and_expression(self, node: Expr) -> str:
        expression = self.visit(node.expr)
        for other in node.other:
            expression = f'({expression} && {self.visit(other)})'
        return expression

    def visit_conditional_or_expression(self, node: Expr) -> str:
        expression = self.visit(node.expr)
        for other in node.other:
            expression = f'({expression} || {self.visit(other)})'
        return expression

    def visit_regex_expression(self, node: Expr) -> str:
        return f'regex({self.visit(node.text)}, {self.visit(node.pattern)})'

    def visit_relational_expression(self, node: Expr) -> str:
        return f'({self.visit(node.expr)} {node.op} {self.visit(node.other)})'

    def visit_unary_not_expression(self, node: Expr) -> str:
        return f'!({self.visit(node.expr)})'

    def visit_str_expression(self, node: Expr) -> str:
        return f'str({self.visit(node.arg)})'

    def visit_additive_expression(self, node: Expr) -> str:
        expression = self.visit(node.expr)
        for index, operator in enumerate(node.op):
            expression += f' {operator} {self.visit(node.other[index])}'
        return f'({expression})'


class PipelineBuilder(LogicalPlanVisitor):

    def __init__(
        self, dataset: Dataset, default_graph: str,
        as_of: Optional[datetime] = None
    ):
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
            key=lambda it: (it.__len__(), it._pattern['predicate']))
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

    def visit_select_query(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        return self.visit(node.p)

    def visit_projection(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        projected_variables = list(map(lambda t: '?' + str(t), node.PV))
        child, cardinalities = self.visit(node.p)
        return ProjectionIterator(child, projected_variables), cardinalities

    def visit_join(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        left_child, left_cardinalities = self.visit(node.p1)
        right_child, right_cardinalities = self.visit(node.p2)
        cardinalities = left_cardinalities + right_cardinalities
        return IndexJoinIterator(left_child, right_child), cardinalities

    def visit_union(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        left_child, left_cardinalities = self.visit(node.p1)
        right_child, right_cardinalities = self.visit(node.p2)
        cardinalities = left_cardinalities + right_cardinalities
        return BagUnionIterator(left_child, right_child), cardinalities

    def visit_filter(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        raw_expression = ExpressionStringifier().visit(node.expr)
        child, cardinalities = self.visit(node.p)
        return FilterIterator(child, raw_expression, node.expr), cardinalities

    def visit_to_multiset(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        return self.visit(node.p)

    def visit_values(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        return ValuesIterator(utils.format_solution_mappings(node.res)), []

    def visit_bgp(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        scan_iterators = list()
        cardinalities = list()
        for triple_pattern in node.triples:
            child, cardinality = self.visit(triple_pattern)
            scan_iterators.append(child)
            cardinalities.extend(cardinality)
        if self._dataset.force_order:
            iterator = self.__build_naive_tree__(scan_iterators)
        else:
            iterator = self.__build_ascending_cardinalities_tree__(scan_iterators)
        for values in node.mappings:
            iterator = IndexJoinIterator(ValuesIterator(utils.format_solution_mappings(values.res)), iterator)
        return iterator, cardinalities

    def visit_scan(self, node: TriplePattern) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        triple_pattern = utils.format_triple_pattern(node, graph=self._default_graph)
        if self._dataset.has_graph(triple_pattern['graph']):
            iterator = ScanIterator(
                self._dataset.get_graph(triple_pattern['graph']),
                triple_pattern, as_of=self._as_of)
        else:
            iterator = EmptyIterator()
        cardinality = {'pattern': triple_pattern, 'cardinality': iterator.__len__()}
        return iterator, [cardinality]

    def visit_insert(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        quads = utils.get_quads_from_update(node, self._default_graph)
        return InsertOperator(quads, self._dataset), []

    def visit_delete(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        quads = utils.get_quads_from_update(node, self._default_graph)
        return DeleteOperator(quads, self._dataset), []

    def visit_modify(self, node: CompValue) -> Tuple[PreemptableIterator, List[Dict[str, Any]]]:
        consistency_level = "serializable"
        if node.where.name == 'Join':
            if node.where.p1.name == 'BGP' and len(node.where.p1.triples) == 0:
                bgp = node.where.p2
            elif node.where.p2.name == 'BGP' and len(node.where.p2.triples) == 0:
                bgp = node.where.p1
        if consistency_level == "serializable":
            source, cardinalities = self.visit(bgp)
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
