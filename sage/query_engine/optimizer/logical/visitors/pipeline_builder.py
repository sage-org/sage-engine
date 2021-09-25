import sage.query_engine.optimizer.utils as utils

from datetime import datetime
from typing import Optional, Dict, List
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


class PipelineBuilder(LogicalPlanVisitor):

    def __init__(
        self, dataset: Dataset, default_graph: str,
        context: dict, as_of: Optional[datetime] = None
    ):
        super().__init__()
        self._dataset = dataset
        self._default_graph = default_graph
        self._context = context
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
        scan_iterators = sorted(scan_iterators, key=lambda it: it.__len__())
        pipeline = scan_iterators.pop(0)
        variables = utils.get_vars(pipeline._pattern)
        while len(scan_iterators) > 0:
            next = self.__find_connected_pattern__(variables, scan_iterators)
            if next >= 0:
                scan_iterator = scan_iterators.pop(next)
            else:
                scan_iterator = scan_iterators.pop(0)
            variables = variables | utils.get_vars(scan_iterator._pattern)
            pipeline = IndexJoinIterator(pipeline, scan_iterator, self._context)
        return pipeline

    def __build_naive_tree__(
        self, scan_iterators: List[ScanIterator]
    ) -> PreemptableIterator:
        pipeline = scan_iterators.pop(0)
        while len(scan_iterators) > 0:
            pipeline = IndexJoinIterator(
                pipeline, scan_iterators.pop(0), self._context
            )
        return pipeline

    def __format_solution_mappings__(
        self, solution_mappings: List[Dict[RDFTerm, RDFTerm]]
    ) -> List[Dict[str, str]]:
        formated_solution_mappings = list()
        for solution_mapping in solution_mappings:
            formated_solution_mapping = dict()
            for variable, value in solution_mapping.items():
                variable = utils.format_term(variable)
                value = utils.format_term(value)
                formated_solution_mapping[variable] = value
            formated_solution_mappings.append(formated_solution_mapping)
        return formated_solution_mappings

    def __format_triple_pattern__(
        self, triple_pattern: TriplePattern
    ) -> Dict[str, str]:
        return {
            'subject': utils.format_term(triple_pattern[0]),
            'predicate': utils.format_term(triple_pattern[1]),
            'object': utils.format_term(triple_pattern[2]),
            'graph': self._default_graph
        }

    def visit_select_query(self, node: CompValue) -> PreemptableIterator:
        return self.visit(node.p)

    def visit_projection(self, node: CompValue) -> PreemptableIterator:
        projected_variables = list(map(lambda t: '?' + str(t), node.PV))
        return ProjectionIterator(
            self.visit(node.p), self._context, projected_variables
        )

    def visit_join(self, node: CompValue) -> PreemptableIterator:
        return IndexJoinIterator(
            self.visit(node.p1), self.visit(node.p2), self._context
        )

    def visit_union(self, node: CompValue) -> PreemptableIterator:
        return BagUnionIterator(
            self.visit(node.p1), self.visit(node.p2), self._context
        )

    def visit_filter(self, node: CompValue) -> PreemptableIterator:
        raw_expression = ExpressionStringifier().visit(node.expr)
        return FilterIterator(
            self.visit(node.p), raw_expression, node.expr, self._context
        )

    def visit_to_multiset(self, node: CompValue) -> PreemptableIterator:
        return self.visit(node.p)

    def visit_values(self, node: CompValue) -> PreemptableIterator:
        values = self.__format_solution_mappings__(node.res)
        return ValuesIterator(values)

    def visit_bgp(self, node: CompValue) -> PreemptableIterator:
        scan_iterators = list()
        for triple_pattern in node.triples:
            scan_iterators.append(self.visit(triple_pattern))
        if self._dataset.enable_join_ordering:
            return self.__build_ascending_cardinalities_tree__(scan_iterators)
        else:
            return self.__build_naive_tree__(scan_iterators)

    def visit_scan(self, node: TriplePattern) -> PreemptableIterator:
        triple_pattern = self.__format_triple_pattern__(node)
        if self._dataset.has_graph(triple_pattern['graph']):
            return ScanIterator(
                self._dataset.get_graph(triple_pattern['graph']),
                triple_pattern, self._context, as_of=self._as_of
            )
        else:
            return EmptyIterator()

    def visit_insert(self, node: CompValue) -> PreemptableIterator:
        quads = utils.get_quads_from_update(node, self._default_graph)
        return InsertOperator(quads, self._dataset)

    def visit_delete(self, node: CompValue) -> PreemptableIterator:
        quads = utils.get_quads_from_update(node, self._default_graph)
        return DeleteOperator(quads, self._dataset)

    def visit_modify(self, node: CompValue) -> PreemptableIterator:
        if node.where.name == 'Join':
            if node.where.p1.name == 'BGP' and len(node.where.p1.triples) == 0:
                bgp = node.where.p2
            elif node.where.p2.name == 'BGP' and len(node.where.p2.triples) == 0:
                bgp = node.where.p1
        if self._context['consistency-level'] == "serializable":
            source = self.visit(bgp)
            delete_templates = list()
            insert_templates = list()
            if node.delete is not None:
                delete_templates = utils.get_quads_from_update(
                    node.delete, self._default_graph
                )
            if node.insert is not None:
                insert_templates = utils.get_quads_from_update(
                    node.insert, self._default_graph
                )
            return SerializableUpdate(
                self._dataset, source, delete_templates, insert_templates
            )
        else:
            # Build the IF EXISTS style query from an UPDATE query with bounded
            # RDF triples in the WHERE, INSERT and DELETE clause.
            # - Assert that all RDF triples from the WHERE clause are bounded
            for triple_pattern in node.where.triples:
                if utils.fully_bounded(triple_pattern):
                    continue
                raise UnsupportedSPARQL(
                    "The SaGe server only supports INSERT/DELETE DATA queries"
                )
            delete_templates = list()
            insert_templates = list()
            if node.delete is not None:
                delete_templates = utils.get_quads_from_update(
                    node.delete, self._default_graph
                )
            if node.insert is not None:
                insert_templates = utils.get_quads_from_update(
                    node.insert, self._default_graph
                )
            triples = list(utils.localize_triples(
                node.where.triples, [self._default_graph])
            )
            if_exists_op = IfExistsOperator(triples, self._dataset, self._as_of)
            delete_op = DeleteOperator(delete_templates, self._dataset)
            insert_op = DeleteOperator(insert_templates, self._dataset)
            return UpdateSequenceOperator(if_exists_op, delete_op, insert_op)
