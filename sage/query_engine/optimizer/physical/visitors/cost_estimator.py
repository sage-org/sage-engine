import sage.database.voids.void as void

from typing import Dict, Any

from sage.query_engine.optimizer.physical.plan_visitor import PhysicalPlanVisitor
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator


class CostEstimartor(PhysicalPlanVisitor):

    def __update_attributes__(
        self, context: Dict[str, Any], attribute: str, distinct_values: int
    ) -> None:
        if not attribute.startswith('?'):
            return None
        if attribute not in context['attributes']:
            context['attributes'][attribute] = []
        context['attributes'][attribute].append(distinct_values)

    def visit(
        self, node: PreemptableIterator, context: Dict[str, Any] = {}
    ) -> Any:
        if 'attributes' not in context:
            context['attributes'] = dict()
        if 'input-size' not in context:
            context['input-size'] = 1
        # context['height'] = 1
        return super().visit(node, context=context)

    def visit_projection(
        self, node: PreemptableIterator, context: Dict[str, Any] = {}
    ) -> float:
        return self.visit(node._source, context=context)

    def visit_values(
        self, node: PreemptableIterator, context: Dict[str, Any] = {}
    ) -> float:
        # estimate the cardinality of the VALUES iterator
        input_size = context['input-size']
        output_size = input_size * len(node._values)
        context['input-size'] = output_size
        print(
            f'Card({node._values}) = {input_size} x {len(node._values)} = ' +
            f'{output_size}'
        )
        return output_size

    def visit_filter(
        self, node: PreemptableIterator, context: Dict[str, Any] = {}
    ) -> float:
        # estimate the selectivity of the FILTER iterator
        if node._produced == 0:
            selectivity = 1 / 3
        else:
            selectivity = node._produced / node._consumed
        # estimate the cardinality of the FILTER iterator
        input_size = self.visit(node._source, context=context)
        output_size = input_size * selectivity
        context['input-size'] = output_size
        print(
            f'Card({node._raw_expression}) = ' +
            f'{input_size} x {selectivity} = {output_size}'
        )
        return input_size

    def visit_join(
        self, node: PreemptableIterator, context: Dict[str, Any] = {}
    ) -> float:
        left = self.visit(node._left, context=context)
        right = self.visit(node._right, context=context)
        return left + right

    def visit_union(
        self, node: PreemptableIterator, context: Dict[str, Any] = {}
    ) -> float:
        left = self.visit(node._left, context=context)
        right = self.visit(node._right, context=context)
        return left + right

    def visit_scan(
        self, node: PreemptableIterator, context: Dict[str, Any] = {}
    ) -> float:
        subject = node._pattern['subject']
        predicate = node._pattern['predicate']
        object = node._pattern['object']
        graph = node._pattern['graph']
        # update the number of distinct values for each joined attributes
        self.__update_attributes__(
            context, subject, void.count_distinct_subjects(graph, predicate)
        )
        self.__update_attributes__(
            context, object, void.count_distinct_objects(graph, predicate)
        )
        # estimate the cardinality of the SCAN iterator
        input_size = context['input-size']
        if node._pattern_produced == 0:
            distinct_values = 1
            if subject.startswith('?'):
                if len(context['attributes'][subject]) > 1:
                    max_value = max(context['attributes'][subject])
                    context['attributes'][subject].remove(max_value)
                    distinct_values *= max_value
            if object.startswith('?'):
                if len(context['attributes'][object]) > 1:
                    max_value = max(context['attributes'][object])
                    context['attributes'][object].remove(max_value)
                    distinct_values *= max_value
            cardinality = node._pattern_cardinality
            selectivity = cardinality / distinct_values
            output_size = input_size * selectivity
            print(
                f'C_out({node._pattern}, runtime=False) = ' +
                f'{input_size} x ({cardinality} / {distinct_values}) = ' +
                f'{output_size}'
            )
        else:
            cardinality = max(
                max(node._cardinality, node._cumulative_cardinality),
                node._pattern_produced
            )
            stages = max(1, node._stages)
            selectivity = cardinality / stages
            output_size = input_size * selectivity
            print(
                f'C_out({node._pattern}, runtime=True) = ' +
                f'{input_size} x ({cardinality} / {stages}) = {output_size}'
            )
        context['input-size'] = output_size
        return output_size
