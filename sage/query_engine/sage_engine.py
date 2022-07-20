from time import time
from datetime import datetime
from typing import List, Optional, Tuple, Dict, Any

from sage.database.core.dataset import Dataset
from sage.query_engine.types import QueryContext, Mappings, SavedPlan
from sage.query_engine.optimizer.parser import Parser
from sage.query_engine.optimizer.optimizer import Optimizer
from sage.query_engine.exceptions import (
    DeleteInsertConflict,
    TooManyResults,
    QuantumExhausted,
    TOPKLimitReached)
from sage.query_engine.iterators.loader import load
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import RootTree


async def executor(
    pipeline: PreemptableIterator, solutions: List[Mappings],
    context: QueryContext = {}
) -> None:
    """
    Executes a pipeline of iterators under a time quantum.

    Parameters
    ----------
    pipeline: PreemptableIterator
        Root of the pipeline of iterator.
    solutions: List[Mappings]
        The solutions mappings of the query.
    context: QueryContext
        Global variables specific to the execution of the query.

    Raises
    ------
        Any exception raised during query execution.
    """
    dataset = Dataset()
    try:
        mappings = await pipeline.next(context=context)
        while mappings is not None:
            solutions.append(mappings)
            if len(solutions) >= dataset.max_results:
                raise TooManyResults()
            mappings = await pipeline.next(context=context)
    except (QuantumExhausted, TOPKLimitReached) as error:
        mappings = pipeline.pop(context=context)
        while mappings is not None:
            solutions.append(mappings)
            mappings = pipeline.pop(context=context)
        raise error


class SageEngine():
    """
    SaGe query engine used to evaluate SPARQL queries following the Web
    preemption model.

    NOTE: In the current implementation, there may be a problem with the
    transactions (for UPDATE queries). Taking into account transactions is on
    the TODO list.
    """

    async def execute(
        self, query: str, saved_plan: Optional[SavedPlan],
        default_graph_uri: str, context: QueryContext = {}
    ) -> Tuple[List[Mappings], Optional[SavedPlan], Dict[str, Any]]:
        """
        Executes a SPARQL query using the Web preemption model.

        Parameters
        ----------
        query: str
            SPARQL query to execute.
        saved_plan: None | SavedPlan
            The saved plan of the SPARQL query, or None if the query needs to be
            executed from the beginning.
        default_graph_uri: str
            URI of the default RDF graph to use.
        context: QueryContext
            Global variables specific to the execution of the query.

        Returns
        -------
        Tuple[List[Mappings], None | SavedPlan, Dict[str, Any]]
            A tuple (solutions, saved_plan, statistics) where:
                - solutions: The solutions found during query execution.
                - saved_plan: None if the query is complete, the saved plan of
                  the query otherwise.
                - statistics: Some statistics collected during query execution.

        Raises
        ------
            Any exception raised during query execution.
        """

        context["default_graph_uri"] = default_graph_uri
        context["snapshot"] = datetime.now()

        loading_start = time()
        if saved_plan is not None:
            plan = load(saved_plan, context=context)
        else:
            logical_plan = Parser.parse(query)
            plan = Optimizer.get_default().optimize(logical_plan, context=context)
        loading_time = (time() - loading_start) * 1000

        solutions = []
        query_done = False
        abort_reason = None
        try:
            context["timestamp"] = time()
            await executor(plan, solutions, context=context)
            query_done = True
        except (QuantumExhausted, TOPKLimitReached, TooManyResults):
            pass
        except DeleteInsertConflict as error:
            abort_reason = str(error)

        export_start = time()
        if not query_done and abort_reason is None:
            saved_plan = RootTree()
            getattr(saved_plan, f"{plan.name}_source").CopyFrom(plan.save())
        else:
            saved_plan = None
        export_time = (time() - export_start) * 1000

        stats = {"import": loading_time, "export": export_time}

        return solutions, saved_plan, stats
