# sage_engine.py
# Author: Thomas MINIER - MIT License 2017-2020
from time import time
from typing import Dict, List, Optional, Tuple

from sage.query_engine.exceptions import DeleteInsertConflict, TooManyResults, QuantumExhausted
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import RootTree

ExecutionResults = Tuple[List[Dict[str, str]], Optional[RootTree], bool, Optional[str]]


async def executor(pipeline: PreemptableIterator, results: list, context: dict) -> None:
    """Execute a pipeline of iterator under a time quantum.

    Args:
      * pipeline: Root of the pipeline of iterator.
      * results: List used to store query results.
      * context: Information about the query execution.

    Throws: Any exception raised during query execution.
    """
    while pipeline.has_next():
        value = await pipeline.next()
        if value is not None:
            results.append(value)
        if len(results) >= context['max_results']:
            raise TooManyResults()


class SageEngine(object):
    """SaGe query engine, used to evaluated a preemptable physical query execution plan"""

    def __init__(self):
        super(SageEngine, self).__init__()

    async def execute(self, plan: PreemptableIterator, context: dict) -> ExecutionResults:
        """Execute a preemptable physical query execution plan under a time quantum.

        Args:
          * plan: Root of the pipeline of iterator.
          * context: Information about the query execution.

        Returns: A tuple (``results``, ``saved_plan``, ``is_done``, ``abort_reason``) where:
          * ``results`` is a list of solution mappings found during query execution
          * ``saved_plan`` is the state of the plan saved using protocol-buffers
          * ``is_done`` is True when the plan has completed query evalution, False otherwise
          * ``abort_reason`` is True if the query was aborted due a to concurrency control issue

        Throws: Any exception raised during query execution.
        """
        results: List[Dict[str, str]] = list()
        query_done = False
        root = None
        abort_reason = None
        try:
            context['start_timestamp'] = time()
            await executor(plan, results, context)
            query_done = True
        except QuantumExhausted:
            pass
        except TooManyResults:
            pass
        except DeleteInsertConflict as err:
            abort_reason = str(err)
        # save the plan if query execution is not done yet and no abort has occurred
        if not query_done and abort_reason is None:
            root = RootTree()
            source_field = plan.serialized_name() + '_source'
            getattr(root, source_field).CopyFrom(plan.save())
        return (results, root, query_done, abort_reason)
