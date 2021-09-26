# sage_engine.py
# Author: Thomas MINIER - MIT License 2017-2020
from time import time
from typing import Dict, List, Optional, Tuple

from sage.query_engine.exceptions import DeleteInsertConflict, TooManyResults, QuantumExhausted
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator

ExecutionResults = Tuple[List[Dict[str, str]], bool, Optional[str]]


async def executor(
    pipeline: PreemptableIterator, results: list, quota: int, max_results: int
) -> None:
    """Execute a pipeline of iterator under a time quantum.

    Args:
      * pipeline: Root of the pipeline of iterator.
      * results: List used to store query results.
      * quota: Duration of a quantum.
      * max_results: Maximum number of results that can be returned per quantum.

    Throws: Any exception raised during query execution.
    """
    context = {'quota': quota, 'start_timestamp': time()}
    value = await pipeline.next(context=context)
    while value is not None:
        results.append(value)
        if len(results) >= max_results:
            raise TooManyResults()
        value = await pipeline.next(context=context)


class SageEngine(object):
    """SaGe query engine, used to evaluated a preemptable physical query execution plan"""

    def __init__(self):
        super(SageEngine, self).__init__()

    async def execute(
        self, plan: PreemptableIterator, quota: int = 75, max_results: int = 10000
    ) -> ExecutionResults:
        """Execute a preemptable physical query execution plan under a time quantum.

        Args:
          * plan: Root of the pipeline of iterator.
          * quota: Duration of a quantum.
          * max_results: Maximum number of results that can be returned per quantum.

        Returns: A tuple (``results``, ``saved_plan``, ``is_done``, ``abort_reason``) where:
          * ``results`` is a list of solution mappings found during query execution
          * ``is_done`` is True when the plan has completed query evalution, False otherwise
          * ``abort_reason`` is True if the query was aborted due a to concurrency control issue

        Throws: Any exception raised during query execution.
        """
        results: List[Dict[str, str]] = list()
        query_done = False
        abort_reason = None
        try:
            await executor(plan, results, quota, max_results)
            query_done = True
        except QuantumExhausted:
            pass
        except TooManyResults:
            pass
        except DeleteInsertConflict as err:
            abort_reason = str(err)
        return (results, query_done, abort_reason)
