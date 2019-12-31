# sage_engine.py
# Author: Thomas MINIER - MIT License 2017-2020
from asyncio import Queue
from asyncio import TimeoutError as asyncTimeoutError
from asyncio import get_event_loop, wait_for
from math import inf
from typing import Dict, List, Optional, Tuple

from sage.query_engine.exceptions import DeleteInsertConflict, TooManyResults
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.primitives import PreemptiveLoop
from sage.query_engine.protobuf.iterators_pb2 import RootTree

ExecutionResults = Tuple[List[Dict[str, str]], Optional[RootTree], bool, Optional[str]]


async def executor(pipeline: PreemptableIterator, queue: Queue, limit: int) -> None:
    """Execute a pipeline of iterator under a time quantum.
    
    Args:
      * pipeline: Root of the pipeline of iterator.
      * queue: Async queue used to store query results.
      * limit: Maximum number of query results to fetch from the pipeline.
    
    Throws: Any exception raised during query execution.
    """
    try:
        with PreemptiveLoop() as loop:
            while pipeline.has_next():
                value = await pipeline.next()
                # discard null values
                if value is not None:
                    await queue.put(value)
                if queue.qsize() >= limit:
                    raise TooManyResults()
                await loop.tick()
    except StopAsyncIteration:
        pass


class SageEngine(object):
    """SaGe query engine, used to evaluated a preemptable physical query execution plan"""

    def __init__(self):
        super(SageEngine, self).__init__()

    async def execute(self, plan: PreemptableIterator, quantum: int, limit=inf) -> ExecutionResults:
        """Execute a preemptable physical query execution plan under a time quantum.

        Args:
          * plan: Root of the pipeline of iterator.
          * quantum: Time quantum used to execute the query.

        Returns: A tuple (``results``, ``saved_plan``, ``is_done``, ``abort_reason``) where:
          * ``results`` is a list of solution mappings found during query execution
          * ``saved_plan`` is the state of the plan saved using protocol-buffers
          * ``is_done`` is True when the plan has completed query evalution, False otherwise
          * ``abort_reason`` is True if the query was aborted due a to concurrency control issue
        
        Throws: Any exception raised during query execution.
        """
        results: List[Dict[str, str]] = list()
        queue = Queue()
        loop = get_event_loop()
        query_done = False
        root = None
        abort_reason = None
        try:
            await wait_for(executor(plan, queue, limit), timeout=quantum)
            # loop.run_until_complete(task)
            query_done = True
        except StopAsyncIteration:
            pass
        except asyncTimeoutError:
            pass
        except TooManyResults:
            pass
        except DeleteInsertConflict as err:
            abort_reason = str(err)
        finally:
            while not queue.empty():
                results.append(queue.get_nowait())
        # save the plan if query execution is not done yet and no abort has occurred
        if (not query_done) and abort_reason is None:
            root = RootTree()
            source_field = plan.serialized_name() + '_source'
            getattr(root, source_field).CopyFrom(plan.save())
        return (results, root, query_done, abort_reason)
