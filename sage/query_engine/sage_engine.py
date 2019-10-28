# sage_engine.py
# Author: Thomas MINIER - MIT License 2017-2018
import uvloop
from asyncio import Queue, get_event_loop, wait_for, set_event_loop_policy
from asyncio import TimeoutError as asyncTimeoutError
from sage.query_engine.primitives import PreemptiveLoop
from sage.query_engine.iterators.utils import IteratorExhausted
from sage.query_engine.exceptions import DeleteInsertConflict, TooManyResults
from sage.query_engine.protobuf.iterators_pb2 import RootTree
from math import inf

set_event_loop_policy(uvloop.EventLoopPolicy())


async def executor(plan, queue, limit):
    """Executor used to evaluated a plan under a time quota"""
    try:
        with PreemptiveLoop() as loop:
            while plan.has_next():
                value = await plan.next()
                # discard null values
                if value is not None:
                    await queue.put(value)
                if queue.qsize() >= limit:
                    raise TooManyResults()
                await loop.tick()
    except IteratorExhausted:
        pass
    except StopIteration:
        pass


class SageEngine(object):
    """SaGe query engine, used to evaluated a preemptable physical query execution plan"""

    def __init__(self):
        super(SageEngine, self).__init__()

    def execute(self, plan, quota, limit=inf):
        """
            Execute a preemptable physical query execution plan under a time quota.

            Args:
                - plan :class:`.PreemptableIterator` - The root of the plan
                - quota ``float`` - The time quota used for query execution

            Returns:
                A tuple (``results``, ``saved_plan``, ``is_done``, ``abort_reason``) where:
                - ``results`` is a list of solution mappings found during query execution
                - ``saved_plan`` is the state of the plan saved using protocol-buffers
                - ``is_done`` is True when the plan has completed query evalution, False otherwise
                - ``abort_reason`` is True if the query was aborted due a to concurrency control issue
        """
        results = list()
        queue = Queue()
        loop = get_event_loop()
        query_done = False
        root = None
        abort_reason = None
        try:
            task = wait_for(executor(plan, queue, limit), timeout=quota)
            loop.run_until_complete(task)
            query_done = True
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
