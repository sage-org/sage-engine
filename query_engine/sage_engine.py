# sage_engine.py
# Author: Thomas MINIER - MIT License 2017-2018
from asyncio import Queue, get_event_loop, shield, wait_for, sleep
from asyncio import TimeoutError as asyncTimeoutError
from query_engine.iterators.projection import ProjectionIterator
from query_engine.iterators.union import BagUnionIterator
from query_engine.iterators.filter import FilterIterator
from query_engine.iterators.utils import IteratorExhausted
from query_engine.protobuf.iterators_pb2 import RootTree
from math import inf


class TooManyResults(Exception):
    """
        Exception raised when the max. number of results for a query execution
        has been exceeded
    """
    pass


async def executor(plan, queue, limit):
    """Executor used to evaluated a plan under a time quota"""
    try:
        print('before plan has next')
        print(str(plan.has_next()))
        while plan.has_next():
            print('av')
            value = await plan.next()
            #print('ap' + str(value))
            if value is not None:
                await shield(queue.put(value))
                if queue.qsize() >= limit:
                    raise TooManyResults()
            await sleep(0)
    except IteratorExhausted:
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
                A tuple (``results``, ``saved_plan``, ``is_done``) where:
                - ``results`` is a list of solution mappings found during query execution
                - ``saved_plan`` is the state of the plan saved using protocol-buffers
                - ``is_done`` is True when the plan has completed query evalution, False otherwise
        """
        print('start execute sage_engine')
        results = list()
        queue = Queue()
        loop = get_event_loop()
        query_done = False
        try:
            print('task wait for')
            task = wait_for(executor(plan, queue, limit), timeout=quota)
            print('task done')
            loop.run_until_complete(task)
            query_done = True
        except asyncTimeoutError:
            pass
        except TooManyResults:
            pass
        finally:
            while not queue.empty():
                print('dans le while de queue')
                results.append(queue.get_nowait())
                # print(queue)
                # print(queue.empty())
                # print(results)
        print('avant de boire')
        root = RootTree()
        print('le root boit de l eau')
        # source_field = plan.serialized_name() + '_source'
        # getattr(root, source_field).CopyFrom(self._source.save())
        if type(plan) is ProjectionIterator:
            print('plan')
            root.proj_source.CopyFrom(plan.save())
        elif type(plan) is BagUnionIterator:
            print('plan')
            root.union_source.CopyFrom(plan.save())
        elif type(plan) is FilterIterator:
            print('plan')
            root.filter_source.CopyFrom(plan.save())

        print('results :' + str(results))
        return (results, root, query_done)
