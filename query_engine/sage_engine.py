# sage_engine.py
# Author: Thomas MINIER - MIT License 2017-2018
from asyncio import Queue, coroutine, get_event_loop, shield, wait_for
from asyncio import TimeoutError as asyncTimeoutError
from query_engine.iterators.projection import ProjectionIterator
from query_engine.iterators.union import BagUnionIterator
from query_engine.iterators.filter import FilterIterator
from query_engine.iterators.utils import IteratorExhausted
from query_engine.protobuf.iterators_pb2 import RootTree


async def executor(plan, queue):
    """Executor used to evaluated a plan under a time quota"""
    try:
        while plan.has_next():
            value = await plan.next()
            await shield(queue.put(value))
    except IteratorExhausted as e:
        pass


class SageEngine(object):
    """SaGe query engine, used to evaluated a preemptable physical query execution plan"""
    def __init__(self):
        super(SageEngine, self).__init__()

    def execute(self, plan, quota, filters=[]):
        """
            Execute a preemptable physical query execution plan under a time quota.

            Args:
                - plan [PreemptableIterator] - The root of the plan, i.e., a preemptable iterator
                - quota [float] - The time quota used for query execution

            Returns:
                A tuple (results, saved_plan, is_done) where:
                - results is a list of solution mappings found during query execution
                - saved_plan is the state of the plan saved using protocol-buffers
                - is_done is bolean set to True when the plan has completed query evalution, False otherwise
        """
        results = list()
        queue = Queue()
        loop = get_event_loop()
        query_done = False
        try:
            task = wait_for(executor(plan, queue), timeout=quota)
            loop.run_until_complete(task)
            query_done = True
        except asyncTimeoutError as e:
            pass
        finally:
            while not queue.empty():
                results.append(queue.get_nowait())
        root = RootTree()
        # source_field = plan.serialized_name() + '_source'
        # getattr(root, source_field).CopyFrom(self._source.save())
        if type(plan) is ProjectionIterator:
            root.proj_source.CopyFrom(plan.save())
        elif type(plan) is BagUnionIterator:
            root.union_source.CopyFrom(plan.save())
        elif type(plan) is FilterIterator:
            root.filter_source.CopyFrom(plan.save())
        return (results, root, query_done)
