# sage_engine.py
# Author: Thomas MINIER - MIT License 2017-2018
from asyncio import Queue, get_event_loop, shield, wait_for
from asyncio import TimeoutError as asyncTimeoutError


async def executor(plan, queue):
    """Executor used to evaluated a plan under a time quota"""
    while plan.has_next():
        value = await plan.next()
        await shield(queue.put(value))


class SageEngine(object):
    """SaGe query engine, used to evaluated a preemptable physical query execution plan"""
    def __init__(self):
        super(SageEngine, self).__init__()

    def execute(self, plan, quota):
        results = list()
        queue = Queue()
        loop = get_event_loop()
        try:
            task = wait_for(executor(plan, queue), timeout=quota)
            loop.run_until_complete(task)
        except asyncTimeoutError as e:
            pass
        finally:
            while not queue.empty():
                results.append(queue.get_nowait())
        return (results, plan.save())
