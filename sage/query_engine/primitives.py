# primitives.py
# Author: Thomas MINIER - MIT License 2017-2020
from asyncio import sleep

class PreemptiveLoop(object):
    """Utility context manager to run loops in a preemptive env. with asyncio.

    Basically, this class allows one to trigger `asyncio.sleep(0)` at fixed interval,
    to yield control to the scheduler in loops that run in an event loop.
    Otherwise, such loops never yield back to the scheduler, which breaks the Round-Robin
    scheduling algorithm of the preemptive Web server.
    Yielding is performed using the `tick()` method.
    
    However, a call to `asyncio.sleep(0)` costs a lot in term of performance, so we only
    trigger every `threshold` calls to the tick() method. 
    **This parameter defaults to 50 cycles**.
    So, to optimize query execution performance, on need to minimize 
    the use of loops in the pipeline of iterators and optimize the `threshold` value.
    
    Argument: Number of ticks between each call to asyncio.sleep

    Example:
      >>> with PreemptiveLoop() as loop:
      >>>   for i in range(10):
      >>>     print(i)
      >>>     await loop.tick()
    """

    def __init__(self, threshold=50):
        super(PreemptiveLoop, self).__init__()
        self._cpt = 0
        self._threshold = threshold

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    async def tick(self) -> None:
        """Move to the next iteration and allow interruption of the current loop (if required)"""
        self._cpt += 1
        # WARNING: await sleep(0) cost a lot, so we only trigger it at fixed interval.
        # Additionnaly, there may be other call to tick() in other iterators in the pipeline.
        if self._cpt > self._threshold:
            self._cpt = 0
            await sleep(0)
