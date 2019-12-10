# primitives.py
# Author: Thomas MINIER - MIT License 2017-2020
from asyncio import sleep

class PreemptiveLoop(object):
    """Utility context manager to run loops in a preemptive env. with asyncio"""

    def __init__(self, threshold=50):
        super(PreemptiveLoop, self).__init__()
        self._cpt = 0
        self._threshold = threshold

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    async def tick(self):
        """Move to the next iteration and allow interruption of the current loop."""
        self._cpt += 1
        # WARNING: await sleep(0) cost a lot, so we only trigger it every 50 cycle.
        # Additionnaly, there may be other call to await sleep(0) in index join in the pipeline.
        if self._cpt > self._threshold:
            self._cpt = 0
            await sleep(0)
