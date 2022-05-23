# preemptable_iterator.py
# Author: Thomas MINIER - MIT License 2017-2020
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Set


class PreemptableIterator(ABC):
    """An abstract class for a preemptable iterator"""

    @abstractmethod
    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        pass

    @abstractmethod
    def explain(self, height: int = 0, step: int = 3) -> None:
        """Print a description of the iterator"""
        pass

    @abstractmethod
    def variables(self, include_values: bool = False) -> Set[str]:
        """Return the domain of the iterator"""
        pass

    @abstractmethod
    def next_stage(self, mappings: Dict[str, str]):
        """Propagate mappings to the bottom of the pipeline in order to compute nested loop joins"""
        pass

    @abstractmethod
    async def next(self, context: Dict[str, Any] = dict()) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.
        """
        pass

    @abstractmethod
    def update_coverage(self, context: Dict[str, Any] = {}) -> float:
        """Compute and update operators progression.

        This function assumes that only nested loop joins are used.

        Returns: The coverage of the query for the given plan.
        """
        pass

    @abstractmethod
    def update_cost(self, context: Dict[str, Any] = {}) -> float:
        """Compute and update operators cost.

        This function assumes that only nested loop joins are used.

        Returns: The cost of the query for the given plan.
        """
        pass

    @abstractmethod
    def save(self) -> Any:
        """Save and serialize the iterator as a Protobuf message"""
        pass
