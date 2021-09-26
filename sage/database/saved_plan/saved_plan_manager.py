# statefull_manager.py
# Author: Thomas MINIER - MIT License 2017-2020
from abc import ABC, abstractmethod
from typing import Dict

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator


class SavedPlanManager(ABC):
    """A SavedPlanManager is an abstract class for storing saved SPARQL query execution plans"""

    @abstractmethod
    def get_plan(self, plan_id: str, dataset: Dataset) -> PreemptableIterator:
        """Get a saved plan by ID.

        Args:
          * ID of the saved plan to retrieve.
          * RDF dataset on which the query is executed.

        Returns: The saved plan corresponding to the input ID.
        """
        pass

    @abstractmethod
    def save_plan(self, plan: PreemptableIterator) -> str:
        """Store a saved plan.

        Argument: Plan to save.

        Returns: The ID of the saved plan.
        """
        pass

    @abstractmethod
    def delete_plan(self, plan_id: str) -> None:
        """Delete a saved plan by ID.

        Argument: ID of the saved plan to delete.
        """
        pass

    @abstractmethod
    def from_config(config: Dict[str, str]):
        """Build a SavedPlanManager from a config dictionnary"""
        pass

    def open(self) -> None:
        """Open the SavedPlanManager connection"""
        pass

    def close(self) -> None:
        """Close the SavedPlanManager connection"""
        pass

    def __enter__(self):
        """Implementation of the __enter__ method from the context manager spec"""
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        """Implementation of the __close__ method from the context manager spec"""
        self.close()

    def __del__(self):
        """Destructor"""
        self.close()
