# statefull_manager.py
# Author: Thomas MINIER - MIT License 2017-2020
from abc import ABC, abstractmethod
from typing import Dict


class StatefullManager(ABC):
    """A StatefullManager is an abstract class for storing saved SPARQL query execution plans"""

    @abstractmethod
    def get_plan(self, plan_id: str) -> str:
        """Get a saved plan by ID.
        
        Argument: ID of the saved plan to retrieve.

        Returns: The saved plan corresponding to the input ID.
        """
        pass

    @abstractmethod
    def save_plan(self, id, plan: str) -> None:
        """Store a saved plan by ID.
        
        Args:
          * id: Unique ID associated with the saved plan.
          * plan: Plan to save.
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
        """Build a StatefullManager from a config dictionnary"""
        pass

    def open(self) -> None:
        """Open the StatefullManager connection"""
        pass

    def close(self) -> None:
        """Close the StatefullManager connection"""
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
