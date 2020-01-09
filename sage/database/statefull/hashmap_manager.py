# hashmap_manager.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict

from sage.database.statefull.statefull_manager import StatefullManager


class HashMapManager(StatefullManager):
    """A HashMapManager stores saved plans in main memory using a simple HashMap"""

    def __init__(self):
        super(HashMapManager, self).__init__()
        self._plans = dict()

    def get_plan(self, plan_id: str) -> str:
        """Get a saved plan by ID.
        
        Argument: ID of the saved plan to retrieve.

        Returns: The saved plan corresponding to the input ID.
        """
        return self._plans[plan_id]

    def save_plan(self, id: str, plan: str) -> None:
        """Store a saved plan by ID.
        
        Args:
          * id: Unique ID associated with the saved plan.
          * plan: Plan to save.
        """
        self._plans[id] = plan

    def delete_plan(self, plan_id: str) -> None:
        """Delete a saved plan by ID.
        
        Argument: ID of the saved plan to delete.
        """
        del self._plans[plan_id]

    def from_config(config: Dict[str, str]):
        """Build a StatefullManager from a config dictionnary"""
        return HashMapManager()
