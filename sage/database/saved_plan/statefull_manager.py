from uuid import uuid4

from sage.singleton import Singleton
from sage.database.saved_plan.saved_plan_manager import SavedPlanManager
from sage.database.saved_plan.utils import encode_saved_plan, decode_saved_plan
from sage.query_engine.types import SavedPlan


class StatefullManager(SavedPlanManager, metaclass=Singleton):
    """
    A HashMapManager stores saved plans in main memory using a simple HashMap.

    NOTE: This implementation does not work in a multiprocessing setup.
    """

    def __init__(self):
        self._plans = dict()

    def get_plan(self, plan_id: str) -> SavedPlan:
        """
        Returns the saved physical plan corresponding to the given ID.

        Parameters
        ----------
        plan_id: str
            ID of the saved physical plan to retrieve.

        Returns
        -------
        SavedPlan
            The saved physical plan corresponding to the given ID.
        """
        saved_plan = self._plans[plan_id]
        return decode_saved_plan(saved_plan)

    def save_plan(self, saved_plan: SavedPlan) -> str:
        """
        Stores a saved physical plan on the server.

        Parameters
        ----------
        saved_plan: SavedPlan
            A saved physical plan to store.

        Returns
        -------
        str
            The ID of the saved physical plan.
        """
        plan_id = str(uuid4())
        self._plans[plan_id] = encode_saved_plan(saved_plan)
        return plan_id

    def delete_plan(self, plan_id: str) -> None:
        """
        Deletes the saved physical plan corresponding to the given ID.

        Parameters
        ----------
        plan_id: str
            ID of the saved physical plan to delete.
        """
        del self._plans[plan_id]
