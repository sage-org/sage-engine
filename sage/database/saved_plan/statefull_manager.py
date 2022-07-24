import tempfile
import os

from sage.singleton import Singleton
from sage.database.saved_plan.saved_plan_manager import SavedPlanManager
from sage.database.saved_plan.utils import encode_saved_plan, decode_saved_plan
from sage.query_engine.types import SavedPlan


class StatefullManager(SavedPlanManager, metaclass=Singleton):
    """
    A StatefullManager stores saved plans on disk. Saved plans are stored on
    disk so that the implementation works in a multiprocessing configuration.
    """

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
        with open(plan_id, "r") as queryfile:
            saved_plan = decode_saved_plan(queryfile.read())
        os.remove(plan_id)
        return saved_plan

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
        tf_query = tempfile.NamedTemporaryFile(delete=False)
        tf_query.write(encode_saved_plan(saved_plan).encode("utf-8"))
        tf_query.flush()
        tf_query.close()
        return tf_query.name

    def delete_plan(self, plan_id: str) -> None:
        """
        Deletes the saved physical plan corresponding to the given ID.

        Parameters
        ----------
        plan_id: str
            ID of the saved physical plan to delete.
        """
        del self._plans[plan_id]

    def close(self) -> None:
        """
        Free resources used to store saved plans.
        """
        pass
