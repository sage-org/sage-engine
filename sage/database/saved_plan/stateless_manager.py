from sage.database.saved_plan.saved_plan_manager import SavedPlanManager
from sage.database.saved_plan.utils import encode_saved_plan, decode_saved_plan
from sage.query_engine.types import SavedPlan


class StatelessManager(SavedPlanManager):
    """
    A StatelessManager is a class that encodes and decodes saved physical query
    plans.
    """

    def get_plan(self, plan_id: str) -> SavedPlan:
        """
        Returns the saved physical plan corresponding to the given ID.

        When using the StatelessManager, the plan_id is just an encoded saved
        plan. Consequently, this method simply decodes the saved plan.

        Parameters
        ----------
        plan_id: str
            ID of the saved physical plan to retrieve.

        Returns
        -------
        SavedPlan
            The saved physical plan corresponding to the given ID.
        """
        return decode_saved_plan(plan_id)

    def save_plan(self, saved_plan: SavedPlan) -> str:
        """
        Stores a saved physical plan on the server.

        When using the StatelessManager, the saved plan is not stored on the
        server, but sent to the client. Consequently, this method simply encodes
        the saved plan.

        Parameters
        ----------
        saved_plan: SavedPlan
            A saved physical plan to store.

        Returns
        -------
        str
            The ID of the saved physical plan.
        """
        return encode_saved_plan(saved_plan)

    def delete_plan(self, plan_id: str) -> None:
        """
        Deletes the saved physical plan corresponding to the given ID.

        When using the StatelessManager, saved plans are not stored on the
        server. Consequently, this method do nothing.

        Parameters
        ----------
        plan_id: str
            ID of the saved physical plan to delete.
        """
        pass
