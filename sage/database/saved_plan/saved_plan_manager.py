from abc import ABC, abstractmethod

from sage.query_engine.types import SavedPlan


class SavedPlanManager(ABC):
    """
    A SavedPlanManager is an abstract class for storing saved SPARQL query
    execution plans.
    """

    @abstractmethod
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
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def delete_plan(self, plan_id: str) -> None:
        """
        Deletes the saved physical plan corresponding to the given ID.

        Parameters
        ----------
        plan_id: str
            ID of the saved physical plan to delete.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Free resources used to store saved plans.
        """
        pass
