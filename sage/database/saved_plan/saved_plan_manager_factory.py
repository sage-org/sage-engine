from sage.database.core.dataset import Dataset
from sage.database.saved_plan.saved_plan_manager import SavedPlanManager
from sage.database.saved_plan.stateless_manager import StatelessManager
from sage.database.saved_plan.statefull_manager import StatefullManager


class SavedPlanManagerFactory():

    @staticmethod
    def create() -> SavedPlanManager:
        if Dataset().is_stateless:
            return StatelessManager()
        return StatefullManager()
