# hashmap_manager.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict

from sage.database.core.dataset import Dataset
from sage.database.saved_plan.saved_plan_manager import SavedPlanManager
from sage.database.saved_plan.utils import encode_saved_plan, decode_saved_plan
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.loader import load
from sage.query_engine.protobuf.iterators_pb2 import RootTree


class StatelessManager(SavedPlanManager):
    """A HashMapManager stores saved plans in main memory using a simple HashMap"""

    def __init__(self):
        super(StatelessManager, self).__init__()

    def get_plan(self, plan_id: str, dataset: Dataset) -> PreemptableIterator:
        """Get a saved plan by ID.

        Args:
          * ID of the saved plan to retrieve.
          * RDF dataset on which the query is executed.

        Returns: The saved plan corresponding to the input ID.
        """
        return load(decode_saved_plan(plan_id), dataset)

    def save_plan(self, plan: PreemptableIterator) -> str:
        """Store a saved plan.

        Argument: Plan to save.

        Returns: The ID of the saved plan.
        """
        saved_plan = RootTree()
        source_field = f'{plan.serialized_name()}_source'
        getattr(saved_plan, source_field).CopyFrom(plan.save())
        return encode_saved_plan(saved_plan)

    def delete_plan(self, plan_id: str) -> None:
        """Delete a saved plan by ID.

        Argument: ID of the saved plan to delete.
        """
        pass

    def from_config(config: Dict[str, str]):
        """Build a StatefullManager from a config dictionnary"""
        return StatelessManager()
