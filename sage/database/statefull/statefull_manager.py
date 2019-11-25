# statefull_manager.py
# Author: Thomas MINIER - MIT License 2017-2019
from abc import ABC, abstractmethod


class StatefullManager(ABC):
    """A StatefullManager is an abstract class for storing saved SPARQL query execution plans"""

    @abstractmethod
    def get_plan(self, plan_id):
        """Get a saved plan by ID"""
        pass

    @abstractmethod
    def save_plan(self, id, plan):
        """Store a saved plan by ID"""
        pass

    @abstractmethod
    def delete_plan(self, plan_id):
        """Delete a saved plan by ID"""
        pass

    @abstractmethod
    def from_config(config):
        """Build a StatefullManager from a config dictionnary"""
        pass

    def open(self):
        """Open the StatefullManager connection"""
        pass

    def close(self):
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
