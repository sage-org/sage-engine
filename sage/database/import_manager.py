# import_manager.py
# Author: Thomas MINIER - MIT License 2017-2020
from importlib import import_module
from typing import Callable, Dict, List

from sage.database.db_connector import DatabaseConnector

BackendFactory = Callable[[Dict[str, str]], DatabaseConnector]


def builtin_backends() -> Dict[str, BackendFactory]:
    """Load the built-in backends: HDT, PostgreSQL and MVCC-PostgreSQL"""
    data = [
        # HDT backend (read-only)
        {
            'name': 'hdt-file',
            'path': 'sage.database.hdt.connector',
            'connector': 'HDTFileConnector',
            'required': [
                'file'
            ]
        },
        # PostgreSQL backend (optimised for read-only)
        {
            'name': 'postgres',
            'path': 'sage.database.postgres.connector',
            'connector': 'PostgresConnector',
            'required': [
                'dbname',
                'user',
                'password'
            ]
        },
        # MVCC-PostgreSQL (read-write)
        {
            'name': 'postgres-mvcc',
            'path': 'sage.database.postgres.mvcc_connector',
            'connector': 'MVCCPostgresConnector',
            'required': [
                'dbname',
                'user',
                'password'
            ]
        }
    ]
    return {item['name']: import_backend(item['name'], item['path'], item['connector'], item['required']) for item in data}


def import_backend(name: str, module_path: str, class_name: str, required_params: List[str]) -> BackendFactory:
    """Load a new backend from the config file"""
    # factory used to build new connector
    def __factory(params: Dict[str, str]) -> DatabaseConnector:
        # load module dynamically
        module = import_module(module_path)
        if not hasattr(module, class_name):
            raise RuntimeError(f"Connector class {class_name} not found in module {module_path}")
        connector = getattr(module, class_name)
        # check that all required params are present
        for key in required_params:
            if key not in params:
                raise SyntaxError(f"Missing required parameters for backend {name}. Expected to see {required_params}")
        return connector.from_config(params)
    return __factory
