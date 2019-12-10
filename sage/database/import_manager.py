# import_manager.py
# Author: Thomas MINIER - MIT License 2017-2020
from importlib import import_module


def builtin_backends():
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


def import_backend(name, module_path, class_name, required_params):
    """Load a new backend from the config file"""
    # factory used to build new connector
    def __factory(params):
        # load module dynamically
        module = import_module(module_path)
        if not hasattr(module, class_name):
            raise RuntimeError("Connector class {} not found in module {}".format(class_name, module_path))
        connector = getattr(module, class_name)
        # check that all required params are present
        for key in required_params:
            if key not in params:
                raise SyntaxError('Missing required parameters for backend {}. Expected to see {}'.format(name, required_params))
        return connector.from_config(params)
    return __factory
