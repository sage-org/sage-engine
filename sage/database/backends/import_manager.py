from importlib import import_module
from typing import Callable, Dict, List

from sage.database.backends.db_connector import DatabaseConnector

BackendFactory = Callable[[Dict[str, str]], DatabaseConnector]


def builtin_backends() -> Dict[str, BackendFactory]:
    """
    Loads the built-in backends: HDT, PostgreSQL and MVCC-PostgreSQL.

    Returns
    -------
    Dict[str, BackendFactory]
        The HDT, PostgreSQL and MVCC-PostgreSQL backends, registered in a dict.
    """
    data = [
        # HDT backend (read-only)
        {
            "name": "hdt-file",
            "path": "sage.database.backends.hdt.connector",
            "connector": "HDTFileConnector",
            "required": ["file"]
        },
        # PostgreSQL backend (optimised for read-only)
        {
            "name": "postgres",
            "path": "sage.database.backends.postgres.spo_table.connector",
            "connector": "DefaultPostgresConnector",
            "required": ["dbname", "user", "password"]
        },
        # PostgreSQL backend with a catalog-based schema (optimised for read-only)
        {
            "name": "postgres-catalog",
            "path": "sage.database.backends.postgres.catalog.connector",
            "connector": "CatalogPostgresConnector",
            "required": ["dbname", "user", "password"]
        },
        # MVCC-PostgreSQL (read-write)
        {
            "name": "postgres-mvcc",
            "path": "sage.database.backends.postgres.mvcc.connector",
            "connector": "MVCCPostgresConnector",
            "required": ["dbname", "user", "password"]
        },
        # SQlite backend (optimised for read-only)
        {
            "name": "sqlite",
            "path": "sage.database.backends.sqlite.spo_table.connector",
            "connector": "DefaultSQliteConnector",
            "required": ["database"]
        },
        # SQlite backend (optimised for read-only)
        {
            "name": "sqlite-catalog",
            "path": "sage.database.backends.sqlite.catalog.connector",
            "connector": "CatalogSQliteConnector",
            "required": ["database"]
        },
        # HBase backend
        {
            "name": "hbase",
            "path": "sage.database.backends.hbase.connector",
            "connector": "HBaseConnector",
            "required": ["thrift_host"]
        }
    ]
    return {item["name"]: import_backend(item["name"], item["path"], item["connector"], item["required"]) for item in data}


def import_backend(
    name: str, module_path: str, class_name: str, required_params: List[str]
) -> BackendFactory:
    """
    Loads a new database backend, defined by the user, and get a factory
    function to build it.

    Parameters
    ----------
    name: str
        Name of the database backend.
    module_path: str
        Path to the python module which contains the backend implementation.
    class_name: str
        Name of the class that implements the backend. It must be a subclass
        of sage.database.db_connector.DatabaseConnector.
    required_params: List[str]
        list of required configuration parameters for the backend.

    Returns
    -------
    BackendFactory
        A factory function that build an instance of the new backend from a
        configuration object.

    Example
    -------
    >>> name = "hdt-bis"
    >>> module_path = "sage.database.hdt.connector"
    >>> class_name = "HDTFileConnector"
    >>> params = [ "file" ]
    >>> factory = import_backend(name, module_path, class_name, params)
    >>> hdt_backend = factory({ "file": "/opt/data/hdt/dbpedia.hdt" })
    """
    # factory used to build new connector
    def __factory(params: Dict[str, str]) -> DatabaseConnector:
        # load module dynamically
        module = import_module(module_path)
        if not hasattr(module, class_name):
            raise RuntimeError(
                f"Connector class {class_name} not found in module {module_path}")
        connector = getattr(module, class_name)
        # check that all required params are present
        for key in required_params:
            if key not in params:
                raise SyntaxError(
                    f"Missing required parameters for backend {name}. "
                    f"Expected to see {required_params}")
        return connector.from_config(params)
    return __factory
