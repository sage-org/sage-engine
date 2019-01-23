# import_manager.py
# Author: Thomas MINIER - MIT License 2017-2019
from importlib import import_module


def import_backend(name, module_path, class_name, required_params):
    """Load a new backend from the config file"""
    module = import_module(module_path)
    if not hasattr(module, class_name):
        raise RuntimeError("Connector class {} not found in module {}".format(class_name, module_path))
    connector = getattr(module, class_name)

    # factory used to build new connector
    def __factory(params):
        # check that all required params are present
        for key in required_params:
            if key not in params:
                raise SyntaxError('Missing required parameters for backend {}. Expected to see {}'.format(name, required_params))
        return connector.from_config(params)
    return __factory
