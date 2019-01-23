# custom_backend.py
# Author: Thomas MINIER - MIT License 2017-2018
import pytest
from database.import_manager import import_backend
from http_server.server import sage_app


def test_import_backend():
    config = {'foo': 'hello world!'}
    factory = import_backend('silly', 'tests.database.custom_backend', 'SillyConnector', ['foo'])
    connector = factory(config)
    assert connector.foo() == config['foo']


def test_import_backend_missing_param():
    with pytest.raises(SyntaxError):
        config = {'foo': 'hello world!'}
        factory = import_backend('silly', 'tests.database.custom_backend', 'SillyConnector', ['foo', 'bar'])
        factory(config)


def test_custom_backend_config():
    app = sage_app('tests/database/config.yaml')


def test_custom_backend_missing_params_config():
    with pytest.raises(SyntaxError):
        sage_app('tests/database/missing_params.yaml')


def test_custom_backend_invalid_declaration_config():
    with pytest.raises(SyntaxError):
        sage_app('tests/database/invalid_declaration.yaml')
