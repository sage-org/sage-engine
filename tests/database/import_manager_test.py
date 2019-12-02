# custom_backend.py
# Author: Thomas MINIER - MIT License 2017-2019
import pytest
from sage.database.import_manager import import_backend
from sage.database.core.yaml_config import load_config


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
    dataset = load_config('tests/database/config.yaml')
    assert dataset.has_graph('foo-dataset')
    assert dataset.get_graph('foo-dataset').search(None, None, None) == 'moo'


def test_custom_backend_missing_params_config():
    with pytest.raises(SyntaxError):
        load_config('tests/database/missing_params.yaml')


def test_custom_backend_invalid_declaration_config():
    with pytest.raises(SyntaxError):
        load_config('tests/database/invalid_declaration.yaml')
