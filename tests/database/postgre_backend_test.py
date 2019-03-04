# postgre_backend_test.py
# Author: Thomas MINIER - MIT License 2017-2019
import pytest
from sage.database.postgre.connector import PostgreConnector
from tests.database.fixtures import index_scan_fixtures


def assert_next_triple(iterator, expected):
    triple = next(iterator)
    assert triple in expected
    expected.remove(triple)


@pytest.mark.parametrize("subj,pred,obj,expected", index_scan_fixtures())
def test_postgre_simple_scan(subj, pred, obj, expected):
    with PostgreConnector('watdiv', 'minier-t', 'minier-t', '') as backend:
        iterator, c = backend.search(subj, pred, obj)
        assert iterator.has_next()
        while iterator.has_next() and len(expected) > 0:
            assert_next_triple(iterator, expected)
        assert not iterator.has_next()
        assert len(expected) == 0


@pytest.mark.parametrize("subj,pred,obj,expected", index_scan_fixtures())
def test_postgre_resume_scan(subj, pred, obj, expected):
    # don't test for scan that yield one matching RDF triple
    if len(expected) > 1:
        with PostgreConnector('watdiv', 'minier-t', 'minier-t', '') as backend:
            iterator, c = backend.search(subj, pred, obj)
            assert iterator.has_next()
            # read first triple, then stop and reload a new iterator
            assert_next_triple(iterator, expected)
            last_read = iterator.last_read()
            # force close the iterator, to end the previous transaction
            iterator.__del__()
            iterator, c = backend.search(subj, pred, obj, last_read=last_read)
            while iterator.has_next() and len(expected) > 0:
                assert_next_triple(iterator, expected)
            assert not iterator.has_next()
            assert len(expected) == 0


def test_postgre_scan_unknown_pattern():
    with PostgreConnector('watdiv', 'minier-t', 'minier-t', '') as backend:
        iterator, c = backend.search('http://example.org#toto', None, None)
        assert not iterator.has_next()
        assert next(iterator) is None
