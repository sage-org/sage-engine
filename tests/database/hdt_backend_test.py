# hdt_backend_test.py
# Author: Thomas MINIER - MIT License 2017-2019
import pytest
from sage.database.hdt.connector import HDTFileConnector
from tests.database.fixtures import index_scan_fixtures


def assert_next_triple(iterator, expected):
    triple = next(iterator)
    assert triple in expected
    expected.remove(triple)


@pytest.mark.parametrize("subj,pred,obj,expected", index_scan_fixtures())
def test_hdt_simple_scan(subj, pred, obj, expected):
    with HDTFileConnector('tests/data/watdiv.10M.hdt') as backend:
        iterator, c = backend.search(subj, pred, obj)
        assert iterator.has_next()
        while iterator.has_next() and len(expected) > 0:
            assert_next_triple(iterator, expected)
        assert not iterator.has_next()
        assert len(expected) == 0


@pytest.mark.parametrize("subj,pred,obj,expected", index_scan_fixtures())
def test_hdt_resume_scan(subj, pred, obj, expected):
    # don't test for scan that yield one matching RDF triple
    if len(expected) > 1:
        with HDTFileConnector('tests/data/watdiv.10M.hdt') as backend:
            iterator, c = backend.search(subj, pred, obj)
            assert iterator.has_next()
            # read first triple, then stop and reload a new iterator
            assert_next_triple(iterator, expected)
            last_read = iterator.last_read()
            iterator, c = backend.search(subj, pred, obj, last_read=last_read)
            while iterator.has_next() and len(expected) > 0:
                assert_next_triple(iterator, expected)
            assert not iterator.has_next()
            assert len(expected) == 0


def test_hdt_scan_unknown_pattern():
    with HDTFileConnector('tests/data/watdiv.10M.hdt') as backend:
        iterator, c = backend.search('http://example.org#toto', None, None)
        assert next(iterator) is None
