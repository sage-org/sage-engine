# rdf_file_connector_test.py
# Author: Thomas MINIER - MIT License 2017-2018
from database.hdt_server_connector import HDTServerConnector
import pytest

db = HDTServerConnector("https://hdt.lod.labs.vu.nl", "<https://hdt.lod.labs.vu.nl/graph/Wikidata>")


@pytest.mark.skip(reason="broken due to an issue with the live HDT server")
def test_simple_search_hdtserver():
    it, card = db.search_triples("http://www.wikidata.org/entity/P2842", None, None)
    nbRead = 0
    assert card == 209
    for s, p, o in it:
        assert s == "<http://www.wikidata.org/entity/P2842>"
        nbRead += 1
    assert nbRead == card


@pytest.mark.skip(reason="broken due to an issue with the live HDT server")
def test_limit_search_hdtserver():
    it, card = db.search_triples("http://www.wikidata.org/entity/P2842", None, None, limit=110)
    nbRead = 0
    assert card == 209
    for s, p, o in it:
        assert s == "<http://www.wikidata.org/entity/P2842>"
        nbRead += 1
    assert nbRead == 110


@pytest.mark.skip(reason="broken due to an issue with the live HDT server")
def test_offset_search_hdtserver():
    it, card = db.search_triples("http://www.wikidata.org/entity/P2842", None, None, offset=105)
    nbRead = 0
    assert card == 209
    for s, p, o in it:
        assert s == "<http://www.wikidata.org/entity/P2842>"
        nbRead += 1
    assert nbRead == 104
