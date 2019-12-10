# bgp_interface_test.py
# Author: Thomas MINIER - MIT License 2017-2018
import pytest
from sage.http_server.server import run_app
from starlette.testclient import TestClient
from tests.http.utils import post_sparql


# fixutre format: query, expected graph content
fixtures = [
    (
        """INSERT DATA { <http://example.org#Thomas_Minier> <http://www.w3.org/2000/01/rdf-schema#label> "Thomas Minier"@en .
        <http://example.org#Thomas_Minier> <http://xmlns.com/foaf/0.1/knows> <http://example.org#Arnaud_Grall> . }""",
        [
            ("http://example.org#Thomas_Minier", "http://www.w3.org/2000/01/rdf-schema#label", "\"Thomas Minier\"@en"),
            ("http://example.org#Thomas_Minier", "http://xmlns.com/foaf/0.1/knows", "http://example.org#Arnaud_Grall")
        ]
    ),
    (
        """INSERT DATA { GRAPH<http://testserver/sparql/update-test> {
            <http://example.org#Thomas_Minier> <http://www.w3.org/2000/01/rdf-schema#label> "Thomas Minier"@en .
            <http://example.org#Thomas_Minier> <http://xmlns.com/foaf/0.1/knows> <http://example.org#Arnaud_Grall> .} }""",
        [
            ("http://example.org#Thomas_Minier", "http://www.w3.org/2000/01/rdf-schema#label", "\"Thomas Minier\"@en"),
            ("http://example.org#Thomas_Minier", "http://xmlns.com/foaf/0.1/knows", "http://example.org#Arnaud_Grall")
        ]
    )
]


class TestInsertDataInterface(object):
    @classmethod
    def setup_method(self):
        self._app = run_app('tests/update/config.yaml')
        self._client = TestClient(self._app)

    @classmethod
    def teardown_method(self):
        pass

    @pytest.mark.parametrize("query,expected_content", fixtures)
    def test_insert_interface(self, query, expected_content):
        # insert data
        response = post_sparql(self._client, query, None, 'http://testserver/sparql/update-test')
        assert response.status_code == 200
        # fetch graph content to assert that data was inserted
        fetch_query = "SELECT * WHERE {?s ?p ?o}"
        has_next = True
        next_link = None
        results = list()
        while has_next:
            response = post_sparql(self._client, fetch_query, next_link, 'http://testserver/sparql/update-test')
            assert response.status_code == 200
            response = response.json()
            has_next = response['hasNext']
            next_link = response['next']
            results += response['bindings']
        assert len(results) == len(expected_content)
        for b in results:
            assert (b['?s'], b['?p'], b['?o']) in expected_content
