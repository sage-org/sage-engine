# bgp_interface_test.py
# Author: Thomas MINIER - MIT License 2017-2018
import pytest
from sage.http_server.server import sage_app
from tests.http.utils import jsonSparql


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
        """INSERT DATA { GRAPH<http://localhost/sparql/update-test> {
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
        app = sage_app('tests/update/config.yaml')
        app.testing = True
        self.app = app.test_client()

    @classmethod
    def teardown_method(self):
        pass

    @pytest.mark.parametrize("query,expected_content", fixtures)
    def test_insert_interface(self, query, expected_content):
        # insert data
        response = jsonSparql(self.app, query, None, 'http://localhost/sparql/update-test')
        # fetch graph content to assert that data was inserted
        fetch_query = "SELECT * WHERE {?s ?p ?o}"
        has_next = True
        next_link = None
        results = list()
        while has_next:
            response = jsonSparql(self.app, fetch_query, next_link, 'http://localhost/sparql/update-test')
            has_next = response['hasNext']
            next_link = response['next']
            results += response['bindings']
        assert len(results) == len(expected_content)
        for b in results:
            assert (b['?s'], b['?p'], b['?o']) in expected_content
