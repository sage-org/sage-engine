# bgp_interface_test.py
# Author: Thomas MINIER - MIT License 2017-2018
import pytest
from sage.http_server.server import sage_app
from tests.http.utils import post_sparql

# fixutre format: query, initial INSERT DATA query, expected graph content
fixtures = [
    (
        """DELETE DATA { <http://example.org#Thomas_Minier> <http://www.w3.org/2000/01/rdf-schema#label> "Thomas Minier"@en .
        <http://example.org#Thomas_Minier> <http://xmlns.com/foaf/0.1/knows> <http://example.org#Arnaud_Grall> . }""",
        """INSERT DATA { <http://example.org#Thomas_Minier> <http://www.w3.org/2000/01/rdf-schema#label> "Thomas Minier"@en .
        <http://example.org#Thomas_Minier> <http://xmlns.com/foaf/0.1/knows> <http://example.org#Arnaud_Grall> . }""",
        []
    ),
    (
        """DELETE DATA { GRAPH<http://localhost/sparql/update-test> {<http://example.org#Thomas_Minier> <http://www.w3.org/2000/01/rdf-schema#label> "Thomas Minier"@en .
        <http://example.org#Thomas_Minier> <http://xmlns.com/foaf/0.1/knows> <http://example.org#Arnaud_Grall> . }}""",
        """INSERT DATA { <http://example.org#Thomas_Minier> <http://www.w3.org/2000/01/rdf-schema#label> "Thomas Minier"@en .
        <http://example.org#Thomas_Minier> <http://xmlns.com/foaf/0.1/knows> <http://example.org#Arnaud_Grall> . }""",
        []
    )
]


class TestDeleteDataInterface(object):
    @classmethod
    def setup_method(self):
        app = sage_app('tests/update/config.yaml')
        app.testing = True
        self.app = app.test_client()

    @classmethod
    def teardown_method(self):
        pass

    @pytest.mark.parametrize("query,initial_insert,expected_content", fixtures)
    def test_delete_interface(self, query, initial_insert, expected_content):
        # first, populate database
        response = post_sparql(self.app, initial_insert, None, 'http://localhost/sparql/update-test')
        # then, execute delete
        response = post_sparql(self.app, query, None, 'http://localhost/sparql/update-test')
        # finally, fetch graph content to assert that data was deleted
        fetch_query = "SELECT * WHERE {?s ?p ?o}"
        has_next = True
        next_link = None
        results = list()
        while has_next:
            response = post_sparql(self.app, fetch_query, next_link, 'http://localhost/sparql/update-test')
            has_next = response['hasNext']
            next_link = response['next']
            results += response['bindings']
        assert len(results) == len(expected_content)
        for b in results:
            assert (b['?s'], b['?p'], b['?o']) in expected_content
