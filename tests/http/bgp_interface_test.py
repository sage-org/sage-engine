# bgp_interface_test.py
# Author: Thomas MINIER - MIT License 2017-2018
import pytest
from http_server.server import sage_app
from tests.http.utils import jsonPost

app = sage_app('data/test_config.yaml')

bgp_queries = [
    ({
        'query': {
            'type': 'bgp',
            'bgp': [
                {
                    'subject': '?s',
                    'predicate': 'http://schema.org/eligibleRegion',
                    'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
                },
                {
                    'subject': '?s',
                    'predicate': 'http://purl.org/goodrelations/includes',
                    'object': '?includes'
                },
                {
                    'subject': '?s',
                    'predicate': 'http://purl.org/goodrelations/validThrough',
                    'object': '?validity'
                }
            ]
        }
    }, 2180, 6),
    ({
        'query': {
            'type': 'bgp',
            'bgp': [
                {
                    'subject': '?v0',
                    'predicate': 'http://schema.org/eligibleRegion',
                    'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
                },
                {
                    'subject': '?v0',
                    'predicate': 'http://purl.org/goodrelations/includes',
                    'object': '?v1'
                },
                {
                    'subject': '?v1',
                    'predicate': 'http://schema.org/contentSize',
                    'object': '?v3'
                }
            ]
        }
    }, 531, 5),
    ({
        'query': {
            'type': 'bgp',
            'bgp': [
                {
                    'subject': '?s',
                    'predicate': 'http://xmlns.com/foaf/age',
                    'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup3'
                },
                {
                    'subject': '?s',
                    'predicate': 'http://schema.org/nationality',
                    'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/Country1'
                },
                {
                    'subject': '?s',
                    'predicate': 'http://db.uwaterloo.ca/~galuc/wsdbm/gender',
                    'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/Gender1'
                }
            ]
        }
    }, 93, 3)
]


class TestBGPInterface(object):
    @classmethod
    def setup_class(self):
        app.testing = True
        self.app = app.test_client()

    @classmethod
    def teardown_class(self):
        pass

    @pytest.mark.parametrize("body,cardinality,calls", bgp_queries)
    def test_bgp_interface(self, body, cardinality, calls):
        query = body
        nbResults = 0
        nbCalls = 0
        hasNext = True
        while hasNext:
            response = jsonPost(self.app, '/sparql/watdiv100', query)
            nbResults += len(response['results']['bindings'])
            hasNext = response['head']['controls']['hasNext']
            query['next'] = response['head']['controls']['next']
            nbCalls += 1
        assert nbResults == cardinality
        assert nbCalls <= calls
