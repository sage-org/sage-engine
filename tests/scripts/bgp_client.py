from requests import post
from time import time
import argparse

parser = argparse.ArgumentParser(description='Evaluate a BGP query over a SaGe server')
parser.add_argument('query', metavar='Q', help='query name')
args = parser.parse_args()

q1 = {
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
}

q2 = {
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
}

q3 = {
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
}

q4 = {
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
            },
            {
                'subject': '?s',
                'predicate': 'http://schema.org/nationality',
                'object': '?nat'
            }
        ]
    }
}

queries = {
    'q1': q1,
    'q2': q2,
    'q3': q3,
    'q4': q4
}

url = "http://localhost:8000/bgp/watdiv100"

body = queries[args.query]

bindings = []
nbCalls = 0
hasNext = True
next = None

while hasNext:
    nbCalls += 1
    start = time()
    if next is not None:
        body['next'] = next
    req = post(url, json=body)
    stop = time() - start
    print("HTTP request executed in %f ms" % (stop * 1000))
    json_results = req.json()
    print('overhead', json_results['stats'])
    print('page size', json_results['pageSize'])
    bindings += json_results['bindings']
    hasNext = json_results['hasNext']
    next = json_results['next']
    print(next)

print('nb HTTP requests', nbCalls)
print('nb results', len(bindings))
