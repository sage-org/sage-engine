from requests import post
from time import time
from pprint import pprint

body = {
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

# body = {
#     'bgp': {
#         'tp1': {
#             'subject': '?v0',
#             'predicate': 'http://schema.org/eligibleRegion',
#             'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/Country9'
#         },
#         'tp2': {
#             'subject': '?v0',
#             'predicate': 'http://purl.org/goodrelations/includes',
#             'object': '?v1'
#         },
#         'tp3': {
#             'subject': '?v1',
#             'predicate': 'http://schema.org/contentSize',
#             'object': '?v3'
#         }
#     }
# }

# body = {
#     'bgp': {
#         'tp1': {
#             'subject': '?s',
#             'predicate': 'http://xmlns.com/foaf/age',
#             'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup3'
#         },
#         'tp2': {
#             'subject': '?s',
#             'predicate': 'http://schema.org/nationality',
#             'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/Country1'
#         },
#         'tp3': {
#             'subject': '?s',
#             'predicate': 'http://db.uwaterloo.ca/~galuc/wsdbm/gender',
#             'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/Gender1'
#         }
#     }
# }

# body = {
#     'bgp': {
#         'tp1': {
#             'subject': '?s',
#             'predicate': 'http://xmlns.com/foaf/age',
#             'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/AgeGroup3'
#         },
#         'tp2': {
#             'subject': '?s',
#             'predicate': 'http://schema.org/nationality',
#             'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/Country1'
#         },
#         'tp3': {
#             'subject': '?s',
#             'predicate': 'http://db.uwaterloo.ca/~galuc/wsdbm/gender',
#             'object': 'http://db.uwaterloo.ca/~galuc/wsdbm/Gender1'
#         },
#         'tp4': {
#             'subject': '?s',
#             'predicate': 'http://schema.org/nationality',
#             'object': '?nat'
#         }
#     }
# }

url = "http://localhost:8000/bgp/watdiv100"

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
    print('page size', json_results['pageSize'])
    bindings += json_results['bindings']
    hasNext = json_results['hasNext']
    next = json_results['next']
    print(next)

print('nb HTTP requests', nbCalls)
print('nb results', len(bindings))
# pprint(bindings)
