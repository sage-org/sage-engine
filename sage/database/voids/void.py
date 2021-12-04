import csv

from typing import Dict


cardinalities = dict()
distinct_subjects = dict()
distinct_objects = dict()


def load_csv(path: str) -> Dict[str, int]:
    data = dict()
    with open(path, 'r') as csvfile:
        for row in csv.reader(csvfile):
            data[row[1]] = int(row[0])
    return data


def count_triples(graph: str, predicate: str) -> int:
    if graph not in cardinalities:
        return 0
    elif predicate not in cardinalities[graph]:
        return 0
    else:
        return cardinalities[graph][predicate]


def count_distinct_subjects(graph: str, predicate: str) -> int:
    if graph not in distinct_subjects:
        return 1
    elif predicate not in distinct_subjects[graph]:
        return 1
    else:
        return distinct_subjects[graph][predicate]


def count_distinct_objects(graph: str, predicate: str) -> int:
    if graph not in distinct_objects:
        return 1
    elif predicate not in distinct_objects[graph]:
        return 1
    else:
        return distinct_objects[graph][predicate]


cardinalities['http://localhost:8080/sparql/imdb'] = load_csv(
    'sage/database/voids/imdb/cardinalities.csv'
)
cardinalities['http://localhost:8080/sparql/watdiv10M'] = load_csv(
    'sage/database/voids/watdiv10M/cardinalities.csv'
)

distinct_subjects['http://localhost:8080/sparql/imdb'] = load_csv(
    'sage/database/voids/imdb/distinct_subjects.csv'
)
distinct_subjects['http://localhost:8080/sparql/watdiv10M'] = load_csv(
    'sage/database/voids/watdiv10M/distinct_subjects.csv'
)

distinct_objects['http://localhost:8080/sparql/imdb'] = load_csv(
    'sage/database/voids/imdb/distinct_objects.csv'
)
distinct_objects['http://localhost:8080/sparql/watdiv10M'] = load_csv(
    'sage/database/voids/watdiv10M/distinct_objects.csv'
)
