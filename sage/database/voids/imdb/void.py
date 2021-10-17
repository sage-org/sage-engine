import csv

from typing import Tuple

from sage.database.core.dataset import Dataset

triples_per_predicate = dict()
distinct_subjects_per_predicate = dict()
distinct_objects_per_predicate = dict()


def load_triples_per_predicate() -> None:
    with open('sage/database/voids/imdb/triple_per_predicate.csv', 'r') as csvfile:
        for row in csv.reader(csvfile):
            triples_per_predicate[row[1]] = int(row[0])


def load_distinct_subjects_per_predicate() -> None:
    with open('sage/database/voids/imdb/distinct_subjects_per_predicate.csv', 'r') as csvfile:
        for row in csv.reader(csvfile):
            distinct_subjects_per_predicate[row[1]] = int(row[0])


def load_distinct_objects_per_predicate() -> None:
    with open('sage/database/voids/imdb/distinct_objects_per_predicate.csv', 'r') as csvfile:
        for row in csv.reader(csvfile):
            distinct_objects_per_predicate[row[1]] = int(row[0])


def count_triples(predicate: str) -> int:
    if predicate in triples_per_predicate:
        return triples_per_predicate[predicate]
    return 0


def count_distinct_subjects(predicate: str) -> int:
    if predicate in distinct_subjects_per_predicate:
        return distinct_subjects_per_predicate[predicate]
    return 1


def count_distinct_objects(predicate: str) -> int:
    if predicate in distinct_objects_per_predicate:
        return distinct_objects_per_predicate[predicate]
    return 1


def estimate_cardinality(dataset: Dataset, pattern: Tuple[str, str, str], default: int = 0) -> int:
    try:
        graph = dataset.get_graph('http://localhost:8080/sparql/imdb')
        _, cardiality = graph.search(pattern[0], pattern[1], pattern[2])
        print(f'estimated cardinality: {cardiality} (default={default})')
        return cardiality
    except Exception:
        return default


load_triples_per_predicate()
load_distinct_subjects_per_predicate()
load_distinct_objects_per_predicate()
