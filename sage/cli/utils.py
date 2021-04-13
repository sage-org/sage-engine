# utils.py
# Author: Thomas MINIER - MIT License 2017-2019
import subprocess

from sys import exit
from os.path import isfile
from yaml import load, FullLoader
from rdflib import Graph
from hdt import HDTDocument


def load_graph(config_path, graph_name, logger, backends=[]):
    """Load a RDF graph from a Sage config file"""
    if isfile(config_path):
        config = load(open(config_path), Loader=FullLoader)
        if 'graphs' not in config:
            logger.error("No RDF graphs declared in the configuration provided")
            exit(1)
        graphs = config['graphs']
        graph = None
        kind = None
        for g in graphs:
            if g['name'] == graph_name and g['backend'] in backends:
                graph = g
                kind = g['backend']
                break
        if graph is None:
            logger.error("No compatible RDF graph named '{}' declared in the configuration provided".format(graph_name))
            exit(1)
        return graph, kind
    else:
        logger.error("Invalid configuration file supplied '{}'".format(config_path))
        exit(1)


def wccount(filename):
    command = f"wc -l {filename} | awk '{{print $1}}'"
    total = subprocess.run(command, shell=True, text=True, stdout=subprocess.PIPE).stdout
    return int(total)


def get_nb_triples(file_path: str, format: str) -> int:
    if format == 'nt':
        return wccount(file_path)
    elif format == 'hdt':
        doc = HDTDocument(file_path, indexed=False)
        _, nb_triples = doc.search_triples("", "", "")
        return nb_triples
    else:
        raise Exception(f'Unsupported RDF format: "{format}"')
