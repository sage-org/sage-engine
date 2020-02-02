# utils.py
# Author: Thomas MINIER - MIT License 2017-2019
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


def __n3_to_str(triple):
    """Convert a rdflib RDF triple into a tuple of strings (in N3 format)"""
    s, p, o = triple
    s = s.n3()
    p = p.n3()
    o = o.n3()
    if s.startswith('<') and s.endswith('>'):
        s = s[1:len(s) - 1]
    if p.startswith('<') and p.endswith('>'):
        p = p[1:len(p) - 1]
    if o.startswith('<') and o.endswith('>'):
        o = o[1:len(o) - 1]
    return (s, p, o)


def get_rdf_reader(file_path, format='nt'):
    """Get an iterator over RDF triples from a file"""
    iterator = None
    nb_triples = 0
    # load standard RDF formats using rdflib
    if format == 'nt' or format == 'ttl':
        g = Graph()
        g.parse(file_path, format=format)
        nb_triples = len(g)
        iterator = map(__n3_to_str, g.triples((None, None, None)))
    elif format == 'hdt':
        # load HDTDocument without additional indexes
        # they are not needed since we only search by "?s ?p ?o"
        doc = HDTDocument(file_path, indexed=False)
        iterator, nb_triples = doc.search_triples("", "", "")
    return iterator, nb_triples
