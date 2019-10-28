# yaml_config.py
# Author: Thomas MINIER - MIT License 2017-2019
from yaml import load
from sage.database.import_manager import builtin_backends
from sage.database.core.graph import Graph
from math import inf
import logging


def load_config(config_file="config.yaml"):
    """Load config file to initialize RDF graphs.
    A config file is a YAML file, loaded as a module.

    Example config file:
    # config.yaml
    name: My LDF server
    maintainer: chuck Norris <me@gmail.com>

    datasets:
    -
        name: DBpedia-2016-04
        description: DBpedia dataset, version 2016-04
        backend: hdt-file
        file: /home/chuck-norris/dbpedia-2016-04.hdt
    -
        name: Chuck-Norris-facts
        description: Best Chuck Norris facts ever
        backend: rdf-file
        format: nt
        file: /home/chuck-norris/facts.nt
    """
    config = load(open(config_file))
    # available backends (populated with sage's native backends)
    backends = builtin_backends()
    # build custom backend (if there is some)
    if 'backends' in config and len(config['backends']) > 0:
        for b in config['backends']:
            if 'name' not in b or 'path' not in b or 'connector' not in b or 'required' not in b:
                raise SyntaxError('Invalid backend declared. Each custom backend must be declared with properties "name", "path", "connector" and "required"')
            backends[b['name']] = import_backend(b['name'], b['path'], b['connector'], b['required'])
    # set time quantum
    if 'quota' in config:
        if config['quota'] == 'inf':
            logging.warning("You are using SaGe with an infinite time quantum. Be sure to configure the Worker timeout of Gunicorn accordingly, otherwise long-running queries might be terminated.")
            quota = inf
        else:
            quota = config['quota']
    else:
        quota = 75
    config['quota'] = quota
    # set page size, i.e. the number of triples per page
    max_results = config['max_results'] if 'max_results' in config else inf
    # recopy default config. options in all config objects when they are missing
    for c in config["datasets"]:
        if 'quota' not in c:
            c['quota'] = quota
        if 'max_results' not in c:
            c['max_results'] = max_results
        if 'publish' not in c:
            c['publish'] = False
        if 'queries' not in c:
            c['queries'] = []
    # build RDF graphs
    graphs = {c["name"]: Graph(c, backends) for c in config["datasets"]}
    return (config, graphs, backends)
