# lookup_interface.py
# Author: Thomas MINIER - MIT License 2017-2018
from flask import Blueprint, request, Response, abort
from query_engine.sage_engine import SageEngine
from query_engine.optimizer.plan_builder import build_query_plan
from http_server.utils import encode_saved_plan, decode_saved_plan, secure_url
import http_server.responses as responses
# from time import time


def choose_format(mimetype):
    if mimetype == "text/turtle":
        return "turtle", mimetype
    elif mimetype == "application/xml":
        return "xml", mimetype
    elif mimetype == "application/n-quads":
        return "nquads", mimetype
    elif mimetype == "application/trig":
        return "trig", mimetype
    elif mimetype == "application/json" or mimetype == "application/json+ld":
        return "json-ld", mimetype
    return "ntriples", "application/n-triples"


def build_describe_query(subject):
    """Create a query SELECT * WHERE { <subject> ?p ?o .}"""
    return {
        'type': 'bgp',
        'bgp': [
            {
                'subject': subject,
                'predicate': '?p',
                'object': '?o'
            }
        ]
    }


def bindings_to_triple(entity, bindings, url):
    for b in responses.deskolemize(bindings, url):
        yield (entity, b["?p"], b["?o"])


def lookup_blueprint(datasets, logger):
    """Get a Blueprint that provides an Entity Lookup service"""
    l_blueprint = Blueprint("lookup-interface", __name__)

    @l_blueprint.route("/entity/<dataset_name>/<entity>", methods=["GET"])
    def lookup_entity(dataset_name, entity):
        """Evaluates a DESCRIBE query over a RDF dataset"""
        logger.info('[IP: {}] [/lookup/] Querying {}'.format(request.environ['REMOTE_ADDR'], dataset_name))
        dataset = datasets.get_dataset(dataset_name)
        if dataset is None:
            abort(404)
        url = secure_url(request.url)
        try:
            engine = SageEngine()

            # Get entity and possible next link
            entity_uri = secure_url(request.base_url)
            next_link = request.args.get("next", default=None)
            post_query = build_describe_query(entity_uri)

            logger.info('[IP: {}] [/lookup/] Entity={}'.format(request.environ['REMOTE_ADDR'], entity_uri))
            quota = dataset.quota / 1000
            max_results = dataset.max_results

            # Load next link
            if next_link is not None:
                logger.debug('[/lookup/{}] Saved plan found, decoding "next" link'.format(dataset_name))
                next_link = decode_saved_plan(next_link)
            else:
                logger.debug('[/lookup/{}] Query to evaluate: {}'.format(dataset_name, post_query))

            # build physical query plan, then execute it with the given quota
            logger.debug('[/lookup/{}] Starting query evaluation...'.format(dataset_name))
            # start = time()
            plan, cardinalities = build_query_plan(post_query, dataset, next_link)
            # loading_time = (time() - start) * 1000
            bindings, saved_plan, is_done = engine.execute(plan, quota, max_results)
            logger.debug('[/lookup/{}] Query evaluation completed'.format(dataset_name))

            # compute controls for the next page
            # start = time()
            next_page = None
            if is_done:
                logger.debug('[/lookup/{}] Query completed under the time quota'.format(dataset_name))
            else:
                logger.debug('[/lookup/{}] The query was not completed under the time quota...'.format(dataset_name))
                logger.debug('[/lookup/{}] Saving the execution to plan to generate a "next" link'.format(dataset_name))
                next_page = encode_saved_plan(saved_plan)
                logger.debug('[/lookup/{}] "next" link successfully generated'.format(dataset_name))
            # exportTime = (time() - start) * 1000
            # stats = {"import": loading_time, "export": exportTime}
            triples = bindings_to_triple(entity_uri, bindings, url)

            headers = dict()
            if next_page is not None:
                headers["X-Sage-Next"] = "{}?next={}".format(entity_uri, next_page)
                headers["Link"] = "<{}?next={}>; rel=\"next\"; title=\"Next page\"".format(entity_uri, next_page)

            return Response(responses.ntriples_streaming(triples), content_type="application/ntriples", headers=headers)
        except Exception:
            abort(500)
    return l_blueprint
