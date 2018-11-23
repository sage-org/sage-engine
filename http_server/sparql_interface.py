# bgp_interface.py
# Author: Thomas MINIER - MIT License 2017-2018
from flask import Blueprint, request, Response, render_template, abort
from query_engine.sage_engine import SageEngine
from query_engine.optimizer.plan_builder import build_query_plan
from http_server.schema import QueryRequest
from http_server.utils import encode_saved_plan, decode_saved_plan, secure_url, format_marshmallow_errors
from database.descriptors import VoidDescriptor
import http_server.responses as responses
from time import time


def sparql_blueprint(dataset, logger):
    """Get a Blueprint that implement a SPARQL interface with quota on /sparql/<dataset-name>"""
    s_blueprint = Blueprint("sparql-interface", __name__)

    @s_blueprint.route("/sparql/", methods=["GET"])
    def sparql_index():
        try:
            url = secure_url(request.url)
            graphs = [dinfo for dinfo in dataset.describe(url)]
            return render_template("interfaces.html", dataset=graphs)
        except Exception as e:
            logger.error(e)
            abort(500)

    @s_blueprint.route("/sparql/<graph_name>", methods=["GET", "POST"])
    def sparql_query(graph_name):
        logger.debug('[IP: {}] [/sparql/] Querying {}'.format(request.environ['REMOTE_ADDR'], graph_name))
        graph = dataset.get_graph(graph_name)
        if graph is None:
            abort(404)

        logger.debug('[/sparql/] Corresponding dataset found')
        mimetype = request.accept_mimetypes.best_match([
            "application/json", "application/xml",
            "application/sparql-results+json", "application/sparql-results+xml"
        ])
        url = secure_url(request.url)
        try:
            # A GET request always returns the homepage of the dataset
            if request.method == "GET" or (not request.is_json):
                dinfo = graph.describe(url)
                dinfo['@id'] = url
                void_desc = {
                    "nt": VoidDescriptor(url, graph).describe("ntriples"),
                    "ttl": VoidDescriptor(url, graph).describe("turtle"),
                    "xml": VoidDescriptor(url, graph).describe("xml")
                }
                return render_template("sage.html", dataset_info=dinfo, void_desc=void_desc)

            engine = SageEngine()
            post_query, err = QueryRequest().load(request.get_json())
            if err is not None and len(err) > 0:
                return Response(format_marshmallow_errors(err), status=400)
            logger.debug('[IP: {}] [/sparql/] Query={}'.format(request.environ['REMOTE_ADDR'], post_query))
            quota = graph.quota / 1000
            max_results = graph.max_results

            # Load next link
            next_link = None
            if 'next' in post_query:
                logger.debug('[/sparql/{}] Saved plan found, decoding "next" link'.format(graph_name))
                next_link = decode_saved_plan(post_query["next"])
            else:
                logger.debug('[/sparql/{}] Query to evaluate: {}'.format(graph_name, post_query))

            # build physical query plan, then execute it with the given quota
            logger.debug('[/sparql/{}] Starting query evaluation...'.format(graph_name))
            start = time()
            plan, cardinalities = build_query_plan(post_query["query"], dataset, graph_name, next_link)
            loading_time = (time() - start) * 1000
            bindings, saved_plan, is_done = engine.execute(plan, quota, max_results)
            logger.debug('[/sparql/{}] Query evaluation completed'.format(graph_name))

            # compute controls for the next page
            start = time()
            next_page = None
            if is_done:
                logger.debug('[/sparql/{}] Query completed under the time quota'.format(graph_name))
            else:
                logger.debug('[/sparql/{}] The query was not completed under the time quota...'.format(graph_name))
                logger.debug('[/sparql/{}] Saving the execution to plan to generate a "next" link'.format(graph_name))
                next_page = encode_saved_plan(saved_plan)
                logger.debug('[/sparql/{}] "next" link successfully generated'.format(graph_name))
            exportTime = (time() - start) * 1000
            stats = {"cardinalities": cardinalities, "import": loading_time, "export": exportTime}

            if mimetype == "application/sparql-results+json":
                return Response(responses.w3c_json_streaming(bindings, next_page, stats, url), content_type='application/json')
            if mimetype == "application/xml" or mimetype == "application/sparql-results+xml":
                return Response(responses.w3c_xml(bindings, next_page, stats), content_type="application/xml")
            return Response(responses.raw_json_streaming(bindings, next_page, stats, url), content_type='application/json')
        except Exception as e:
            logger.error(e)
            abort(500)
    return s_blueprint
