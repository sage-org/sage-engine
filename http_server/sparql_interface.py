# bgp_interface.py
# Author: Thomas MINIER - MIT License 2017-2018
from flask import Blueprint, request, Response, render_template, abort, json
from query_engine.sage_engine import SageEngine
from query_engine.optimizer.plan_builder import build_query_plan
from http_server.schema import QueryRequest
from http_server.utils import encode_saved_plan, decode_saved_plan, secure_url, format_marshmallow_errors
from database.descriptors import VoidDescriptor
import http_server.responses as responses
from time import time


def sparql_blueprint(datasets, logger):
    """Get a Blueprint that implement a SPARQL interface with quota on /sparql/<dataset-name>"""
    sparql_blueprint = Blueprint("sparql-interface", __name__)

    @sparql_blueprint.route("/sparql/", methods=["GET"])
    def sparql_index():
        try:
            mimetype = request.accept_mimetypes.best_match(["application/json", "text/html"])
            url = secure_url(request.url)
            api_doc = {
                "@context": "http://www.w3.org/ns/hydra/context.jsonld",
                "@id": url,
                "@type": "ApiDocumentation",
                "title": "SaGe SPARQL API",
                "description": "A SaGe interface which allow evaluation of SPARQL queries over RDF datasets",
                "entrypoint": "{}sparql".format(url),
                "supportedClass": []
            }
            for dinfo in datasets.describe(url):
                api_doc["supportedClass"].append(dinfo)
            if mimetype is "text/html":
                return render_template("interfaces.html", api=api_doc)
            return json.jsonify(api_doc)
        except Exception:
            abort(500)

    @sparql_blueprint.route("/sparql/<dataset_name>", methods=["GET", "POST"])
    def sparql_query(dataset_name):
        logger.info('[/sparql/] Querying the dataset "{}"'.format(dataset_name))
        dataset = datasets.get_dataset(dataset_name)
        if dataset is None:
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
                dinfo = dataset.describe(url)
                dinfo['@id'] = url
                void_desc = {
                    "nt": VoidDescriptor(url, dataset).describe("ntriples"),
                    "ttl": VoidDescriptor(url, dataset).describe("turtle"),
                    "xml": VoidDescriptor(url, dataset).describe("xml")
                }
                return render_template("sage.html", dataset_info=dinfo, void_desc=void_desc)

            engine = SageEngine()
            post_query, err = QueryRequest().load(request.get_json())
            if err is not None and len(err) > 0:
                return Response(format_marshmallow_errors(err), status=400)
            logger.info('[/sparql/] Query: "{}"'.format(post_query))
            quota = dataset.quota / 1000
            max_results = dataset.maxResults

            # Load next link
            next_link = None
            if 'next' in post_query:
                logger.debug('[/sparql/{}] Saved plan found, decoding "next" link'.format(dataset_name))
                next_link = decode_saved_plan(post_query["next"])
            else:
                logger.debug('[/sparql/{}] Query to evaluate: {}'.format(dataset_name, post_query))

            # build physical query plan, then execute it with the given quota
            logger.debug('[/sparql/{}] Starting query evaluation...'.format(dataset_name))
            start = time()
            plan, cardinalities = build_query_plan(post_query["query"], dataset, next_link)
            loading_time = (time() - start) * 1000
            bindings, saved_plan, is_done = engine.execute(plan, quota, max_results)
            logger.debug('[/sparql/{}] Query evaluation completed'.format(dataset_name))

            # compute controls for the next page
            start = time()
            next_page = None
            if is_done:
                logger.debug('[/sparql/{}] Query completed under the time quota'.format(dataset_name))
            else:
                logger.debug('[/sparql/{}] The query was not completed under the time quota...'.format(dataset_name))
                logger.debug('[/sparql/{}] Saving the execution to plan to generate a "next" link'.format(dataset_name))
                next_page = encode_saved_plan(saved_plan)
                logger.debug('[/sparql/{}] "next" link successfully generated'.format(dataset_name))
            exportTime = (time() - start) * 1000
            stats = {"cardinalities": cardinalities, "import": loading_time, "export": exportTime}

            if mimetype == "application/sparql-results+json":
                return json.jsonify(responses.json(bindings, next_page, stats))
            if mimetype == "application/xml" or mimetype == "application/sparql-results+xml":
                return Response(responses.xml(bindings, next_page, stats), content_type="application/xml")
            return json.jsonify(responses.raw_json(bindings, next_page, stats))
        except Exception:
            abort(500)
    return sparql_blueprint
