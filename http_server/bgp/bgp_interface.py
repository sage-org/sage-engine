# bgp_interface.py
# Author: Thomas MINIER - MIT License 2017-2018
from flask import Blueprint, request, Response, render_template, abort, json
from query_engine.optimizer.plan_builder import build_left_plan
from query_engine.iterators.utils import itimeout
from http_server.bgp.schemas import BGPQuery
from http_server.utils import secure_url, format_marshmallow_errors
from time import time


def bgp_blueprint(datasets):
    """Get a Blueprint that implement a Nested Loop Join interface with deadlines on /nlj/<dataset-name>"""
    bgp_blueprint = Blueprint('bgp-ldf', __name__)

    @bgp_blueprint.route('/bgp/', methods=["GET"])
    def nlj_index():
        mimetype = request.accept_mimetypes.best_match(['application/trig', 'text/html'])
        datasets_infos = datasets._config["datasets"]
        if mimetype is 'text/html':
            return render_template("index_sage.html", datasets=datasets_infos)
        return Response(response="", content_type="application/trig")

    @bgp_blueprint.route('/bgp/<dataset_name>', methods=['GET', 'POST'])
    def get_nlj_fragment(dataset_name):
        dataset = datasets.get_dataset(dataset_name)
        if dataset is None:
            abort(404)
        mimetype = request.accept_mimetypes.best_match(['application/trig', 'text/html'])
        url = secure_url(request.url)
        # process GET request as regular TPF queries
        if request.method == "GET" or (not request.is_json):
            (subject, predicate, obj, offset, limit) = (
                request.args.get("subject", ""),
                request.args.get("predicate", ""),
                request.args.get("object", ""),
                request.args.get("offset", 0),
                request.args.get("limit", 0))
            (triples, cardinality) = dataset.search_triples(subject, predicate, obj, offset=int(offset), limit=int(limit))
            triples = list(triples)
            if mimetype is 'text/html':
                return render_template("sage.html", triples=triples, cardinality=cardinality)
            return json.jsonify(triples=triples, cardinality=cardinality)

        # else, process POST requests as NLJ requests
        post_query, errors = BGPQuery().load(request.get_json())
        if len(errors) > 0:
            return Response(status="400", response=format_marshmallow_errors(errors), content_type='text/plain')
        quota = int(request.args.get("quota", dataset.deadline()))
        bgp = post_query['bgp']
        controls = post_query['controls']
        # build physical query plan, then execute it with the given number of tickets
        start = time()
        join = build_left_plan(bgp, dataset._factory, controls=controls)
        loadingTime = (time() - start) * 1000
        bindings = list(itimeout(join, quota))
        # compute controls for the next page
        hasNext = join.has_next()
        start = time()
        controls = join.export() if hasNext else None
        exportTime = (time() - start) * 1000
        stats = {'import': loadingTime, 'export': exportTime}
        # if controls is not None:
        #     controls['hash'] = hash_controls(controls)
        return json.jsonify(bindings=bindings, cardinality=len(bindings), next=hasNext, controls=controls, stats=stats)
    return bgp_blueprint
