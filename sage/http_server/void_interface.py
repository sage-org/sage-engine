# void_interface.py
# Author: Thomas MINIER - MIT License 2017-2018
from flask import Blueprint, request, Response, abort, redirect
from sage.database.descriptors import VoidDescriptor, many_void
from sage.http_server.utils import secure_url


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


def void_blueprint(dataset, logger):
    """Get a Blueprint that provides VOID descritions of the hosted RDF datasets"""
    v_blueprint = Blueprint("void-interface", __name__)

    @v_blueprint.route("/void/", methods=["GET"])
    def void_all():
        """Describe all RDF datasets hosted by the Sage endpoint"""
        try:
            mimetype = request.accept_mimetypes.best_match([
                "application/n-triples", "text/turtle", "application/xml",
                "application/n-quads", "application/trig",
                "application/json", "application/json+ld"
            ])
            url = secure_url(request.url_root)
            if url.endswith('/'):
                url = url[0:len(url) - 1]
            format, mimetype = choose_format(mimetype)
            description = many_void(url, dataset, format)
            return Response(description, content_type=mimetype)
        except Exception as e:
            logger.error(e)
            abort(500)

    @v_blueprint.route("/.well-known/void/", methods=["GET"])
    def void_well_known():
        return redirect("/void", code=302)

    @v_blueprint.route("/void/<graph_name>", methods=["GET"])
    def void_dataset(graph_name):
        """Describe one RDF dataset"""
        try:
            logger.debug('[/void/] Loading VoID descriptions for dataset {}'.format(graph_name))
            graph = dataset.get_graph(graph_name)
            if graph is None:
                abort(404)

            logger.debug('[/void/] Corresponding dataset found')
            mimetype = request.accept_mimetypes.best_match([
                "application/n-triples", "text/turtle", "application/xml",
                "application/n-quads", "application/trig",
                "application/json", "application/json+ld"
            ])
            url = secure_url(request.url_root)
            if url.endswith('/'):
                url = url[0:len(url) - 1]
            descriptor = VoidDescriptor(url, graph)
            format, mimetype = choose_format(mimetype)
            return Response(descriptor.describe(format), content_type=mimetype)
        except Exception as e:
            logger.error(e)
            abort(500)
    return v_blueprint
