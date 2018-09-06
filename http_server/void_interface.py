# void_interface.py
# Author: Thomas MINIER - MIT License 2017-2018
from flask import Blueprint, request, Response, abort
from database.descriptors import VoidDescriptor, many_void
from http_server.utils import secure_url


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


def void_blueprint(datasets, logger):
    """Get a Blueprint that provides VOID descritions of the hosted RDF datasets"""
    void_blueprint = Blueprint("void-interface", __name__)

    @void_blueprint.route("/void/", methods=["GET"])
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
            description = many_void(url, datasets, format)
            return Response(description, content_type=mimetype)
        except Exception:
            abort(500)

    @void_blueprint.route("/void/<dataset_name>", methods=["GET"])
    def void_dataset(dataset_name):
        """Describe one RDF dataset"""
        try:
            logger.debug('[/void/] Loading VoID descriptions for dataset {}'.format(dataset_name))
            dataset = datasets.get_dataset(dataset_name)
            if dataset is None:
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
            descriptor = VoidDescriptor(url, dataset)
            format, mimetype = choose_format(mimetype)
            return Response(descriptor.describe(format), content_type=mimetype)
        except Exception:
            abort(500)
    return void_blueprint
