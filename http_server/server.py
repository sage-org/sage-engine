# server.py
# Author: Thomas MINIER - MIT License 2017-2018
from markdown import markdown
from flask import Flask, Markup, render_template, request, abort
from flask_cors import CORS
from database.datasets import DatasetCollection
from http_server.sparql_interface import sparql_blueprint
from http_server.void_interface import void_blueprint
from http_server.utils import secure_url
import logging


def sage_app(config_file):
    """Build a Sage application with the given configuration file"""
    datasets = DatasetCollection(config_file)
    app = Flask(__name__)
    CORS(app)

    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

    @app.route('/')
    def index():
        try:
            datasets_infos = datasets._config["datasets"]
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
            long_description = Markup(markdown(datasets.long_description))
            return render_template("index_sage.html", datasets=datasets_infos, api=api_doc, server_public_url=datasets.public_url, long_description=long_description)
        except Exception as e:
            print(e)
            abort(500)

    @app.route('/sparql11-compliance')
    def sparql11_compliance():
        return render_template('sparql11_compliance.html')

    @app.route('/sage-voc')
    def voc():
        return app.send_static_file('sage-voc.ttl')

    @app.route('/documentation')
    def doc():
        return render_template("documentation.html")

    app.register_blueprint(sparql_blueprint(datasets, gunicorn_logger))
    app.register_blueprint(void_blueprint(datasets, gunicorn_logger))
    return app
