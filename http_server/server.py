# server.py
# Author: Thomas MINIER - MIT License 2017-2018
from flask import Flask, render_template, request
from flask_cors import CORS
from database.datasets import DatasetCollection
from http_server.sparql_interface import sparql_blueprint
from http_server.void_interface import void_blueprint
from http_server.utils import secure_url


def sage_app(config_file):
    """Build a Sage application with the given configuration file"""
    datasets = DatasetCollection(config_file)
    app = Flask(__name__)
    CORS(app)

    @app.route('/')
    def index():
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
        return render_template("index_sage.html", datasets=datasets_infos, api=api_doc)

    @app.route('/software')
    def software():
        return render_template("clients.html")

    @app.route('/documentation')
    def doc():
        return render_template("documentation.html")

    app.register_blueprint(sparql_blueprint(datasets, app.logger))
    app.register_blueprint(void_blueprint(datasets, app.logger))
    return app
