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
import os


def sage_app(config_file):
    """Build a Sage application with the given configuration file"""
    datasets = DatasetCollection(config_file)
    app = Flask(__name__)
    CORS(app)

    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

    @app.context_processor
    def inject_user():
        config = dict()
        if "google_analytics" in datasets._config:
            config["google_analytics"] = datasets._config["google_analytics"]
        return dict(config=config)

    @app.route('/')
    def index():
        try:
            url = secure_url(request.url)
            dinfos = [dinfo for dinfo in datasets.describe(url)]
            long_description = Markup(markdown(datasets.long_description))
            return render_template("index_sage.html", datasets=dinfos, server_public_url=datasets.public_url, default_query=datasets.default_query, long_description=long_description)
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

    @app.route('/api')
    def open_api():
        return app.send_static_file('api.yaml')

    @app.route('/specs')
    def specs():
        try:
            specs = {
                'load_avg': os.getloadavg(),
                'cpu_count': os.cpu_count()
            }
            return render_template("specs.html", specs=specs)
        except Exception:
            abort(500)

    app.register_blueprint(sparql_blueprint(datasets, gunicorn_logger))
    app.register_blueprint(void_blueprint(datasets, gunicorn_logger))
    return app
