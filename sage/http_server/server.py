# server.py
# Author: Thomas MINIER - MIT License 2017-2018
from flask import Flask, Response, render_template
from flask_cors import CORS
from sage.database.datasets import Dataset
from sage.http_server.sparql_interface import sparql_blueprint
from sage.http_server.void_interface import void_blueprint
from sage.http_server.publish_query_interface import publish_query_blueprint
from sage.http_server.utils import generate_sitemap
import datetime
import logging


def sage_app(config_file):
    """Build a Sage application with the given configuration file"""
    dataset = Dataset(config_file)
    app = Flask(__name__)
    start_date = datetime.datetime.now()
    CORS(app)

    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

    @app.route('/')
    def index():
        return render_template('index.html')

    @app.route('/sitemap.xml')
    def sitemap():
        return Response(generate_sitemap(dataset, start_date.strftime("%Y-%m-%d")), content_type="application/xml")

    app.register_blueprint(sparql_blueprint(dataset, gunicorn_logger))
    app.register_blueprint(void_blueprint(dataset, gunicorn_logger))
    app.register_blueprint(publish_query_blueprint(dataset, gunicorn_logger))
    return app
