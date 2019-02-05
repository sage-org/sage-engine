# publish_query_interface.py
# Author: Thomas MINIER - MIT License 2017-2018
from flask import Blueprint, render_template, abort, url_for
from json import dumps


def publish_query_blueprint(dataset, logger):
    """Get a Blueprint that publish queries as RDF datasets"""
    pq_blueprint = Blueprint("publish-query-interface", __name__)

    @pq_blueprint.route("/query/<graph_name>/<query_name>", methods=["GET"])
    def publish_query(graph_name, query_name):
        """Get a SPARQL query as a RDF dataset"""
        try:
            if not dataset.has_graph(graph_name):
                abort(404)
            graph = dataset.get_graph(graph_name)
            query = graph.get_query(query_name)
            # query that are not designed to be published cannot be accessed
            if query is None or not query['publish']:
                abort(404)
            # compute json-ld description of the query as a dataset
            query_url = url_for("sparql-interface.sparql_index", query=query['value'].replace("\n", " "), _external=True) + "&default-graph-uri=" + url_for('sparql-interface.sparql_index', _external=True) + '/' + graph_name
            jsonld_description = {
                "@context": "http://schema.org/",
                "@type": "Dataset",
                "name": query["name"],
                "description": query["description"],
                "comment": query['value'].replace("\n", " ").replace("\t", "").replace("\r", ""),
                "isBasedOn": url_for('sparql-interface.sparql_query', graph_name=graph_name, _external=True),
                "distribution": [
                    {
                        "@type": "DataDownload",
                        "encodingFormat": "text/html",
                        "contentUrl": query_url,
                        "accessMode": "Paginated results"
                    }
                ],
                "creator": {
                     "@type": "Organization",
                     "url": "https://sites.google.com/site/gddlina",
                     "name": "Distributed Data Management team (LS2N, University of Nantes)",
                     "alternateName": "GDD Team",
                     "description": "GDD is a research group of LS2N, University of Nantes. GDD is working in federated distributed systems following 3 research directions: 1) federated data structures and consistency, 2) collaborative data sharing in federations, and 3) security and usage control in federations.",
                     "memberOf": {
                        "@type": "Organization",
                        "url": "https://www.ls2n.fr/?lang=en",
                        "name": "Laboratoire des Sciences du Numerique de Nantes",
                        "alternateName": "LS2N",
                        "logo": "https://www.ls2n.fr/wp-content/themes/dk-resp-ls2n/images/logo_LS2N_bf.jpg"
                     }
                  }
            }
            if "keywords" in query:
                jsonld_description["keywords"] = query["keywords"]
            return render_template('query.html', query=query, graph_name=graph_name, description=dumps(jsonld_description, indent=2), query_url=query_url)
        except Exception as e:
            logger.error(e)
            abort(500)
    return pq_blueprint
