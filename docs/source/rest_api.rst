REST API Documentation
======================

.. http:post:: /sparql/(str:dataset_name)

   Evaluate a Basic Graph pattern query agains the dataset named ``dataset_name``.

   **Example request**:

   The following request evaluate the SPARQL query:

   .. code-block:: sparql

      SELECT *
      WHERE {
        <http://dbpedia.org/resource/Burn_After_Reading> a ?o
      }

   .. sourcecode:: http

      POST /sparql/dbpedia-3.5 HTTP/1.1
      Host: example.com
      Accept: application/json
      Content-Type: application/json

      {
        "query": {
          "type": "bgp",
          "bgp": [
            {
              "subject": "http://dbpedia.org/resource/Burn_After_Reading",
              "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
              "object": "?o"
            }
          ]
        },
        "next": null
      }

   **Example response**:

   .. sourcecode:: http

      HTTP/1.1 200 OK
      Vary: Accept
      Content-Type: application/json

      {
        "bindings": [
          {
            "?o": "http://dbpedia.org/ontology/Film"
          },
          {
            "?o": "http://schema.org/CreativeWork"
          }
        ],
        "pageSize": 2,
        "hasNext": true,
        "next": "link to the next page",
        "stats": {
          "import": 0.02,
          "export": 0.05,
          "cardinalities": [

          ]
        }
      }

   :jsonparam query: Content of the query to be evaluated
   :jsonparam next: A link used to resume query exection from a saved state.
   :reqheader Accept: the response content type depends on
                      :mailheader:`Accept` header
   :resjson json bindings: Solution bindings
   :resjson integer pageSize: Number of solution bindings in this page
   :resjson boolean hasNext: ``True`` if there is a next page after this one, ``False`` otherwise
   :resjson string next: 'Next' link that can be used to fetch the next page of solution bindings
   :resjson json stats: Some statistics about query execution
   :reqheader Authorization: optional OAuth token to authenticate
   :resheader Content-Type: this depends on :mailheader:`Accept`
                            header of request
   :statuscode 200: No error
   :statuscode 400: Input query poorly formatted
   :statuscode 500: Server internal error
