.. sage-engine documentation master file, created by
   sphinx-quickstart on Fri Jun  1 08:28:51 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

The SaGe query engine
=======================================

`SaGe <http://sage.univ-nantes.fr/>`_ is a SPARQL query engine for public Linked Data providers
that implements Web preemption. The SPARQL engine includes a smart
SaGe client and a SaGe SPARQL query server hosting RDF datasets.
SPARQL queries are suspended by the web server after a fixed
quantum of time and resumed upon client request.
Using Web preemption, SaGe ensures stable response times
for query execution and completeness of results under high load.

The complete approach and experimental results are available
in a Research paper accepted at The Web Conference 2019, `available here <https://hal.archives-ouvertes.fr/hal-02017155/document>`_.
*Thomas Minier, Hala Skaf-Molli and Pascal Molli.
"SaGe: Web Preemption for Public SPARQL Query services"
in Proceedings of the 2019 World Wide Web Conference (WWW'19),
San Francisco, USA, May 13-17, 2019*.

We appreciate your feedback/comments/questions
to be sent to our `mailing list <sage@univ-nantes.fr>`_
or `our issue tracker on github <https://github.com/sage-org/sage-engine/issues>`_.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   sage



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

