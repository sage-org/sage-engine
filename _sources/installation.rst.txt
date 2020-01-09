Installation
============

Installation in a `virtualenv <https://virtualenv.pypa.io/en/stable/>`__
is **strongly advised!**

| Requirements: \* `git <https://git-scm.com/>`__ \*
  `pip <https://pip.pypa.io/en/stable/>`__ \*
  `npm <https://nodejs.org/en/>`__ (shipped with Node.js on most
  systems) \* **gcc/clang** with **c++11 support** \* **Python
  Development headers** > You should have the ``Python.h`` header
  available on your system.
| > For example, for Python 3.6, install the ``python3.6-dev`` package
  on Debian/Ubuntu systems.

Installing SaGe with the HDT backend
------------------------------------

The core engine of the SaGe SPARQL query server with `HDT <http://www.rdfhdt.org/>`_ as a backend can be installed as follows:

.. code:: bash

    git clone https://github.com/sage-org/sage-engine
    cd sage-engine/
    pip install -r requirements.txt
    pip install -e .[hdt]

Installing SaGe with the PostgreSQL backend
--------------------------------------------

The SaGe SPARQL query server can also be installed with `PostgreSQL <https://www.postgresql.org/>`_ as a backend.

.. code:: bash

    pip install -e .[postgres]
