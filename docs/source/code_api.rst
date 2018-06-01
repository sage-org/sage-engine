Sage API documentation
======================

Query engine
------------

.. automodule:: query_engine.sage_engine

  .. autoclass:: SageEngine
    :members:


Database
--------

.. autoclass:: database.db_connector.DatabaseConnector
  :members:

.. autoclass:: database.hdt_file_connector.HDTFileConnector
  :inherited-members:
  :members:

.. autoclass:: database.rdf_file_connector.RDFFileConnector
  :inherited-members:
  :members:

.. autoclass:: database.rdf_index.TripleIndex
  :inherited-members:
  :members:

.. autoclass:: database.utils.TripleDictionary
  :inherited-members:
  :members:

Iterators
---------

.. autoclass:: query_engine.iterators.preemptable_iterator.PreemptableIterator
  :members:

.. autoclass:: query_engine.iterators.scan.ScanIterator
  :members:

.. autoclass:: query_engine.iterators.nlj.NestedLoopJoinIterator
  :members:

.. autoclass:: query_engine.iterators.projection.ProjectionIterator
  :members:
