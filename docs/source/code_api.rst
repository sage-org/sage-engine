Sage API documentation
======================

Query engine
------------

.. automodule:: sage.query_engine.sage_engine

  .. autoclass:: SageEngine
    :members:


Database
--------

.. autoclass:: sage.database.db_connector.DatabaseConnector
  :members:

.. autoclass:: sage.database.hdt_file_connector.HDTFileConnector
  :inherited-members:
  :members:

Iterators
---------

.. autoclass:: sage.query_engine.iterators.preemptable_iterator.PreemptableIterator
  :members:

.. autoclass:: sage.query_engine.iterators.scan.ScanIterator
  :members:

.. autoclass:: sage.query_engine.iterators.nlj.NestedLoopJoinIterator
  :members:

.. autoclass:: sage.query_engine.iterators.projection.ProjectionIterator
  :members:
