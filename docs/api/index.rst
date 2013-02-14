.. _api:

Core datastore API
==================

.. toctree::
   :maxdepth: 2

   key
   query
   basic
   shims
   collections
   serialize

datastore base class
--------------------

.. autoclass:: datastore.Datastore
   :members:


Examples
--------


Hello World
___________

    >>> import datastore.core
    >>> ds = datastore.DictDatastore()
    >>> hello = datastore.Key('hello')
    >>> ds.put(hello, 'world')
    >>> ds.contains(hello)
    True
    >>> ds.get(hello)
    'world'
    >>> ds.delete(hello)
    >>> ds.get(hello)
    None
