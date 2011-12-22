.. _api-key:

Keys
====

All objects stored through datastore are described by a ``key``. This key
uniquely identifies a particular object, and provides namespacing for queries.
The :py:class:`datastore.Key` class below provides the required functionality.
One can define another Key class with a different format that conforms to the
same interface (particularly stringifying, hashes, namespacing, and ancestry).

Key
---

.. autoclass:: datastore.Key
   :members:

Namespace
---------

.. autoclass:: datastore.key.Namespace
   :members:
