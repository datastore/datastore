.. _api-shims:

Shims
=====

Sometimes common functionality can be compartmentalized into logic that can
be plugged in or not. For example, serializing and deserializing data as it is
stored or extracted is a very common operation. Likewise, applications may need
to perform routine operations as data makes its way from the top-level logic to
the underlying storage.

To address this use case in an elegant way, datastore uses the notion of a
``shim`` datastore, which implements all four main
:ref:`datastore operations <api-datastore>` in terms of an underlying child
datastore. For example, a json serializer datastore could implement ``get``
and ``put`` as::

    def get(self, key):
      value = self.child_datastore.get(key)
      return json.loads(value)

    def put(self, key, value):
      value = json.dumps(value)
      self.child_datastore.put(key, value)


ShimDatastore
-------------

To implement a shim datastore, derive from :py:class:`datastore.ShimDatastore`
and override any of the operations.

.. autoclass:: datastore.ShimDatastore
   :members:


KeyTransformDatastore
---------------------

.. autoclass:: datastore.KeyTransformDatastore
   :members:

LowercaseKeyDatastore
---------------------

.. autoclass:: datastore.LowercaseKeyDatastore
   :members:

NamespaceDatastore
---------------------

.. autoclass:: datastore.NamespaceDatastore
   :members:

SymlinkDatastore
---------------------

.. autoclass:: datastore.SymlinkDatastore
   :members:
