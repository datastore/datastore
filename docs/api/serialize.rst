.. _api-serialize:

Serialize
=========

Serializing shemes are often application-specific, and thus libraries often
avoid imposing one. At the same time, it would be ideal if a variety of
serializers were available and trivially pluggable into the data storage
pattern through a simple interface.

The :py:mod:`pickle` protocol has established the
:py:meth:`pickle.loads` and :py:meth:`pickle.dumps` serialization interface,
which other serializers (like :py:mod:`json`) have adopted. This significantly
simplifies things, but specific calls to serializers need to be added whenever
inserting of extracting data with data storage libraries.

To flexibly solve this issue, datastore defines a
:py:class:`datastore.SerializerShimDatastore` which can be layered on top of
any other datastore.
As data is ``put``, the serializer shim serializes it and ``put``s it into
the underlying ``child_datastore``. Correspondingly, on the way out (through
``get`` or ``query``) the data is retrieved from the ``child_datastore`` and
deserialized.


SerializerShimDatastore
_______________________

.. autoclass:: datastore.serialize.SerializerShimDatastore
   :members:

datastore.serialize.shim
------------------------

.. autofunction:: datastore.serialize.shim


Serializers
___________

The serializers that :py:class:`datastore.SerializerShimDatastore` accepts
must respond to the protocol outlined in
:py:class:`datastore.serialize.Serializer` (the :py:mod:`pickle` protocol).

serialize.Serializer
--------------------

.. autoclass:: datastore.serialize.Serializer
   :members:

serialize.NonSerializer
-----------------------

.. autoclass:: datastore.serialize.NonSerializer
   :members:

serialize.prettyjson
--------------------

.. autoclass:: datastore.serialize.prettyjson
   :members:

serialize.Stack
---------------

.. autoclass:: datastore.serialize.Stack
   :members:

serialize.map_serializer
------------------------

.. autoclass:: datastore.serialize.map_serializer
   :members:


Generators
----------

.. autofunction:: datastore.serialize.deserialized_gen

.. autofunction:: datastore.serialize.serialized_gen


Examples
________

TODO
