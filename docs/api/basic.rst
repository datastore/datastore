.. _api-key:

Basic Datastores
================


DictDatastore
-------------

.. autoclass:: datastore.DictDatastore
   :members:

Example:

    >>> import pprint
    >>> from datastore import DictDatastore, Key, Query
    >>> ds = DictDatastore()
    >>> for i in range(0, 3):
    ...   key = Key('/%d' % i)
    ...   ds.put(key.child('A'), '%d a value' % i)
    ...   ds.put(key.child('B'), '%d b value' % i)
    ...
    >>> pprint.pprint(ds._items)
    {'/0': {Key('/0/A'): '0 a value', Key('/0/B'): '0 b value'},
     '/1': {Key('/1/A'): '1 a value', Key('/1/B'): '1 b value'},
     '/2': {Key('/2/A'): '2 a value', Key('/2/B'): '2 b value'}}
    >>> ds.get(Key('/1/A'))
    '1 a value'
    >>> for item in ds.query(Query(Key('/2'))):
    ...   print item
    ...
    2 b value
    2 a value



InterfaceMappingDatastore
-------------------------

.. autoclass:: datastore.InterfaceMappingDatastore
   :members:

Example:

    >>> import pylibmc
    >>> from datastore import InterfaceMappingDatastore, Key
    >>> mc = pylibmc.Client(['127.0.0.1'])
    >>> mc_ds = InterfaceMappingDatastore(mc, put='set', key=str)
    >>> mc_ds.put(Key('Hello'), 'World')
    >>> mc_ds.get(Key('Hello'))
    'World'
    >>> mc.get('/Hello')
    'World'
    >>> mc.set('/Hello', 'Goodbye!')
    True
    >>> mc_ds.get(Key('/Hello'))
    'Goodbye!'