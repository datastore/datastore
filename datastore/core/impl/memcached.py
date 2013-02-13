
__version__ = '1.0'
__author__ = 'Juan Batiz-Benet <juan@benet.ai>'
__doc__ = '''
memcached datastore implementation.

Tested with:
  * memcached 1.4.5
  * libmemcached 0.50
  * pylibmc 1.2.2

'''

#TODO: Implements queries using a key index.
#TODO: Implement TTL (and key configurations)

import datastore


class MemcachedDatastore(datastore.InterfaceMappingDatastore):
  '''Flat memcached datastore. Does not support queries.

  This datastore is implemented as an InterfaceMappingDatastore, as the
  memcached interface is very similar to datastore's.

  The only differences (which InterfaceMappingDatastore takes care of) are:
  - keys should be converted into strings
  - `put` calls should be mapped to `set`
  '''

  def __init__(self, memcached):
    '''Initialize the datastore with given memcached client `memcached`.

    Args:
      memcached: A memcached client to use. Must implement the basic memcached
          interface: set, get, delete. This datastore keeps the interface so
          basic in order to work with any memcached client (or pool of clients).
    '''
    super(MemcachedDatastore, self).__init__(memcached, put='set', key=str)

  def query(self, query):
    '''Returns an iterable of objects matching criteria expressed in `query`

    Args:
      query: Query object describing the objects to return.

    Raturns:
      Cursor with all objects matching criteria
    '''
    #TODO
    raise NotImplementedError

'''
Hello World:

    >>> import pylibmc
    >>> import datastore
    >>> from datastore.impl.memcached import MemcachedDatastore
    >>>
    >>> mc = pylibmc.Client(['127.0.0.1'])
    >>> ds = MemcachedDatastore(mc)
    >>>
    >>> hello = datastore.Key('hello')
    >>> ds.put(hello, 'world')
    >>> ds.contains(hello)
    True
    >>> ds.get(hello)
    'world'
    >>> ds.delete(hello)
    >>> ds.get(hello)
    None

'''
