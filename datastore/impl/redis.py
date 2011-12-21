
__version__ = '1.0'
__author__ = 'Juan Batiz-Benet <jbenet@cs.stanford.edu>'
__doc__ = '''
redis datastore implementation.

Tested with:
redis 2.2.12
redis-py 2.4.10

'''

#TODO: Implement queries using a key index.
#TODO: Implement TTL (and key configurations)


import datastore


class RedisDatastore(datastore.ShimDatastore):
  '''Simple redis datastore. Does not support queries.

  The redis interface is very similar to datastore's. The only differences are:
  - values must be strings (SerializerShimDatastore)
  - keys should be converted into strings (InterfaceMappingDatastore)
  - `put` calls should be mapped to `set` (InterfaceMappingDatastore)
  '''

  def __init__(self, redis, serializer=None):
    '''Initialize the datastore with given redis client `redis`.

    Args:
      redis: A redis client to use. Must implement the basic redis
          interface: set, get, delete. This datastore keeps the interface so
          basic in order to work with any redis client (or pool of clients).
    '''
    self._redis = redis

    # use an InterfaceMappingDatastore to access the native redis interface
    mapper = datastore.InterfaceMappingDatastore(redis, put='set', key=str)

    # use a SerializerShimDatastore to ensure values are stringified
    serial = datastore.SerializerShimDatastore(mapper, serializer=serializer)

    # initialize ShimDatastore with serial as our child_datastore
    super(RedisDatastore, self).__init__(serial)

  def query(self, query):
    '''Returns an iterable of objects matching criteria expressed in `query`
    Implementations of query will be the largest differentiating factor
    amongst datastores. All datastores **must** implement query, even using
    query's worst case scenario, see Query class for details.

    Args:
      query: Query object describing the objects to return.

    Raturns:
      Cursor with all objects matching criteria
    '''
    #TODO: remember to deserialize values.
    raise NotImplementedError
