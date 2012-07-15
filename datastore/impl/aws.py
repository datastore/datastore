
__version__ = '1.0'
__author__ = 'Juan Batiz-Benet <juan@benet.ai>'
__doc__ = '''
aws datastore implementation.

Tested with:
* boto 2.5.2

'''

#TODO: Implement queries using a key index.
#TODO: Implement TTL (and key configurations)


from boto.s3.key import Key as S3Key
from boto.exception import S3ResponseError

import datastore


class S3BucketDatastore(datastore.Datastore):
  '''Simple aws s3 datastore. Does not support queries.

  The s3 interface is very similar to datastore's. The only differences are:
  - values must be strings (SerializerShimDatastore)
  - keys must be converted into strings
  '''

  def __init__(self, s3bucket):
    '''Initialize the datastore with given s3 bucket `s3bucket`.

    Args:
      s3bucket: An s3 bucket to use.

    Example::

      from boto.s3.connection import S3Connection
      s3conn = S3Connection('<aws access key>', '<aws secret key>')
      s3bucket = s3conn.get_bucket('<bucket name>')
      s3ds = S3BucketDatastore(s3bucket)
    '''
    self._s3bucket = s3bucket

  def _s3key(self, key):
    '''Return an s3 key for given datastore key.'''
    k = S3Key(self._s3bucket)
    k.key = str(key)
    return k

  @classmethod
  def _s3keys_get_contents_as_string_gen(cls, s3keys):
    '''s3 content retriever generator.'''
    for s3key in s3keys:
      yield s3key.get_contents_as_string()


  def get(self, key):
    '''Return the object named by key or None if it does not exist.

    Args:
      key: Key naming the object to retrieve

    Returns:
      object or None
    '''
    try:
      return self._s3key(key).get_contents_as_string()
    except S3ResponseError, e:
      return None

  def put(self, key, value):
    '''Stores the object `value` named by `key`.

    Args:
      key: Key naming `value`
      value: the object to store.
    '''
    self._s3key(key).set_contents_from_string(value)

  def delete(self, key):
    '''Removes the object named by `key`.

    Args:
      key: Key naming the object to remove.
    '''
    self._s3key(key).delete()

  def query(self, query):
    '''Returns an iterable of objects matching criteria expressed in `query`

    Implementations of query will be the largest differentiating factor
    amongst datastores. All datastores **must** implement query, even using
    query's worst case scenario, see :ref:class:`Query` for details.

    Args:
      query: Query object describing the objects to return.

    Raturns:
      iterable cursor with all objects matching criteria
    '''
    allkeys = self._s3bucket.list(prefix=str(query.key).strip('/'))
    iterable = self._s3keys_get_contents_as_string_gen(allkeys)
    return query(iterable) # must apply filters, order, etc naively.


  def contains(self, key):
    '''Returns whether the object named by `key` exists.

    Args:
      key: Key naming the object to check.

    Returns:
      boalean whether the object exists
    '''
    return self._s3key(key).exists()


'''
Hello World:

    >>> import datastore
    >>> from datastore.impl.aws import S3BucketDatastore
    >>> from boto.s3.connection import S3Connection
    >>>
    >>> s3conn = S3Connection('<aws access key>', '<aws secret key>')
    >>> s3bucket = s3conn.get_bucket('<bucket name>')
    >>> ds = S3BucketDatastore(s3bucket)
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
