

import json
from basic import Datastore, ShimDatastore

default_serializer = json



class Serializer(object):
  '''Serializing protocol. Serialized data must be a string.'''
  @classmethod
  def loads(cls, value):
    raise NotImplementedError

  @classmethod
  def dumps(cls, value):
    raise NotImplementedError



class NonSerializer(Serializer):
  '''Implements serializing protocol but does not serialize at all.
  If only storing strings (or already-serialized values).
  '''
  @classmethod
  def loads(cls, value):
    return value

  @classmethod
  def dumps(cls, value):
    return value



class prettyjson(Serializer):
  '''json wrapper serializer that pretty-prints.
  Useful for human readable values and versioning.
  '''

  @classmethod
  def loads(cls, value):
    return json.loads(value)

  @classmethod
  def dumps(cls, value):
    return json.dumps(value, sort_keys=True, indent=1)



def deserialized_gen(serializer, iterable):
  '''Generator that deserializes objects from `iterable`.'''
  for item in iterable:
    yield serializer.loads(item)

def serialized_gen(serializer, iterable):
  '''Generator that serializes objects from `iterable`.'''
  for item in iterable:
    yield serializer.dumps(item)



class map_serializer(Serializer):
  '''map serializer that ensures the serialized value is a mapping type.'''

  sentinel = '@wrapped'

  @classmethod
  def loads(cls, value):
    if len(value) == 1 and cls.sentinel in value:
      value = value[cls.sentinel]
    return value

  @classmethod
  def dumps(cls, value):
    if not hasattr(value, '__getitem__') or not hasattr(value, 'iteritems'):
      value = {cls.sentinel: value}
    return value


class Stack(Serializer, list):
  '''represents a stack of serializers, applying each object in a row.'''

  def loads(self, value):
    for serializer in reversed(self):
      value = serializer.loads(value)
    return value

  def dumps(self, value):
    for serializer in self:
      value = serializer.dumps(value)
    return value



class SerializerShimDatastore(ShimDatastore):
  '''Represents a Datastore that serializes and deserializes values.'''

  # value serializer
  # override this with their own custom serializer on a class-wide or per-
  # instance basis. If you plan to store mostly strings, use NonSerializer.
  serializer = default_serializer

  def __init__(self, datastore, serializer=None):
    '''Initializes internals and tests the serializer.'''
    super(SerializerShimDatastore, self).__init__(datastore)

    if serializer:
      self.serializer = serializer

    # ensure serializer works
    test = { 'value': repr(self) }
    errstr = 'Serializer error: serialized value does not match original'
    assert self.serializer.loads(self.serializer.dumps(test)) == test, errstr


  def serializedValue(self, value):
    '''Returns serialized `value` or None.'''
    return self.serializer.dumps(value) if value is not None else None

  def deserializedValue(self, value):
    '''Returns deserialized `value` or None.'''
    return self.serializer.loads(value) if value is not None else None


  def get(self, key):
    '''De-serializes values on the way out.'''
    value = self.child_datastore.get(key)
    return self.deserializedValue(value)

  def put(self, key, value):
    '''Serializes values on the way in.'''
    value = self.serializedValue(value)
    self.child_datastore.put(key, value)

  def query(self, query):
    '''De-serializes values on the way out.'''
    # run the query on the child datastore
    cursor = self.child_datastore.query(query)

    # chain the deserializing generator to the cursor's result set iterable
    cursor._iterable = deserialized_gen(self.serializer, cursor._iterable)

    return cursor
