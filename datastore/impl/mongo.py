
import bson
import pymongo
import datastore


__version__ = '1.2'
__author__ = 'Juan Batiz-Benet <jbenet@cs.stanford.edu>'


class Doc(object):
  '''Document key constants for datastore documents.'''
  _id = '_id'
  key = 'key'
  value = 'val'
  wrapped = '_wrapped'


class MongoDatastore(datastore.Datastore):
  '''Represents a Mongo database as a datastore.'''

  def __init__(self, mongoDatabase):
    self.database = mongoDatabase
    self._indexed = {}

  def _collectionNamed(self, name):
    '''Returns the `collection` named `name`.'''

    collection = self.database[name]

    # ensure there is an index, at least once per run.
    if name not in self._indexed:
      collection.create_index(Doc.key, unique=True)
      self._indexed[name] = True

    return collection

  @staticmethod
  def _collectionNameForKey(key):
    '''Returns the name of the collection to house objects with `key`.
    Users can override this function to enforce their own collection naming.
    '''
    name = str(key.path)[1:] # remove first slash.
    name = name.replace(':', '_')
    name = name.replace('/', '.')
    return name


  def _collection(self, key):
    '''Returns the `collection` corresponding to `key`.'''
    return self._collectionNamed(self._collectionNameForKey(key))

  @staticmethod
  def _wrap(key, value):
    '''Returns a value to insert. Non-documents are wrapped in a document.'''
    if not isinstance(value, dict) or Doc.key not in value or value[Doc.key] != key:
      return { Doc.key:key, Doc.value:value, Doc.wrapped:True}

    #TODO is this necessary?
    if Doc._id in value:
      del value[Doc._id]

    return value

  @staticmethod
  def _unwrap(value):
    '''Returns a value to return. Wrapped-documents are unwrapped.'''
    if value is not None and Doc.wrapped in value and value[Doc.wrapped]:
      return value[Doc.value]

    if isinstance(value, dict) and Doc._id in value:
      del value[Doc._id]

    return value


  def get(self, key):
    '''Return the object named by key.'''
    # query the corresponding mongodb collection for this key
    value = self._collection(key).find_one( { Doc.key:str(key) } )
    return self._unwrap(value)

  def put(self, key, value):
    '''Stores the object.'''
    strkey = str(key)
    value = self._wrap(strkey, value)

    # update (or insert) the relevant document matching key
    self._collection(key).update( { Doc.key:strkey }, value, upsert=True, safe=True)

  def delete(self, key):
    '''Removes the object.'''
    self._collection(key).remove( { Doc.key:str(key) } )

  def contains(self, key):
    '''Returns whether the object is in this datastore.'''
    return self._collection(key).find( { Doc.key:str(key) } ).count() > 0

  def query(self, query):
    '''Returns a sequence of objects matching criteria expressed in `query`'''
    coll = self._collection(query.key.child('_'))
    return MongoQuery.translate(coll, query)




def unwrapper_gen(iterable):
  '''A generator to unwrap results in `iterable`.'''
  for item in iterable:
    yield MongoDatastore._unwrap(item)




class MongoQuery(object):
  '''Translates queries from dronestore queries to mongodb queries.'''
  operators = { '>':'$gt', '>=':'$gte', '!=':'$ne', '<=':'$lte', '<':'$lt' }

  @classmethod
  def translate(self, collection, query):
    '''Translate given datastore `query` to a mongodb query on `collection`.'''

    # must call find
    mongo_cursor = collection.find(self.filters(query.filters))

    if len(query.orders) > 0:
      mongo_cursor.sort(self.orders(query.orders))

    if query.offset > 0:
      mongo_cursor.skip(query.offset)

    # must execute before the limit to make sure the counts yield only skipped.
    skipped = mongo_cursor.count() - mongo_cursor.count(with_limit_and_skip=True)

    if query.limit:
      mongo_cursor.limit(query.limit)

    # make sure to unwrap all retrieved values
    iterable = unwrapper_gen(mongo_cursor)

    # create datastore Cursor with query and iterable of results
    datastore_cursor = datastore.Cursor(query, iterable)
    datastore_cursor.skipped = skipped
    return datastore_cursor

  @classmethod
  def filter(cls, filter):
    '''Transform given `filter` into a mongodb filter tuple.'''
    if filter.op == '=':
      return filter.field, filter.value
    return filter.filter, { cls.operators[filter.op] : filter.value }

  @classmethod
  def filters(cls, filters):
    '''Transform given `filters` into a mongodb filter dictionary.'''
    return dict([cls.filter(f) for f in filters])

  @classmethod
  def orders(cls, orders):
    '''Transform given `orders` into a mongodb order list.'''
    keys = [cls.field(o.field) for o in orders]
    vals = [1 if o.isAscending() else -1 for o in orders]
    return zip(keys, vals)
