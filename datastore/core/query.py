
from key import Key


def _object_getattr(obj, field):
  '''Attribute getter for the objects to operate on.

  This function can be overridden in classes or instances of Query, Filter, and
  Order. Thus, a custom function to extract values to attributes can be
  specified, and the system can remain agnostic to the client's data model,
  without loosing query power.

  For example, the default implementation works with attributes and items::

    def _object_getattr(obj, field):
      # check whether this key is an attribute
      if hasattr(obj, field):
        value = getattr(obj, field)

      # if not, perhaps it is an item (raw dicts, etc)
      elif field in obj:
        value = obj[field]

      # return whatever we've got.
      return value

  Or consider a more complex, application-specific structure::

    def _object_getattr(version, field):

      if field in ['key', 'committed', 'created', 'hash']:
        return getattr(version, field)

      else:
        return version.attributes[field]['value']


  '''

  # TODO: consider changing this to raise an exception if no value is found.
  value = None

  # check whether this key is an attribute
  if hasattr(obj, field):
    value = getattr(obj, field)

  # if not, perhaps it is an item (raw dicts, etc)
  elif field in obj:
    value = obj[field]

  # return whatever we've got.
  return value




def limit_gen(limit, iterable):
  '''A generator that applies a count `limit`.'''
  limit = int(limit)
  assert limit >= 0, 'negative limit'

  for item in iterable:
    if limit <= 0:
      break
    yield item
    limit -= 1


def offset_gen(offset, iterable, skip_signal=None):
  '''A generator that applies an `offset`, skipping `offset` elements from
  `iterable`. If skip_signal is a callable, it will be called with every
  skipped element.
  '''
  offset = int(offset)
  assert offset >= 0, 'negative offset'

  for item in iterable:
    if offset > 0:
      offset -= 1
      if callable(skip_signal):
        skip_signal(item)
    else:
      yield item


def chain_gen(iterables):
  '''A generator that chains `iterables`.'''
  for iterable in iterables:
    for item in iterable:
      yield item




class Filter(object):
  '''Represents a Filter for a specific field and its value.

  Filters are used on queries to narrow down the set of matching objects.

  Args:
    field: the attribute name (string) on which to apply the filter.

    op: the conditional operator to apply (one of
        ['<', '<=', '=', '!=', '>=', '>']).

    value: the attribute value to compare against.

  Examples::

    Filter('name', '=', 'John Cleese')
    Filter('age', '>=', 18)

  '''

  conditional_operators = ['<', '<=', '=', '!=', '>=', '>']
  '''Conditional operators that Filters support.'''

  _conditional_cmp = {
    "<"  : lambda a, b: a < b,
    "<=" : lambda a, b: a <= b,
    "="  : lambda a, b: a == b,
    "!=" : lambda a, b: a != b,
    ">=" : lambda a, b: a >= b,
    ">"  : lambda a, b: a > b
  }


  object_getattr = staticmethod(_object_getattr)
  '''Object attribute getter. Can be overridden to match client data model.
  See :py:meth:`datastore.query._object_getattr`.
  '''

  def __init__(self, field, op, value):
    if op not in self.conditional_operators:
      raise ValueError('"%s" is not a valid filter Conditional Operator' % op)

    self.field = field
    self.op = op
    self.value = value

  def __call__(self, obj):
    '''Returns whether this object passes this filter.
    This method aggressively tries to find the appropriate value.
    '''
    value = self.object_getattr(obj, self.field)

    # TODO: which way should the direction go here? it may make more sense to
    #       convert the passed-in value instead. Or try both? Or not at all?
    if not isinstance(value, self.value.__class__) and not self.value is None and not value is None:
      value = self.value.__class__(value)

    return self.valuePasses(value)

  def valuePasses(self, value):
    '''Returns whether this value passes this filter'''
    return self._conditional_cmp[self.op](value, self.value)


  def __str__(self):
    return '%s %s %s' % (self.field, self.op, self.value)

  def __repr__(self):
    return "Filter('%s', '%s', %s)" % (self.field, self.op, repr(self.value))


  def __eq__(self, o):
    return self.field == o.field and self.op == o.op and self.value == o.value

  def __ne__(self, other):
    return not self.__eq__(other)

  def __hash__(self):
    return hash(repr(self))

  def generator(self, iterable):
    '''Generator function that iteratively filters given `items`.'''
    for item in iterable:
      if self(item):
        yield item

  @classmethod
  def filter(cls, filters, iterable):
    '''Returns the elements in `iterable` that pass given `filters`'''
    if isinstance(filters, Filter):
      filters = [filters]

    for filter in filters:
      iterable = filter.generator(iterable)

    return iterable





class Order(object):
  '''Represents an Order upon a specific field, and a direction.
  Orders are used on queries to define how they operate on objects

  Args:
    order: an order in string form. This follows the format: [+-]name
           where + is ascending, - is descending, and name is the name
           of the field to order by.
           Note: if no ordering operator is specified, + is default.

  Examples::

    Order('+name')   #  ascending order by name
    Order('-age')    # descending order by age
    Order('score')   #  ascending order by score

  '''

  order_operators = ['-', '+']
  '''Ordering operators: + is ascending, - is descending.'''

  object_getattr = staticmethod(_object_getattr)
  '''Object attribute getter. Can be overridden to match client data model.
  See :py:meth:`datastore.query._object_getattr`.
  '''

  def __init__(self, order):
    self.op = '+'

    try:
      if order[0] in self.order_operators:
        self.op = order[0]
        order = order[1:]
    except IndexError:
      raise ValueError('Order input be at least two characters long.')

    self.field = order

    if self.op not in self.order_operators:
      raise ValueError('"%s" is not a valid Order Operator.' % op)


  def __str__(self):
    return '%s%s' % (self.op, self.field)

  def __repr__(self):
    return "Order('%s%s')" % (self.op, self.field)


  def __eq__(self, other):
    return self.field == other.field and self.op == other.op

  def __ne__(self, other):
    return not self.__eq__(other)

  def __hash__(self):
    return hash(repr(self))


  def isAscending(self):
    return self.op == '+'

  def isDescending(self):
    return not self.isAscending()


  def keyfn(self, obj):
    '''A key function to be used in pythonic sort operations.'''
    return self.object_getattr(obj, self.field)

  @classmethod
  def multipleOrderComparison(cls, orders):
    '''Returns a function that will compare two items according to `orders`'''
    comparers = [ (o.keyfn, 1 if o.isAscending() else -1) for o in orders]

    def cmpfn(a, b):
      for keyfn, ascOrDesc in comparers:
        comparison = cmp(keyfn(a), keyfn(b)) * ascOrDesc
        if comparison is not 0:
          return comparison
      return 0

    return cmpfn

  @classmethod
  def sorted(cls, items, orders):
    '''Returns the elements in `items` sorted according to `orders`'''
    return sorted(items, cmp=cls.multipleOrderComparison(orders))




class Query(object):
  '''A Query describes a set of objects.

  Queries are used to retrieve objects and instances matching a set of criteria
  from Datastores. Query objects themselves are simply descriptions,
  the actual Query implementations are left up to the Datastores.
  '''

  '''Object attribute getter. Can be overridden to match client data model.'''
  object_getattr = staticmethod(_object_getattr)

  def __init__(self, key=Key('/'), limit=None, offset=0, offset_key=None, object_getattr=None):
    ''' Initialize a query.

    Parameters
      key: a key representing the level of this query. For example, a Query with
           Key('/MontyPython/Actor:') would query objects in that key path, eg:
             Key('/MontyPython/Actor:JohnCleese')
             Key('/MontyPython/Actor:EricIdle')
             Key('/MontyPython/Actor:GrahamChapman')

            It is up to datastores how to implement this namespacing. E.g.,
            some datastores may store values in different tables or collections.

      limit: an integer representing the maximum number of results to return.

      offset: an integer representing a number of results to skip.

      object_getattr: a function to extract attribute values from an object. It
           is used to satisfy query filters and orders. Defining this function
           allows the client to control the data model of the stored values.
           The default function attempts to access values as attributes
           (__getattr__) or items (__getitem__).
    '''
    if not isinstance(key, Key):
      raise TypeError('key must be of type %s' % Key)

    self.key = key

    self.limit = int(limit) if limit is not None else None
    self.offset = int(offset)
    self.offset_key = offset_key

    self.filters = []
    self.orders = []

    if object_getattr:
      self.object_getattr = object_getattr

  def __str__(self):
    '''Returns a string describing this query.'''
    return repr(self)

  def __repr__(self):
    '''Returns the representation of this query. Enables eval(repr(.)).'''
    return 'Query.from_dict(%s)' % self.dict()

  def __call__(self, iterable):
    '''Naively apply this query on an iterable of objects.
    Applying a query applies filters, sorts by appropriate orders, and returns
    a limited set.

    WARNING: When orders are applied, this function operates on the entire set
             of entities directly, not just iterators/generators. That means
             the entire result set will be in memory. Datastores with large
             objects and large query results should translate the Query and
             perform their own optimizations.
    '''

    cursor = Cursor(self, iterable)
    cursor.apply_filter()
    cursor.apply_order()
    cursor.apply_offset()
    cursor.apply_limit()
    return cursor


  def order(self, order):
    '''Adds an Order to this query.

    Args:
      see :py:class:`Order <datastore.query.Order>` constructor

    Returns self for JS-like method chaining::

      query.order('+age').order('-home')

    '''
    order = order if isinstance(order, Order) else Order(order)

    # ensure order gets attr values the same way the rest of the query does.
    order.object_getattr = self.object_getattr
    self.orders.append(order)
    return self # for chaining


  def filter(self, *args):
    '''Adds a Filter to this query.

    Args:
      see :py:class:`Filter <datastore.query.Filter>` constructor

    Returns self for JS-like method chaining::

      query.filter('age', '>', 18).filter('sex', '=', 'Female')

    '''
    if len(args) == 1 and isinstance(args[0], Filter):
      filter = args[0]
    else:
      filter = Filter(*args)

    # ensure filter gets attr values the same way the rest of the query does.
    filter.object_getattr = self.object_getattr
    self.filters.append(filter)
    return self # for chaining


  def __cmp__(self, other):
    return cmp(self.dict(), other.dict())

  def __hash__(self):
    return hash(repr(self))

  def copy(self):
    '''Returns a copy of this query.'''
    if self.object_getattr is Query.object_getattr:
      other = Query(self.key)
    else:
      other = Query(self.key, object_getattr=self.object_getattr)
    other.limit = self.limit
    other.offset = self.offset
    other.offset_key = self.offset_key
    other.filters = self.filters
    other.orders = self.orders
    return other

  def dict(self):
    '''Returns a dictionary representing this query.'''
    d = dict()
    d['key'] = str(self.key)

    if self.limit is not None:
      d['limit'] = self.limit
    if self.offset > 0:
      d['offset'] = self.offset
    if self.offset_key:
      d['offset_key'] = str(self.offset_key)
    if len(self.filters) > 0:
      d['filter'] = [[f.field, f.op, f.value] for f in self.filters]
    if len(self.orders) > 0:
      d['order'] = [str(o) for o in self.orders]

    return d

  @classmethod
  def from_dict(cls, dictionary):
    '''Constructs a query from a dictionary.'''
    query = cls(Key(dictionary['key']))

    for key, value in dictionary.items():

      if key == 'order':
        for order in value:
          query.order(order)

      elif key == 'filter':
        for filter in value:
          if not isinstance(filter, Filter):
            filter = Filter(*filter)
          query.filter(filter)

      elif key in ['limit', 'offset', 'offset_key']:
        setattr(query, key, value)
    return query



def is_iterable(obj):
  return hasattr(obj, '__iter__') or hasattr(obj, '__getitem__')


class Cursor(object):
  '''Represents a query result generator.'''

  __slots__ = ('query', '_iterable', '_iterator', 'skipped', 'returned', )

  def __init__(self, query, iterable):
    if not isinstance(query, Query):
      raise ValueError('Cursor received invalid query: %s' % query)

    if not is_iterable(iterable):
      raise ValueError('Cursor received invalid iterable: %s' % iterable)

    self.query = query
    self._iterable = iterable
    self._iterator = None
    self.returned = 0
    self.skipped = 0


  def __iter__(self):
    '''The cursor itself is the iterator. Note that it cannot be used twice,
    and once iteration starts, the cursor cannot be modified.
    '''
    if self._iterator:
      raise RuntimeError('Attempt to iterate over Cursor twice.')

    self._iterator = iter(self._iterable)
    return self

  def next(self):
    '''Iterator next. Build up count of returned elements during iteration.'''

    # if iteration has not begun, begin it.
    if not self._iterator:
      self.__iter__()

    next = self._iterator.next()
    if next is not StopIteration:
      self._returned_inc(next)
    return next


  def _skipped_inc(self, item):
    '''A function to increment the skipped count.'''
    self.skipped += 1

  def _returned_inc(self, item):
    '''A function to increment the returned count.'''
    self.returned += 1


  def _ensure_modification_is_safe(self):
    '''Assertions to ensure modification of this Cursor is safe.'''
    assert self.query, 'Cursor must have a Query.'
    assert is_iterable(self._iterable), 'Cursor must have a resultset iterable.'
    assert not self._iterator, 'Cursor must not be modified after iteration.'


  def apply_filter(self):
    '''Naively apply query filters.'''
    self._ensure_modification_is_safe()

    if len(self.query.filters) > 0:
      self._iterable = Filter.filter(self.query.filters, self._iterable)

  def apply_order(self):
    '''Naively apply query orders.'''
    self._ensure_modification_is_safe()

    if len(self.query.orders) > 0:
      self._iterable = Order.sorted(self._iterable, self.query.orders)
      # not a generator :(

  def apply_offset(self):
    '''Naively apply query offset.'''
    self._ensure_modification_is_safe()

    if self.query.offset != 0:
      self._iterable = \
        offset_gen(self.query.offset, self._iterable, self._skipped_inc)
        # _skipped_inc helps keep count of skipped elements

  def apply_limit(self):
    '''Naively apply query limit.'''
    self._ensure_modification_is_safe()

    if self.query.limit is not None:
      self._iterable = limit_gen(self.query.limit, self._iterable)

