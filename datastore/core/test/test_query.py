import time
import datetime
import unittest
import hashlib
import nanotime

from ..key import Key
from ..query import Filter, Order, Query, Cursor



def version_objects():
  sr1 = {}
  sr1['key'] = '/ABCD'
  sr1['hash'] = hashlib.sha1('herp').hexdigest()
  sr1['parent'] = '0000000000000000000000000000000000000000'
  sr1['created'] = nanotime.now().nanoseconds()
  sr1['committed'] = nanotime.now().nanoseconds()
  sr1['attributes'] = {'str' : {'value' : 'herp'} }
  sr1['type'] = 'Hurr'

  sr2 = {}
  sr2['key'] = '/ABCD'
  sr2['hash'] = hashlib.sha1('derp').hexdigest()
  sr2['parent'] = hashlib.sha1('herp').hexdigest()
  sr2['created'] = nanotime.now().nanoseconds()
  sr2['committed'] = nanotime.now().nanoseconds()
  sr2['attributes'] = {'str' : {'value' : 'derp'} }
  sr2['type'] = 'Hurr'

  sr3 = {}
  sr3['key'] = '/ABCD'
  sr3['hash'] = hashlib.sha1('lerp').hexdigest()
  sr3['parent'] = hashlib.sha1('derp').hexdigest()
  sr3['created'] = nanotime.now().nanoseconds()
  sr3['committed'] = nanotime.now().nanoseconds()
  sr3['attributes'] = {'str' : {'value' : 'lerp'} }
  sr3['type'] = 'Hurr'

  return sr1, sr2, sr3


class TestFilter(unittest.TestCase):

  def assertFilter(self, filter, objects, match):
    result = [o for o in Filter.filter(filter, objects)]
    self.assertEqual(result, match)

  def test_basic(self):

    v1, v2, v3 = version_objects()
    vs = [v1, v2, v3]

    t1 = v1['committed']
    t2 = v2['committed']
    t3 = v3['committed']

    fkgtA = Filter('key', '>', '/A')

    self.assertTrue(fkgtA(v1))
    self.assertTrue(fkgtA(v2))
    self.assertTrue(fkgtA(v3))

    self.assertTrue(fkgtA.valuePasses('/BCDEG'))
    self.assertTrue(fkgtA.valuePasses('/ZCDEFDSA/fdsafdsa/fdsafdsaf'))
    self.assertFalse(fkgtA.valuePasses('/6353456346543'))
    self.assertFalse(fkgtA.valuePasses('.'))
    self.assertTrue(fkgtA.valuePasses('afsdafdsa'))

    self.assertFilter(fkgtA, vs, vs)

    fkltA = Filter('key', '<', '/A')

    self.assertFalse(fkltA(v1))
    self.assertFalse(fkltA(v2))
    self.assertFalse(fkltA(v3))

    self.assertFalse(fkltA.valuePasses('/BCDEG'))
    self.assertFalse(fkltA.valuePasses('/ZCDEFDSA/fdsafdsa/fdsafdsaf'))
    self.assertTrue(fkltA.valuePasses('/6353456346543'))
    self.assertTrue(fkltA.valuePasses('.'))
    self.assertFalse(fkltA.valuePasses('A'))
    self.assertFalse(fkltA.valuePasses('afsdafdsa'))

    self.assertFilter(fkltA, vs, [])

    fkeqA = Filter('key', '=', '/ABCD')

    self.assertTrue(fkeqA(v1))
    self.assertTrue(fkeqA(v2))
    self.assertTrue(fkeqA(v3))

    self.assertFalse(fkeqA.valuePasses('/BCDEG'))
    self.assertFalse(fkeqA.valuePasses('/ZCDEFDSA/fdsafdsa/fdsafdsaf'))
    self.assertFalse(fkeqA.valuePasses('/6353456346543'))
    self.assertFalse(fkeqA.valuePasses('A'))
    self.assertFalse(fkeqA.valuePasses('.'))
    self.assertFalse(fkeqA.valuePasses('afsdafdsa'))
    self.assertTrue(fkeqA.valuePasses('/ABCD'))

    self.assertFilter(fkeqA, vs, vs)
    self.assertFilter([fkeqA, fkltA], vs, [])
    self.assertFilter([fkeqA, fkeqA], vs, vs)

    fkgtB = Filter('key', '>', '/B')

    self.assertFalse(fkgtB(v1))
    self.assertFalse(fkgtB(v2))
    self.assertFalse(fkgtB(v3))

    self.assertFalse(fkgtB.valuePasses('/A'))
    self.assertTrue(fkgtB.valuePasses('/BCDEG'))
    self.assertTrue(fkgtB.valuePasses('/ZCDEFDSA/fdsafdsa/fdsafdsaf'))
    self.assertFalse(fkgtB.valuePasses('/6353456346543'))
    self.assertFalse(fkgtB.valuePasses('.'))
    self.assertTrue(fkgtB.valuePasses('A'))
    self.assertTrue(fkgtB.valuePasses('afsdafdsa'))

    self.assertFilter(fkgtB, vs, [])
    self.assertFilter([fkgtB, fkgtA], vs, [])
    self.assertFilter([fkgtB, fkgtB], vs, [])

    fkltB = Filter('key', '<', '/B')

    self.assertTrue(fkltB(v1))
    self.assertTrue(fkltB(v2))
    self.assertTrue(fkltB(v3))

    self.assertTrue(fkltB.valuePasses('/A'))
    self.assertFalse(fkltB.valuePasses('/BCDEG'))
    self.assertFalse(fkltB.valuePasses('/ZCDEFDSA/fdsafdsa/fdsafdsaf'))
    self.assertTrue(fkltB.valuePasses('/6353456346543'))
    self.assertTrue(fkltB.valuePasses('.'))
    self.assertFalse(fkltB.valuePasses('A'))
    self.assertFalse(fkltB.valuePasses('afsdafdsa'))

    self.assertFilter(fkltB, vs, vs)

    fkgtAB = Filter('key', '>', '/AB')

    self.assertTrue(fkgtAB(v1))
    self.assertTrue(fkgtAB(v2))
    self.assertTrue(fkgtAB(v3))

    self.assertFalse(fkgtAB.valuePasses('/A'))
    self.assertTrue(fkgtAB.valuePasses('/BCDEG'))
    self.assertTrue(fkgtAB.valuePasses('/ZCDEFDSA/fdsafdsa/fdsafdsaf'))
    self.assertFalse(fkgtAB.valuePasses('/6353456346543'))
    self.assertFalse(fkgtAB.valuePasses('.'))
    self.assertTrue(fkgtAB.valuePasses('A'))
    self.assertTrue(fkgtAB.valuePasses('afsdafdsa'))

    self.assertFilter(fkgtAB, vs, vs)
    self.assertFilter([fkgtAB, fkltB], vs, vs)
    self.assertFilter([fkltB, fkgtAB], vs, vs)

    fgtet1 = Filter('committed', '>=', t1)
    fgtet2 = Filter('committed', '>=', t2)
    fgtet3 = Filter('committed', '>=', t3)

    self.assertTrue(fgtet1(v1))
    self.assertTrue(fgtet1(v2))
    self.assertTrue(fgtet1(v3))

    self.assertFalse(fgtet2(v1))
    self.assertTrue(fgtet2(v2))
    self.assertTrue(fgtet2(v3))

    self.assertFalse(fgtet3(v1))
    self.assertFalse(fgtet3(v2))
    self.assertTrue(fgtet3(v3))

    self.assertFilter(fgtet1, vs, vs)
    self.assertFilter(fgtet2, vs, [v2, v3])
    self.assertFilter(fgtet3, vs, [v3])

    fltet1 = Filter('committed', '<=', t1)
    fltet2 = Filter('committed', '<=', t2)
    fltet3 = Filter('committed', '<=', t3)

    self.assertTrue(fltet1(v1))
    self.assertFalse(fltet1(v2))
    self.assertFalse(fltet1(v3))

    self.assertTrue(fltet2(v1))
    self.assertTrue(fltet2(v2))
    self.assertFalse(fltet2(v3))

    self.assertTrue(fltet3(v1))
    self.assertTrue(fltet3(v2))
    self.assertTrue(fltet3(v3))

    self.assertFilter(fltet1, vs, [v1])
    self.assertFilter(fltet2, vs, [v1, v2])
    self.assertFilter(fltet3, vs, vs)

    self.assertFilter([fgtet2, fltet2], vs, [v2])
    self.assertFilter([fgtet1, fltet3], vs, vs)
    self.assertFilter([fgtet3, fltet1], vs, [])

    feqt1 = Filter('committed', '=', t1)
    feqt2 = Filter('committed', '=', t2)
    feqt3 = Filter('committed', '=', t3)

    self.assertTrue(feqt1(v1))
    self.assertFalse(feqt1(v2))
    self.assertFalse(feqt1(v3))

    self.assertFalse(feqt2(v1))
    self.assertTrue(feqt2(v2))
    self.assertFalse(feqt2(v3))

    self.assertFalse(feqt3(v1))
    self.assertFalse(feqt3(v2))
    self.assertTrue(feqt3(v3))

    self.assertFilter(feqt1, vs, [v1])
    self.assertFilter(feqt2, vs, [v2])
    self.assertFilter(feqt3, vs, [v3])

  def test_none(self):
    # test query against None
    feqnone = Filter('val', '=', None)
    vs = [{'val': None}, {'val': 'something'}]
    self.assertFilter(feqnone, vs, vs[0:1])

    feqzero = Filter('val', '=', 0)
    vs = [{'val': 0}, {'val': None}]
    self.assertFilter(feqzero, vs, vs[0:1])

  def test_object(self):
    t1 = nanotime.now()
    t2 = nanotime.now()

    f1 = Filter('key', '>', '/A')
    f2 = Filter('key', '<', '/A')
    f3 = Filter('committed', '=', t1)
    f4 = Filter('committed', '>=', t2)

    self.assertEqual(f1, eval(repr(f1)))
    self.assertEqual(f2, eval(repr(f2)))
    self.assertEqual(f3, eval(repr(f3)))
    self.assertEqual(f4, eval(repr(f4)))

    self.assertEqual(str(f1), 'key > /A')
    self.assertEqual(str(f2), 'key < /A')
    self.assertEqual(str(f3), 'committed = %s' % t1)
    self.assertEqual(str(f4), 'committed >= %s' % t2)

    self.assertEqual(f1, Filter('key', '>', '/A'))
    self.assertEqual(f2, Filter('key', '<', '/A'))
    self.assertEqual(f3, Filter('committed', '=', t1))
    self.assertEqual(f4, Filter('committed', '>=', t2))

    self.assertNotEqual(f2, Filter('key', '>', '/A'))
    self.assertNotEqual(f1, Filter('key', '<', '/A'))
    self.assertNotEqual(f4, Filter('committed', '=', t1))
    self.assertNotEqual(f3, Filter('committed', '>=', t2))

    self.assertEqual(hash(f1), hash(Filter('key', '>', '/A')))
    self.assertEqual(hash(f2), hash(Filter('key', '<', '/A')))
    self.assertEqual(hash(f3), hash(Filter('committed', '=', t1)))
    self.assertEqual(hash(f4), hash(Filter('committed', '>=', t2)))

    self.assertNotEqual(hash(f2), hash(Filter('key', '>', '/A')))
    self.assertNotEqual(hash(f1), hash(Filter('key', '<', '/A')))
    self.assertNotEqual(hash(f4), hash(Filter('committed', '=', t1)))
    self.assertNotEqual(hash(f3), hash(Filter('committed', '>=', t2)))



class TestOrder(unittest.TestCase):

  def test_basic(self):
    o1 = Order('key')
    o2 = Order('+committed')
    o3 = Order('-created')

    v1, v2, v3 = version_objects()

    # test  isAscending
    self.assertTrue(o1.isAscending())
    self.assertTrue(o2.isAscending())
    self.assertFalse(o3.isAscending())

    # test keyfn
    self.assertEqual(o1.keyfn(v1), (v1['key']))
    self.assertEqual(o1.keyfn(v2), (v2['key']))
    self.assertEqual(o1.keyfn(v3), (v3['key']))
    self.assertEqual(o1.keyfn(v1), (v2['key']))
    self.assertEqual(o1.keyfn(v1), (v3['key']))

    self.assertEqual(o2.keyfn(v1),    (v1['committed']))
    self.assertEqual(o2.keyfn(v2),    (v2['committed']))
    self.assertEqual(o2.keyfn(v3),    (v3['committed']))
    self.assertNotEqual(o2.keyfn(v1), (v2['committed']))
    self.assertNotEqual(o2.keyfn(v1), (v3['committed']))

    self.assertEqual(o3.keyfn(v1),    (v1['created']))
    self.assertEqual(o3.keyfn(v2),    (v2['created']))
    self.assertEqual(o3.keyfn(v3),    (v3['created']))
    self.assertNotEqual(o3.keyfn(v1), (v2['created']))
    self.assertNotEqual(o3.keyfn(v1), (v3['created']))

    # test sorted
    self.assertEqual(Order.sorted([v3, v2, v1], [o1]), [v3, v2, v1])
    self.assertEqual(Order.sorted([v3, v2, v1], [o1, o2]), [v1, v2, v3])
    self.assertEqual(Order.sorted([v1, v3, v2], [o1, o3]), [v3, v2, v1])
    self.assertEqual(Order.sorted([v3, v2, v1], [o1, o2, o3]), [v1, v2, v3])
    self.assertEqual(Order.sorted([v1, v3, v2], [o1, o3, o2]), [v3, v2, v1])

    self.assertEqual(Order.sorted([v3, v2, v1], [o2]), [v1, v2, v3])
    self.assertEqual(Order.sorted([v3, v2, v1], [o2, o1]), [v1, v2, v3])
    self.assertEqual(Order.sorted([v3, v2, v1], [o2, o3]), [v1, v2, v3])
    self.assertEqual(Order.sorted([v3, v2, v1], [o2, o1, o3]), [v1, v2, v3])
    self.assertEqual(Order.sorted([v3, v2, v1], [o2, o3, o1]), [v1, v2, v3])

    self.assertEqual(Order.sorted([v1, v2, v3], [o3]), [v3, v2, v1])
    self.assertEqual(Order.sorted([v1, v2, v3], [o3, o2]), [v3, v2, v1])
    self.assertEqual(Order.sorted([v1, v2, v3], [o3, o1]), [v3, v2, v1])
    self.assertEqual(Order.sorted([v1, v2, v3], [o3, o2, o1]), [v3, v2, v1])
    self.assertEqual(Order.sorted([v1, v2, v3], [o3, o1, o2]), [v3, v2, v1])


  def test_object(self):
    self.assertEqual(Order('key'), eval(repr(Order('key'))))
    self.assertEqual(Order('+committed'), eval(repr(Order('+committed'))))
    self.assertEqual(Order('-created'), eval(repr(Order('-created'))))

    self.assertEqual(str(Order('key')), '+key')
    self.assertEqual(str(Order('+committed')), '+committed')
    self.assertEqual(str(Order('-created')), '-created')

    self.assertEqual(Order('key'), Order('+key'))
    self.assertEqual(Order('-key'), Order('-key'))
    self.assertEqual(Order('+committed'), Order('+committed'))

    self.assertNotEqual(Order('key'), Order('-key'))
    self.assertNotEqual(Order('+key'), Order('-key'))
    self.assertNotEqual(Order('+committed'), Order('+key'))

    self.assertEqual(hash(Order('+key')), hash(Order('+key')))
    self.assertEqual(hash(Order('-key')), hash(Order('-key')))
    self.assertNotEqual(hash(Order('+key')), hash(Order('-key')))
    self.assertEqual(hash(Order('+committed')), hash(Order('+committed')))
    self.assertNotEqual(hash(Order('+committed')), hash(Order('+key')))


class TestQuery(unittest.TestCase):

  def test_basic(self):

    now = nanotime.now().nanoseconds()

    q1 = Query(Key('/'), limit=100)
    q2 = Query(Key('/'), offset=200)
    q3 = Query(Key('/'), object_getattr=getattr)

    q1.offset = 300
    q3.limit = 1

    q1.filter('key', '>', '/ABC')
    q1.filter('created', '>', now)

    q2.order('key')
    q2.order('-created')

    q1d = {'key': '/', 'limit':100, 'offset':300, \
      'filter': [['key', '>', '/ABC'], ['created', '>', now]] }

    q2d = {'key': '/', 'offset':200, 'order': ['+key', '-created'] }

    q3d = {'key': '/', 'limit':1}

    self.assertEqual(q1.dict(), q1d)
    self.assertEqual(q2.dict(), q2d)
    self.assertEqual(q3.dict(), q3d)

    self.assertEqual(q1, Query.from_dict(q1d))
    self.assertEqual(q2, Query.from_dict(q2d))
    self.assertEqual(q3, Query.from_dict(q3d))

    self.assertEqual(q1, eval(repr(q1)))
    self.assertEqual(q2, eval(repr(q2)))
    self.assertEqual(q3, eval(repr(q3)))

    self.assertEqual(q1, q1.copy())
    self.assertEqual(q2, q2.copy())
    self.assertEqual(q3, q3.copy())


  def test_cursor(self):

    k = Key('/')

    self.assertRaises(ValueError, Cursor, None, None)
    self.assertRaises(ValueError, Cursor, Query(Key('/')), None)
    self.assertRaises(ValueError, Cursor, None, [1])
    c = Cursor(Query(k), [1, 2, 3, 4, 5]) # should not raise

    self.assertEqual(c.skipped, 0)
    self.assertEqual(c.returned, 0)
    self.assertEqual(c._iterable, [1, 2, 3, 4, 5])

    c.skipped = 1
    c.returned = 2
    self.assertEqual(c.skipped, 1)
    self.assertEqual(c.returned, 2)

    c._skipped_inc(None)
    c._skipped_inc(None)
    self.assertEqual(c.skipped, 3)

    c._returned_inc(None)
    c._returned_inc(None)
    c._returned_inc(None)
    self.assertEqual(c.returned, 5)

    self.subtest_cursor(Query(k), [5, 4, 3, 2, 1], [5, 4, 3, 2, 1])
    self.subtest_cursor(Query(k, limit=3), [5, 4, 3, 2, 1], [5, 4, 3])
    self.subtest_cursor(Query(k, limit=0), [5, 4, 3, 2, 1], [])
    self.subtest_cursor(Query(k, offset=2), [5, 4, 3, 2, 1], [3, 2, 1])
    self.subtest_cursor(Query(k, offset=5), [5, 4, 3, 2, 1], [])
    self.subtest_cursor(Query(k, limit=2, offset=2), [5, 4, 3, 2, 1], [3, 2])

    v1, v2, v3 = version_objects()
    vs = [v1, v2, v3]

    t1 = v1['committed']
    t2 = v2['committed']
    t3 = v3['committed']

    self.subtest_cursor(Query(k), vs, vs)
    self.subtest_cursor(Query(k, limit=2), vs, [v1, v2])
    self.subtest_cursor(Query(k, offset=1), vs, [v2, v3])
    self.subtest_cursor(Query(k, offset=1, limit=1), vs, [v2])

    self.subtest_cursor(Query(k).filter('committed', '>=', t2), vs, [v2, v3])
    self.subtest_cursor(Query(k).filter('committed', '<=', t1), vs, [v1])

    self.subtest_cursor(Query(k).order('+committed'), vs, [v1, v2, v3])
    self.subtest_cursor(Query(k).order('-created'), vs, [v3, v2, v1])


  def subtest_cursor(self, query, iterable, expected_results):

    self.assertRaises(ValueError, Cursor, None, None)
    self.assertRaises(ValueError, Cursor, query, None)
    self.assertRaises(ValueError, Cursor, None, iterable)
    cursor = Cursor(query, iterable)
    self.assertEqual(cursor.skipped, 0)
    self.assertEqual(cursor.returned, 0)

    cursor._ensure_modification_is_safe()
    cursor.apply_filter()
    cursor.apply_order()
    cursor.apply_offset()
    cursor.apply_limit()

    cursor_results = []
    for i in cursor:
      self.assertRaises(AssertionError, cursor._ensure_modification_is_safe)
      self.assertRaises(AssertionError, cursor.apply_filter)
      self.assertRaises(AssertionError, cursor.apply_order)
      self.assertRaises(AssertionError, cursor.apply_offset)
      self.assertRaises(AssertionError, cursor.apply_limit)
      cursor_results.append(i)

    # ensure iteration happens only once.
    self.assertRaises(RuntimeError, iter, cursor)

    self.assertEqual(cursor_results, expected_results)
    self.assertEqual(cursor.returned, len(expected_results))
    self.assertEqual(cursor.skipped, query.offset)
    if query.limit:
      self.assertTrue(cursor.returned <= query.limit)


if __name__ == '__main__':
  unittest.main()
