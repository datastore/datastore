
import unittest
import logging

from ..basic import DictDatastore
from ..key import Key
from ..query import Query


class TestDatastore(unittest.TestCase):
  pkey = Key('/dfadasfdsafdas/')
  stores = []
  numelems = []

  def check_length(self,len):
    try:
      for sn in self.stores:
        self.assertEqual(len(sn), len)
    except TypeError, e:
      pass

  def subtest_remove_nonexistent(self):
    self.assertTrue(len(self.stores) > 0)
    self.check_length(0)

    # ensure removing non-existent keys is ok.
    for value in range(0, self.numelems):
      key = self.pkey.child(value)
      for sn in self.stores:
        self.assertFalse(sn.contains(key))
        sn.delete(key)
        self.assertFalse(sn.contains(key))

    self.check_length(0)

  def subtest_insert_elems(self):
    # insert numelems elems
    for value in range(0, self.numelems):
      key = self.pkey.child(value)
      for sn in self.stores:
        self.assertFalse(sn.contains(key))
        sn.put(key, value)
        self.assertTrue(sn.contains(key))
        self.assertEqual(sn.get(key), value)

    # reassure they're all there.
    self.check_length(self.numelems)

    for value in range(0, self.numelems):
      key = self.pkey.child(value)
      for sn in self.stores:
        self.assertTrue(sn.contains(key))
        self.assertEqual(sn.get(key), value)

    self.check_length(self.numelems)

  def check_query(self, query, total, slice):
    allitems = list(range(0, total))
    resultset = None

    for sn in self.stores:
      try:
        contents = list(sn.query(Query(self.pkey)))
        expected = contents[slice]
        resultset = sn.query(query)
        result = list(resultset)

        # make sure everything is there.
        self.assertTrue(len(contents) == len(allitems),\
          '%s == %s' %  (str(contents), str(allitems)))
        self.assertTrue(all([val in contents for val in allitems]))

        self.assertTrue(len(result) == len(expected),\
          '%s == %s' %  (str(result), str(expected)))
        self.assertTrue(all([val in result for val in expected]))

        #TODO: should order be preserved?
        #self.assertEqual(result, expected)

      except NotImplementedError:
        print 'WARNING: %s does not implement query.' % sn

    return resultset

  def subtest_queries(self):
    for value in range(0, self.numelems):
      key = self.pkey.child(value)
      for sn in self.stores:
        sn.put(key, value)

    k = self.pkey
    n = int(self.numelems)

    self.check_query(Query(k), n, slice(0, n))
    self.check_query(Query(k, limit=n), n, slice(0, n))
    self.check_query(Query(k, limit=n/2), n, slice(0, n/2))
    self.check_query(Query(k, offset=n/2), n, slice(n/2, n))
    self.check_query(Query(k, offset=n/3, limit=n/3), n, slice(n/3, 2*(n/3)))
    del k
    del n


  def subtest_update(self):
    # change numelems elems
    for value in range(0, self.numelems):
      key = self.pkey.child(value)
      for sn in self.stores:
        self.assertTrue(sn.contains(key))
        sn.put(key, value + 1)
        self.assertTrue(sn.contains(key))
        self.assertNotEqual(value, sn.get(key))
        self.assertEqual(value + 1, sn.get(key))

    self.check_length(self.numelems)

  def subtest_remove(self):
    # remove numelems elems
    for value in range(0, self.numelems):
      key = self.pkey.child(value)
      for sn in self.stores:
        self.assertTrue(sn.contains(key))
        sn.delete(key)
        self.assertFalse(sn.contains(key))

    self.check_length(0)


  def subtest_simple(self, stores, numelems=1000):
    self.stores = stores
    self.numelems = numelems

    self.subtest_remove_nonexistent()
    self.subtest_insert_elems()
    self.subtest_queries()
    self.subtest_update()
    self.subtest_remove()


class TestNullDatastore(unittest.TestCase):

  def test_null(self):
    from ..basic import NullDatastore

    s = NullDatastore()

    for c in range(1, 20):
      c = str(c)
      k = Key(c)
      self.assertFalse(s.contains(k))
      self.assertEqual(s.get(k), None)
      s.put(k, c)
      self.assertFalse(s.contains(k))
      self.assertEqual(s.get(k), None)

    for item in s.query(Query(Key('/'))):
      raise Exception('Should not have found anything.')


class TestDictionaryDatastore(TestDatastore):

  def test_dictionary(self):

    s1 = DictDatastore()
    s2 = DictDatastore()
    s3 = DictDatastore()
    stores = [s1, s2, s3]

    self.subtest_simple(stores)



class TestCacheShimDatastore(TestDatastore):

  def test_simple(self):
    from ..basic import CacheShimDatastore
    from ..basic import NullDatastore

    class NullMinusQueryDatastore(NullDatastore):
      def query(self, query):
        raise NotImplementedError

    # make sure the cache is used
    s1 = CacheShimDatastore(NullMinusQueryDatastore(), cache=DictDatastore())

    # make sure the cache is not relief upon
    s2 = CacheShimDatastore(DictDatastore(), cache=NullDatastore())

    # make sure the cache works in tandem
    s3 = CacheShimDatastore(DictDatastore(), cache=DictDatastore())

    self.subtest_simple([s1, s2, s3])


class TestLoggingDatastore(TestDatastore):

  def test_simple(self):
    from ..basic import LoggingDatastore

    class NullLogger(logging.getLoggerClass()):
      def debug(self, *args, **kwargs): pass
      def info(self, *args, **kwargs): pass
      def warning(self, *args, **kwargs): pass
      def error(self, *args, **kwargs): pass
      def critical(self, *args, **kwargs): pass

    s1 = LoggingDatastore(DictDatastore(), logger=NullLogger('null'))
    s2 = LoggingDatastore(DictDatastore())
    self.subtest_simple([s1, s2])




class TestKeyTransformDatastore(TestDatastore):

  def test_simple(self):
    from ..basic import KeyTransformDatastore

    s1 = KeyTransformDatastore(DictDatastore())
    s2 = KeyTransformDatastore(DictDatastore())
    s3 = KeyTransformDatastore(DictDatastore())
    stores = [s1, s2, s3]

    self.subtest_simple(stores)

  def test_reverse_transform(self):
    from ..basic import KeyTransformDatastore

    def transform(key):
      return key.reverse

    ds = DictDatastore()
    kt = KeyTransformDatastore(ds, keytransform=transform)

    k1 = Key('/a/b/c')
    k2 = Key('/c/b/a')
    self.assertFalse(ds.contains(k1))
    self.assertFalse(ds.contains(k2))
    self.assertFalse(kt.contains(k1))
    self.assertFalse(kt.contains(k2))

    ds.put(k1, 'abc')
    self.assertEqual(ds.get(k1), 'abc')
    self.assertFalse(ds.contains(k2))
    self.assertFalse(kt.contains(k1))
    self.assertEqual(kt.get(k2), 'abc')

    kt.put(k1, 'abc')
    self.assertEqual(ds.get(k1), 'abc')
    self.assertEqual(ds.get(k2), 'abc')
    self.assertEqual(kt.get(k1), 'abc')
    self.assertEqual(kt.get(k2), 'abc')

    ds.delete(k1)
    self.assertFalse(ds.contains(k1))
    self.assertEqual(ds.get(k2), 'abc')
    self.assertEqual(kt.get(k1), 'abc')
    self.assertFalse(kt.contains(k2))

    kt.delete(k1)
    self.assertFalse(ds.contains(k1))
    self.assertFalse(ds.contains(k2))
    self.assertFalse(kt.contains(k1))
    self.assertFalse(kt.contains(k2))

  def test_lowercase_transform(self):
    from ..basic import KeyTransformDatastore

    def transform(key):
      return Key(str(key).lower())

    ds = DictDatastore()
    lds = KeyTransformDatastore(ds, keytransform=transform)

    k1 = Key('hello')
    k2 = Key('HELLO')
    k3 = Key('HeLlo')

    ds.put(k1, 'world')
    ds.put(k2, 'WORLD')

    self.assertEqual(ds.get(k1), 'world')
    self.assertEqual(ds.get(k2), 'WORLD')
    self.assertFalse(ds.contains(k3))

    self.assertEqual(lds.get(k1), 'world')
    self.assertEqual(lds.get(k2), 'world')
    self.assertEqual(lds.get(k3), 'world')

    def test(key, val):
      lds.put(key, val)
      self.assertEqual(lds.get(k1), val)
      self.assertEqual(lds.get(k2), val)
      self.assertEqual(lds.get(k3), val)

    test(k1, 'a')
    test(k2, 'b')
    test(k3, 'c')



class TestLowercaseKeyDatastore(TestDatastore):

  def test_simple(self):
    from ..basic import LowercaseKeyDatastore

    s1 = LowercaseKeyDatastore(DictDatastore())
    s2 = LowercaseKeyDatastore(DictDatastore())
    s3 = LowercaseKeyDatastore(DictDatastore())
    stores = [s1, s2, s3]

    self.subtest_simple(stores)


  def test_lowercase(self):
    from ..basic import LowercaseKeyDatastore

    ds = DictDatastore()
    lds = LowercaseKeyDatastore(ds)

    k1 = Key('hello')
    k2 = Key('HELLO')
    k3 = Key('HeLlo')

    ds.put(k1, 'world')
    ds.put(k2, 'WORLD')

    self.assertEqual(ds.get(k1), 'world')
    self.assertEqual(ds.get(k2), 'WORLD')
    self.assertFalse(ds.contains(k3))

    self.assertEqual(lds.get(k1), 'world')
    self.assertEqual(lds.get(k2), 'world')
    self.assertEqual(lds.get(k3), 'world')

    def test(key, val):
      lds.put(key, val)
      self.assertEqual(lds.get(k1), val)
      self.assertEqual(lds.get(k2), val)
      self.assertEqual(lds.get(k3), val)

    test(k1, 'a')
    test(k2, 'b')
    test(k3, 'c')


class TestNamespaceDatastore(TestDatastore):

  def test_simple(self):
    from ..basic import NamespaceDatastore

    s1 = NamespaceDatastore(Key('a'), DictDatastore())
    s2 = NamespaceDatastore(Key('b'), DictDatastore())
    s3 = NamespaceDatastore(Key('c'), DictDatastore())
    stores = [s1, s2, s3]

    self.subtest_simple(stores)


  def test_namespace(self):
    from ..basic import NamespaceDatastore

    k1 = Key('/c/d')
    k2 = Key('/a/b')
    k3 = Key('/a/b/c/d')

    ds = DictDatastore()
    nd = NamespaceDatastore(k2, ds)

    ds.put(k1, 'cd')
    ds.put(k3, 'abcd')

    self.assertEqual(ds.get(k1), 'cd')
    self.assertFalse(ds.contains(k2))
    self.assertEqual(ds.get(k3), 'abcd')

    self.assertEqual(nd.get(k1), 'abcd')
    self.assertFalse(nd.contains(k2))
    self.assertFalse(nd.contains(k3))

    def test(key, val):
      nd.put(key, val)
      self.assertEqual(nd.get(key), val)
      self.assertFalse(ds.contains(key))
      self.assertFalse(nd.contains(k2.child(key)))
      self.assertEqual(ds.get(k2.child(key)), val)

    for i in range(0, 10):
      test(Key(str(i)), 'val%d' % i)



class TestNestedPathDatastore(TestDatastore):

  def test_simple(self):
    from ..basic import NestedPathDatastore

    s1 = NestedPathDatastore(DictDatastore())
    s2 = NestedPathDatastore(DictDatastore(), depth=2)
    s3 = NestedPathDatastore(DictDatastore(), length=2)
    s4 = NestedPathDatastore(DictDatastore(), length=1, depth=2)
    stores = [s1, s2, s3, s4]

    self.subtest_simple(stores)


  def test_nested_path(self):
    from ..basic import NestedPathDatastore

    nested_path = NestedPathDatastore.nestedPath

    def test(depth, length, expected):
      nested = nested_path('abcdefghijk', depth, length)
      self.assertEqual(nested, expected)

    test(3, 2, 'ab/cd/ef')
    test(4, 2, 'ab/cd/ef/gh')
    test(3, 4, 'abcd/efgh/ijk')
    test(1, 4, 'abcd')
    test(3, 10, 'abcdefghij/k')

  def subtest_nested_path_ds(self, **kwargs):
    from ..basic import NestedPathDatastore

    k1 = kwargs.pop('k1')
    k2 = kwargs.pop('k2')
    k3 = kwargs.pop('k3')
    k4 = kwargs.pop('k4')

    ds = DictDatastore()
    np = NestedPathDatastore(ds, **kwargs)

    self.assertFalse(ds.contains(k1))
    self.assertFalse(ds.contains(k2))
    self.assertFalse(ds.contains(k3))
    self.assertFalse(ds.contains(k4))

    self.assertFalse(np.contains(k1))
    self.assertFalse(np.contains(k2))
    self.assertFalse(np.contains(k3))
    self.assertFalse(np.contains(k4))

    np.put(k1, k1)
    np.put(k2, k2)

    self.assertFalse(ds.contains(k1))
    self.assertFalse(ds.contains(k2))
    self.assertTrue(ds.contains(k3))
    self.assertTrue(ds.contains(k4))

    self.assertTrue(np.contains(k1))
    self.assertTrue(np.contains(k2))
    self.assertFalse(np.contains(k3))
    self.assertFalse(np.contains(k4))

    self.assertEqual(np.get(k1), k1)
    self.assertEqual(np.get(k2), k2)
    self.assertEqual(ds.get(k3), k1)
    self.assertEqual(ds.get(k4), k2)

    np.delete(k1)
    np.delete(k2)

    self.assertFalse(ds.contains(k1))
    self.assertFalse(ds.contains(k2))
    self.assertFalse(ds.contains(k3))
    self.assertFalse(ds.contains(k4))

    self.assertFalse(np.contains(k1))
    self.assertFalse(np.contains(k2))
    self.assertFalse(np.contains(k3))
    self.assertFalse(np.contains(k4))

    ds.put(k3, k1)
    ds.put(k4, k2)

    self.assertFalse(ds.contains(k1))
    self.assertFalse(ds.contains(k2))
    self.assertTrue(ds.contains(k3))
    self.assertTrue(ds.contains(k4))

    self.assertTrue(np.contains(k1))
    self.assertTrue(np.contains(k2))
    self.assertFalse(np.contains(k3))
    self.assertFalse(np.contains(k4))

    self.assertEqual(np.get(k1), k1)
    self.assertEqual(np.get(k2), k2)
    self.assertEqual(ds.get(k3), k1)
    self.assertEqual(ds.get(k4), k2)

    ds.delete(k3)
    ds.delete(k4)

    self.assertFalse(ds.contains(k1))
    self.assertFalse(ds.contains(k2))
    self.assertFalse(ds.contains(k3))
    self.assertFalse(ds.contains(k4))

    self.assertFalse(np.contains(k1))
    self.assertFalse(np.contains(k2))
    self.assertFalse(np.contains(k3))
    self.assertFalse(np.contains(k4))


  def test_3_2(self):

    opts = {}
    opts['k1'] = Key('/abcdefghijk')
    opts['k2'] = Key('/abcdefghijki')
    opts['k3'] = Key('/ab/cd/ef/abcdefghijk')
    opts['k4'] = Key('/ab/cd/ef/abcdefghijki')
    opts['depth'] = 3
    opts['length'] = 2

    self.subtest_nested_path_ds(**opts)

  def test_5_3(self):

    opts = {}
    opts['k1'] = Key('/abcdefghijk')
    opts['k2'] = Key('/abcdefghijki')
    opts['k3'] = Key('/abc/def/ghi/jka/bcd/abcdefghijk')
    opts['k4'] = Key('/abc/def/ghi/jki/abc/abcdefghijki')
    opts['depth'] = 5
    opts['length'] = 3

    self.subtest_nested_path_ds(**opts)

  def test_keyfn(self):

    opts = {}
    opts['k1'] = Key('/abcdefghijk')
    opts['k2'] = Key('/abcdefghijki')
    opts['k3'] = Key('/kj/ih/gf/abcdefghijk')
    opts['k4'] = Key('/ik/ji/hg/abcdefghijki')
    opts['depth'] = 3
    opts['length'] = 2
    opts['keyfn'] = lambda key: key.name[::-1]

    self.subtest_nested_path_ds(**opts)



class TestSymlinkDatastore(TestDatastore):

  def test_simple(self):
    from ..basic import SymlinkDatastore

    s1 = SymlinkDatastore(DictDatastore())
    s2 = SymlinkDatastore(DictDatastore())
    s3 = SymlinkDatastore(DictDatastore())
    s4 = SymlinkDatastore(DictDatastore())
    stores = [s1, s2, s3, s4]

    self.subtest_simple(stores)


  def test_symlink_basic(self):
    from ..basic import SymlinkDatastore

    dds = DictDatastore()
    sds = SymlinkDatastore(dds)

    a = Key('/A')
    b = Key('/B')

    sds.put(a, 1)
    self.assertEqual(sds.get(a), 1)
    self.assertEqual(sds.get(b), None)
    self.assertNotEqual(sds.get(b), sds.get(a))

    sds.link(a, b)
    self.assertEqual(sds.get(a), 1)
    self.assertEqual(sds.get(b), 1)
    self.assertEqual(sds.get(a), sds.get(b))

    sds.put(b, 2)
    self.assertEqual(sds.get(a), 2)
    self.assertEqual(sds.get(b), 2)
    self.assertEqual(sds.get(a), sds.get(b))

    sds.delete(a)
    self.assertEqual(sds.get(a), None)
    self.assertEqual(sds.get(b), None)
    self.assertEqual(sds.get(b), sds.get(a))

    sds.put(a, 3)
    self.assertEqual(sds.get(a), 3)
    self.assertEqual(sds.get(b), 3)
    self.assertEqual(sds.get(b), sds.get(a))

    sds.delete(b)
    self.assertEqual(sds.get(a), 3)
    self.assertEqual(sds.get(b), None)
    self.assertNotEqual(sds.get(b), sds.get(a))

  def test_symlink_internals(self):
    from ..basic import SymlinkDatastore

    dds = DictDatastore()
    sds = SymlinkDatastore(dds)

    a = Key('/A')
    b = Key('/B')
    c = Key('/C')
    d = Key('/D')

    lva = sds._link_value_for_key(a)
    lvb = sds._link_value_for_key(b)
    lvc = sds._link_value_for_key(c)
    lvd = sds._link_value_for_key(d)

    # helper to check queries
    sds_query = lambda: list(sds.query(Query(Key('/'))))
    dds_query = lambda: list(dds.query(Query(Key('/'))))

    # ensure _link_value_for_key and _link_for_value work
    self.assertEqual(lva, str(a.child(sds.sentinel)))
    self.assertEqual(a, sds._link_for_value(lva))

    # adding a value should work like usual
    sds.put(a, 1)
    self.assertEqual(sds.get(a), 1)
    self.assertEqual(sds.get(b), None)
    self.assertNotEqual(sds.get(b), sds.get(a))

    self.assertEqual(dds.get(a), 1)
    self.assertEqual(dds.get(b), None)

    self.assertEqual(sds_query(), [1])
    self.assertEqual(dds_query(), [1])

    # _follow_link(sds._link_value_for_key(a)) should == get(a)
    self.assertEqual(sds._follow_link(lva), 1)
    self.assertEqual(list(sds._follow_link_gen([lva])), [1])

    # linking keys should work
    sds.link(a, b)
    self.assertEqual(sds.get(a), 1)
    self.assertEqual(sds.get(b), 1)
    self.assertEqual(sds.get(a), sds.get(b))

    self.assertEqual(dds.get(a), 1)
    self.assertEqual(dds.get(b), lva)

    self.assertEqual(sds_query(), [1, 1])
    self.assertEqual(dds_query(), [lva, 1])

    # changing link should affect source
    sds.put(b, 2)
    self.assertEqual(sds.get(a), 2)
    self.assertEqual(sds.get(b), 2)
    self.assertEqual(sds.get(a), sds.get(b))

    self.assertEqual(dds.get(a), 2)
    self.assertEqual(dds.get(b), lva)

    self.assertEqual(sds_query(), [2, 2])
    self.assertEqual(dds_query(), [lva, 2])

    # deleting source should affect link
    sds.delete(a)
    self.assertEqual(sds.get(a), None)
    self.assertEqual(sds.get(b), None)
    self.assertEqual(sds.get(b), sds.get(a))

    self.assertEqual(dds.get(a), None)
    self.assertEqual(dds.get(b), lva)

    self.assertEqual(sds_query(), [None])
    self.assertEqual(dds_query(), [lva])

    # putting back source should yield working link
    sds.put(a, 3)
    self.assertEqual(sds.get(a), 3)
    self.assertEqual(sds.get(b), 3)
    self.assertEqual(sds.get(b), sds.get(a))

    self.assertEqual(dds.get(a), 3)
    self.assertEqual(dds.get(b), lva)

    self.assertEqual(sds_query(), [3, 3])
    self.assertEqual(dds_query(), [lva, 3])


    # deleting link should not affect source
    sds.delete(b)
    self.assertEqual(sds.get(a), 3)
    self.assertEqual(sds.get(b), None)
    self.assertNotEqual(sds.get(b), sds.get(a))

    self.assertEqual(dds.get(a), 3)
    self.assertEqual(dds.get(b), None)

    self.assertEqual(sds_query(), [3])
    self.assertEqual(dds_query(), [3])

    # linking should bring back to normal
    sds.link(a, b)
    self.assertEqual(sds.get(a), 3)
    self.assertEqual(sds.get(b), 3)
    self.assertEqual(sds.get(b), sds.get(a))

    self.assertEqual(dds.get(a), 3)
    self.assertEqual(dds.get(b), lva)

    self.assertEqual(sds_query(), [3, 3])
    self.assertEqual(dds_query(), [lva, 3])

    # Adding another link should not affect things.
    sds.link(a, c)
    self.assertEqual(sds.get(a), 3)
    self.assertEqual(sds.get(b), 3)
    self.assertEqual(sds.get(c), 3)
    self.assertEqual(sds.get(a), sds.get(b))
    self.assertEqual(sds.get(a), sds.get(c))

    self.assertEqual(dds.get(a), 3)
    self.assertEqual(dds.get(b), lva)
    self.assertEqual(dds.get(c), lva)

    self.assertEqual(sds_query(), [3, 3, 3])
    self.assertEqual(dds_query(), [lva, lva, 3])

    # linking should be transitive
    sds.link(b, c)
    sds.link(c, d)
    self.assertEqual(sds.get(a), 3)
    self.assertEqual(sds.get(b), 3)
    self.assertEqual(sds.get(c), 3)
    self.assertEqual(sds.get(d), 3)
    self.assertEqual(sds.get(a), sds.get(b))
    self.assertEqual(sds.get(a), sds.get(c))
    self.assertEqual(sds.get(a), sds.get(d))

    self.assertEqual(dds.get(a), 3)
    self.assertEqual(dds.get(b), lva)
    self.assertEqual(dds.get(c), lvb)
    self.assertEqual(dds.get(d), lvc)

    self.assertEqual(sds_query(), [3, 3, 3, 3])
    self.assertEqual(set(dds_query()), set([3, lva, lvb, lvc]))

    self.assertRaises(AssertionError, sds.link, d, a)


  def test_symlink_recursive(self):
    from ..basic import SymlinkDatastore

    dds = DictDatastore()
    sds1 = SymlinkDatastore(dds)
    sds2 = SymlinkDatastore(sds1)

    a = Key('/A')
    b = Key('/B')

    sds2.put(a, 1)
    self.assertEqual(sds2.get(a), 1)
    self.assertEqual(sds2.get(b), None)
    self.assertNotEqual(sds2.get(b), sds2.get(a))

    sds2.link(a, b)
    self.assertEqual(sds2.get(a), 1)
    self.assertEqual(sds2.get(b), 1)
    self.assertEqual(sds2.get(a), sds2.get(b))
    self.assertEqual(sds1.get(a), sds1.get(b))

    sds2.link(a, b)
    self.assertEqual(sds2.get(a), 1)
    self.assertEqual(sds2.get(b), 1)
    self.assertEqual(sds2.get(a), sds2.get(b))
    self.assertEqual(sds1.get(a), sds1.get(b))

    sds2.link(a, b)
    self.assertEqual(sds2.get(a), 1)
    self.assertEqual(sds2.get(b), 1)
    self.assertEqual(sds2.get(a), sds2.get(b))
    self.assertEqual(sds1.get(a), sds1.get(b))

    sds2.put(b, 2)
    self.assertEqual(sds2.get(a), 2)
    self.assertEqual(sds2.get(b), 2)
    self.assertEqual(sds2.get(a), sds2.get(b))
    self.assertEqual(sds1.get(a), sds1.get(b))

    sds2.delete(a)
    self.assertEqual(sds2.get(a), None)
    self.assertEqual(sds2.get(b), None)
    self.assertEqual(sds2.get(b), sds2.get(a))

    sds2.put(a, 3)
    self.assertEqual(sds2.get(a), 3)
    self.assertEqual(sds2.get(b), 3)
    self.assertEqual(sds2.get(b), sds2.get(a))

    sds2.delete(b)
    self.assertEqual(sds2.get(a), 3)
    self.assertEqual(sds2.get(b), None)
    self.assertNotEqual(sds2.get(b), sds2.get(a))



class TestDirectoryDatastore(TestDatastore):

  def test_simple(self):
    from ..basic import DirectoryDatastore

    s1 = DirectoryDatastore(DictDatastore())
    s2 = DirectoryDatastore(DictDatastore())
    self.subtest_simple([s1, s2])


  def test_directory_init(self):
    from ..basic import DirectoryDatastore

    ds = DirectoryDatastore(DictDatastore())

    # initialize directory at /foo
    dir_key = Key('/foo')
    ds.directory(dir_key)
    self.assertEqual(ds.get(dir_key), [])

    # can add to dir
    bar_key = Key('/foo/bar')
    ds.directoryAdd(dir_key, bar_key)
    self.assertEqual(ds.get(dir_key), [str(bar_key)])

    # re-init does not wipe out directory at /foo
    dir_key = Key('/foo')
    ds.directory(dir_key)
    self.assertEqual(ds.get(dir_key), [str(bar_key)])


  def test_directory_simple(self):
    from ..basic import DirectoryDatastore

    ds = DirectoryDatastore(DictDatastore())

    # initialize directory at /foo
    dir_key = Key('/foo')
    ds.directory(dir_key)

    # adding directory entries
    bar_key = Key('/foo/bar')
    baz_key = Key('/foo/baz')
    ds.directoryAdd(dir_key, bar_key)
    ds.directoryAdd(dir_key, baz_key)
    keys = list(ds.directoryRead(dir_key))
    self.assertEqual(keys, [bar_key, baz_key])

    # removing directory entries
    ds.directoryRemove(dir_key, bar_key)
    keys = list(ds.directoryRead(dir_key))
    self.assertEqual(keys, [baz_key])

    ds.directoryRemove(dir_key, baz_key)
    keys = list(ds.directoryRead(dir_key))
    self.assertEqual(keys, [])

    # generator
    with self.assertRaises(StopIteration):
      gen = ds.directoryRead(dir_key)
      gen.next()


  def test_directory_double_add(self):
    from ..basic import DirectoryDatastore

    ds = DirectoryDatastore(DictDatastore())

    # initialize directory at /foo
    dir_key = Key('/foo')
    ds.directory(dir_key)

    # adding directory entries
    bar_key = Key('/foo/bar')
    baz_key = Key('/foo/baz')
    ds.directoryAdd(dir_key, bar_key)
    ds.directoryAdd(dir_key, baz_key)
    ds.directoryAdd(dir_key, bar_key)
    ds.directoryAdd(dir_key, baz_key)
    ds.directoryAdd(dir_key, baz_key)
    ds.directoryAdd(dir_key, bar_key)

    keys = list(ds.directoryRead(dir_key))
    self.assertEqual(keys, [bar_key, baz_key])


  def test_directory_remove(self):
    from ..basic import DirectoryDatastore

    ds = DirectoryDatastore(DictDatastore())

    # initialize directory at /foo
    dir_key = Key('/foo')
    ds.directory(dir_key)

    # adding directory entries
    bar_key = Key('/foo/bar')
    baz_key = Key('/foo/baz')
    ds.directoryAdd(dir_key, bar_key)
    ds.directoryAdd(dir_key, baz_key)
    keys = list(ds.directoryRead(dir_key))
    self.assertEqual(keys, [bar_key, baz_key])

    # removing directory entries
    ds.directoryRemove(dir_key, bar_key)
    ds.directoryRemove(dir_key, bar_key)
    ds.directoryRemove(dir_key, bar_key)
    keys = list(ds.directoryRead(dir_key))
    self.assertEqual(keys, [baz_key])




class TestDirectoryTreeDatastore(TestDatastore):

  def test_simple(self):
    from ..basic import DirectoryTreeDatastore

    s1 = DirectoryTreeDatastore(DictDatastore())
    s2 = DirectoryTreeDatastore(DictDatastore())
    self.subtest_simple([s1, s2])



class TestDatastoreCollection(TestDatastore):

  def test_tiered(self):
    from ..basic import TieredDatastore

    s1 = DictDatastore()
    s2 = DictDatastore()
    s3 = DictDatastore()
    ts = TieredDatastore([s1, s2, s3])

    k1 = Key('1')
    k2 = Key('2')
    k3 = Key('3')

    s1.put(k1, '1')
    s2.put(k2, '2')
    s3.put(k3, '3')

    self.assertTrue(s1.contains(k1))
    self.assertFalse(s2.contains(k1))
    self.assertFalse(s3.contains(k1))
    self.assertTrue(ts.contains(k1))

    self.assertEqual(ts.get(k1), '1')
    self.assertEqual(s1.get(k1), '1')
    self.assertFalse(s2.contains(k1))
    self.assertFalse(s3.contains(k1))

    self.assertFalse(s1.contains(k2))
    self.assertTrue(s2.contains(k2))
    self.assertFalse(s3.contains(k2))
    self.assertTrue(ts.contains(k2))

    self.assertEqual(s2.get(k2), '2')
    self.assertFalse(s1.contains(k2))
    self.assertFalse(s3.contains(k2))

    self.assertEqual(ts.get(k2), '2')
    self.assertEqual(s1.get(k2), '2')
    self.assertEqual(s2.get(k2), '2')
    self.assertFalse(s3.contains(k2))

    self.assertFalse(s1.contains(k3))
    self.assertFalse(s2.contains(k3))
    self.assertTrue(s3.contains(k3))
    self.assertTrue(ts.contains(k3))

    self.assertEqual(s3.get(k3), '3')
    self.assertFalse(s1.contains(k3))
    self.assertFalse(s2.contains(k3))

    self.assertEqual(ts.get(k3), '3')
    self.assertEqual(s1.get(k3), '3')
    self.assertEqual(s2.get(k3), '3')
    self.assertEqual(s3.get(k3), '3')

    ts.delete(k1)
    ts.delete(k2)
    ts.delete(k3)

    self.assertFalse(ts.contains(k1))
    self.assertFalse(ts.contains(k2))
    self.assertFalse(ts.contains(k3))

    self.subtest_simple([ts])

  def test_sharded(self, numelems=1000):
    from ..basic import ShardedDatastore

    s1 = DictDatastore()
    s2 = DictDatastore()
    s3 = DictDatastore()
    s4 = DictDatastore()
    s5 = DictDatastore()
    stores = [s1, s2, s3, s4, s5]
    hash = lambda key: int(key.name) * len(stores) / numelems
    sharded = ShardedDatastore(stores, shardingfn=hash)
    sumlens = lambda stores: sum(map(lambda s: len(s), stores))

    def checkFor(key, value, sharded, shard=None):
      correct_shard = sharded._stores[hash(key) % len(sharded._stores)]

      for s in sharded._stores:
        if shard and s == shard:
          self.assertTrue(s.contains(key))
          self.assertEqual(s.get(key), value)
        else:
          self.assertFalse(s.contains(key))

      if correct_shard == shard:
        self.assertTrue(sharded.contains(key))
        self.assertEqual(sharded.get(key), value)
      else:
        self.assertFalse(sharded.contains(key))

    self.assertEqual(sumlens(stores), 0)
    # test all correct.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      shard = stores[hash(key) % len(stores)]
      checkFor(key, value, sharded)
      shard.put(key, value)
      checkFor(key, value, sharded, shard)
    self.assertEqual(sumlens(stores), numelems)

    # ensure its in the same spots.
    for i in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      shard = stores[hash(key) % len(stores)]
      checkFor(key, value, sharded, shard)
      shard.put(key, value)
      checkFor(key, value, sharded, shard)
    self.assertEqual(sumlens(stores), numelems)

    # ensure its in the same spots.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      shard = stores[hash(key) % len(stores)]
      checkFor(key, value, sharded, shard)
      sharded.put(key, value)
      checkFor(key, value, sharded, shard)
    self.assertEqual(sumlens(stores), numelems)

    # ensure its in the same spots.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      shard = stores[hash(key) % len(stores)]
      checkFor(key, value, sharded, shard)
      if value % 2 == 0:
        shard.delete(key)
      else:
        sharded.delete(key)
      checkFor(key, value, sharded)
    self.assertEqual(sumlens(stores), 0)

    # try out adding it to the wrong shards.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      incorrect_shard = stores[(hash(key) + 1) % len(stores)]
      checkFor(key, value, sharded)
      incorrect_shard.put(key, value)
      checkFor(key, value, sharded, incorrect_shard)
    self.assertEqual(sumlens(stores), numelems)

    # ensure its in the same spots.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      incorrect_shard = stores[(hash(key) + 1) % len(stores)]
      checkFor(key, value, sharded, incorrect_shard)
      incorrect_shard.put(key, value)
      checkFor(key, value, sharded, incorrect_shard)
    self.assertEqual(sumlens(stores), numelems)

    # this wont do anything
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      incorrect_shard = stores[(hash(key) + 1) % len(stores)]
      checkFor(key, value, sharded, incorrect_shard)
      sharded.delete(key)
      checkFor(key, value, sharded, incorrect_shard)
    self.assertEqual(sumlens(stores), numelems)

    # this will place it correctly.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      incorrect_shard = stores[(hash(key) + 1) % len(stores)]
      correct_shard = stores[(hash(key)) % len(stores)]
      checkFor(key, value, sharded, incorrect_shard)
      sharded.put(key, value)
      incorrect_shard.delete(key)
      checkFor(key, value, sharded, correct_shard)
    self.assertEqual(sumlens(stores), numelems)

    # this will place it correctly.
    for value in range(0, numelems):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      correct_shard = stores[(hash(key)) % len(stores)]
      checkFor(key, value, sharded, correct_shard)
      sharded.delete(key)
      checkFor(key, value, sharded)
    self.assertEqual(sumlens(stores), 0)

    self.subtest_simple([sharded])


if __name__ == '__main__':
  unittest.main()
