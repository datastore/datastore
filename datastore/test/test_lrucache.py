
import unittest

from datastore import Key
from datastore import DictDatastore
from datastore import NullDatastore
from datastore import CacheShimDatastore
from datastore.impl import lrucache

from test_basic import TestDatastore

class TestLRUCacheDatastore(TestDatastore):


  def subtest_cache(self, lrus):

    lru1 = lrus[0]
    lru2 = lrus[1]
    lru3 = lrus[2]

    for value in range(0, 3000):
      key = Key('/LRU/%d' % value)
      for lru in lrus:
        self.assertFalse(lru.contains(key))

        lru.put(key, value)

        self.assertTrue(lru.contains(key))
        self.assertEqual(lru.get(key), value)

    if hasattr(lru1, '__len__'):
      self.assertEqual(len(lru1), 1000)
      self.assertEqual(len(lru2), 2000)
      self.assertEqual(len(lru3), 3000)

    for value in range(0, 3000):
      key = Key('/LRU/%d' % value)
      self.assertEqual(lru1.contains(key), value >= 2000)
      self.assertEqual(lru2.contains(key), value >= 1000)
      self.assertTrue(lru3.contains(key))

    if hasattr(lru1, 'clear'):
      lru1.clear()
      lru2.clear()
      lru3.clear()

      self.assertEqual(len(lru1), 0)
      self.assertEqual(len(lru2), 0)
      self.assertEqual(len(lru3), 0)

      self.subtest_simple(lrus)


  def test_lru(self):

    lru1 = lrucache.LRUCache(1000)
    lru2 = lrucache.LRUCache(2000)
    lru3 = lrucache.LRUCache(3000)
    lrus = [lru1, lru2, lru3]
    self.subtest_cache(lrus)


  def test_shim(self):

    lru1 = lrucache.LRUCache(1000)
    lru2 = lrucache.LRUCache(2000)
    lru3 = lrucache.LRUCache(3000)

    ds1 = CacheShimDatastore(NullDatastore(), cache=lru1)
    ds2 = CacheShimDatastore(NullDatastore(), cache=lru2)
    ds3 = CacheShimDatastore(NullDatastore(), cache=lru3)

    lrus = [ds1, ds2, ds3]
    self.subtest_cache(lrus)

    lru1.clear()
    lru2.clear()
    lru3.clear()

    class NullMinusQueryDatastore(NullDatastore):
      def query(self, query):
        raise NotImplementedError

    ds1 = CacheShimDatastore(NullMinusQueryDatastore(), cache=lru1)
    ds2 = CacheShimDatastore(NullMinusQueryDatastore(), cache=lru2)
    ds3 = CacheShimDatastore(NullMinusQueryDatastore(), cache=lru3)

    lrus = [ds1, ds2, ds3]
    self.subtest_simple(lrus)

    ds1 = CacheShimDatastore(DictDatastore(), cache=lru1)
    ds2 = CacheShimDatastore(DictDatastore(), cache=lru2)
    ds3 = CacheShimDatastore(DictDatastore(), cache=lru3)

    lrus = [ds1, ds2, ds3]
    self.subtest_simple(lrus)




if __name__ == '__main__':
  unittest.main()
