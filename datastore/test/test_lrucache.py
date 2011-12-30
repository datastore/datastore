
import unittest

from datastore import Key
from datastore.impl import lrucache

from test_basic import TestDatastore

class TestLRUCacheDatastore(TestDatastore):

  def test_lru(self):

    lru1 = lrucache.LRUCache(1000)
    lru2 = lrucache.LRUCache(2000)
    lru3 = lrucache.LRUCache(3000)
    lrus = [lru1, lru2, lru3]

    for value in range(0, 3000):
      key = Key('/LRU/%d' % value)
      for lru in lrus:
        self.assertFalse(lru.contains(key))

        lru.put(key, value)

        self.assertTrue(lru.contains(key))
        self.assertEqual(lru.get(key), value)

    self.assertEqual(len(lru1), 1000)
    self.assertEqual(len(lru2), 2000)
    self.assertEqual(len(lru3), 3000)

    for value in range(0, 3000):
      key = Key('/LRU/%d' % value)
      self.assertEqual(lru1.contains(key), value >= 2000)
      self.assertEqual(lru2.contains(key), value >= 1000)
      self.assertTrue(lru3.contains(key))

    lru1.clear()
    lru2.clear()
    lru3.clear()

    self.assertEqual(len(lru1), 0)
    self.assertEqual(len(lru2), 0)
    self.assertEqual(len(lru3), 0)

    self.subtest_simple(lrus)


if __name__ == '__main__':
  unittest.main()
