
import pylibmc
import unittest

from datastore.impl.memcached import MemcachedDatastore
from test_basic import TestDatastore


class TestMemcachedDatastore(TestDatastore):

  behaviors = {'tcp_nodelay': True, 'ketama': True}
  servers = ['127.0.0.1']

  def setUp(self):
    c = pylibmc.Client(self.servers, binary=True, behaviors=self.behaviors)
    self.client = c
    self.client.flush_all()

  def tearDown(self):
    self.client.flush_all()
    self.client.disconnect_all()
    del self.client

  def test_memcached(self):
    ms = MemcachedDatastore(self.client)
    self.subtest_simple([ms], numelems=500)


if __name__ == '__main__':
  unittest.main()
