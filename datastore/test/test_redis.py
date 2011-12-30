
import redis
import unittest

from datastore.impl.redis import RedisDatastore
from test_basic import TestDatastore


class TestRedisDatastore(TestDatastore):

  redis_args = {'host': 'localhost', 'port' : 6379}

  def setUp(self):
    self.client = redis.Redis(**self.redis_args)
    self.client.flushall()

  def tearDown(self):
    self.client.flushall()
    del self.client

  def test_redis(self):
    rds = RedisDatastore(self.client)
    self.subtest_simple([rds], numelems=500)


if __name__ == '__main__':
  unittest.main()
