
import redis
import unittest

from datastore.impl.redis import RedisDatastore
from test_basic import TestDatastore


class TestRedisDatastore(TestDatastore):

  redis_args = {'host': 'localhost', 'port' : 6379}

  def setUp(self):
    try:
      self.client = redis.Redis(**self.redis_args)
      self.client.flushall()
    except redis.ConnectionError:
      err = 'Error connecting to redis. '\
            'Check this server is running: %(host)s:%(port)d'
      raise Exception(err % self.redis_args)

  def tearDown(self):
    self.client.flushall()
    del self.client

  def test_redis(self):
    rds = RedisDatastore(self.client)
    self.subtest_simple([rds], numelems=500)


if __name__ == '__main__':
  unittest.main()
