
import pymongo
import unittest

from datastore.impl.mongo import MongoDatastore
from test_basic import TestDatastore


class TestMongoDatastore(TestDatastore):

  def setUp(self):
    self.conn = pymongo.Connection()
    self.conn.drop_database('datastore_testdb')

  def tearDown(self):
    self.conn.drop_database('datastore_testdb')
    del self.conn

  def test_mongo(self):
    ms = MongoDatastore(self.conn.datastore_testdb)
    self.subtest_simple([ms], numelems=500)


if __name__ == '__main__':
  unittest.main()
