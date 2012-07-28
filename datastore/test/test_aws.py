
import redis
import unittest
import logging

from datastore import SerializerShimDatastore
from datastore.impl.aws import S3BucketDatastore
from boto.s3.connection import S3Connection
from test_basic import TestDatastore

class TestS3BucketDatastore(TestDatastore):

  s3bucketname = '<aws bucket name>'
  s3connargs = ['<aws access key>', '<aws secret key>']

  def _deletekeys(self):
    keys = list(self.s3bucket.list())
    for key in keys:
      print 'deleting', key
      self.s3bucket.delete_key(key)

  def setUp(self):
    logging.getLogger('boto').setLevel(logging.CRITICAL)

    err = 'Use a real S3 %s. Edit datastore/test/test_aws.py.'
    assert self.s3bucketname != '<aws bucket name>', err % 'bucket'
    assert self.s3connargs[0] != '<aws access key>', err % 'access key.'
    assert self.s3connargs[1] != '<aws secret key>', err % 'secret key.'

    self.s3conn = S3Connection(*self.s3connargs)
    self.s3bucket = self.s3conn.get_bucket(self.s3bucketname)
    self._deletekeys() # make sure we're clean :)

  def tearDown(self):
    self._deletekeys() # clean up after ourselves :]
    del self.s3bucket
    del self.s3conn

  def test_s3(self):
    ds = S3BucketDatastore(self.s3bucket)
    ser = SerializerShimDatastore(ds)
    self.subtest_simple([ser], numelems=20)


if __name__ == '__main__':
  unittest.main()
