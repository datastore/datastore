
import os
import shutil
import unittest

from datastore.impl import filesystem
from datastore import serialize
from test_basic import TestDatastore


class TestFileSystemDatastore(TestDatastore):

  tmp = os.path.normpath('/tmp/datastore.test.fs')

  def setUp(self):
    if os.path.exists(self.tmp):
      shutil.rmtree(self.tmp)

  def tearDown(self):
    shutil.rmtree(self.tmp)

  def test_datastore(self):
    dirs = map(str, range(0, 4))
    dirs = map(lambda d: os.path.join(self.tmp, d), dirs)
    fses = map(filesystem.FileSystemDatastore, dirs)
    dses = map(serialize.shim, fses)
    self.subtest_simple(dses, numelems=500)


if __name__ == '__main__':
  unittest.main()
