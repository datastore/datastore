
import os
import shutil
import unittest

from datastore.impl import git
from datastore import serialize
from test_filesystem import TestDatastore


class TestGitDatastore(TestDatastore):

  tmp = os.path.normpath('/tmp/datastore.test.git')

  def setUp(self):
    if os.path.exists(self.tmp):
      shutil.rmtree(self.tmp)

  def tearDown(self):
    pass
    # shutil.rmtree(self.tmp)

  def test_datastore(self):
    p = os.path.join(self.tmp, '1')
    g = git.GitDatastore(p)
    s = serialize.shim(g, serialize.prettyjson)
    self.subtest_simple([s], numelems=100)


if __name__ == '__main__':
  unittest.main()
