
import os
import shutil
import unittest

from datastore import serialize
from datastore.core.test.test_basic import TestDatastore

from . import FileSystemDatastore


class TestFileSystemDatastore(TestDatastore):

    tmp = os.path.normpath('/tmp/datastore.test.fs')

    def setUp(self):
        if os.path.exists(self.tmp):
            shutil.rmtree(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_datastore(self):
        dirs = list(map(str, list(range(0, 4))))
        dirs = [os.path.join(self.tmp, d) for d in dirs]
        fses = list(map(FileSystemDatastore, dirs))
        dses = list(map(serialize.shim, fses))
        self.subtest_simple(dses, numelems=500)


if __name__ == '__main__':
    unittest.main()
