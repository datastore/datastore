
__version__ = '0.3.6'
__author__ = 'Juan Batiz-Benet'
__email__ = 'juan@benet.ai'

__doc__ = '''
datastore is a generic layer of abstraction for data store and database access.
It is a **simple** API with the aim to enable application development in a
datastore-agnostic way, allowing datastores to be swapped seamlessly without
changing application code. Thus, one can leverage different datastores with
different strengths without committing the application to one datastore
throughout its lifetime.
'''

import key
from key import Key
from key import Namespace

import basic
from basic import Datastore
from basic import NullDatastore
from basic import DictDatastore
from basic import InterfaceMappingDatastore

from basic import ShimDatastore
from basic import CacheShimDatastore
from basic import LoggingDatastore
from basic import KeyTransformDatastore
from basic import LowercaseKeyDatastore
from basic import NamespaceDatastore
from basic import NestedPathDatastore
from basic import SymlinkDatastore
from basic import DirectoryTreeDatastore
from basic import DirectoryDatastore

from basic import DatastoreCollection
from basic import ShardedDatastore
from basic import TieredDatastore

import query
from query import Query
from query import Cursor

import serialize
from serialize import SerializerShimDatastore


# patch datastore with core variables
import datastore
datastore.__version__ = __version__
datastore.__author__ = __author__
datastore.__email__ = __email__

for k, v in locals().items():
  if k in ['datastore']:
    continue

  if k.startswith('__'):
    continue

  if hasattr(datastore, k):
    continue

  setattr(datastore, k, v)
