
__version__ = '0.1'
__author__ = 'Juan Batiz-Benet <jbenet@cs.stanford.edu>'

__doc__ = '''
datastore is a generic layer of abstraction for data store and database access.
It is a **simple** APi with the aim to enable application development in a
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
from basic import DictDatastore

from basic import DatastoreCollection
from basic import ShardedDatastore
from basic import TieredDatastore

import query
from query import Query
from query import Cursor
