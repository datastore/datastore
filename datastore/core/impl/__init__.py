
__doc__ = '''
This package contains all the specific datastore implementations.

Currently, the main distribution includes:

=========================================   ================================
   module                                   storage system
=========================================   ================================
:py:mod:`datastore.impl.aws`                   Amazon Web Services (S3)
:py:mod:`datastore.impl.filesystem`            flat-file filesystem
:py:mod:`datastore.impl.git`                   git (dulwich)
:py:mod:`datastore.impl.lrucache`              pylru
:py:mod:`datastore.impl.memcached`             memcached
:py:mod:`datastore.impl.mongo`                 MongoDB (pymongo)
:py:mod:`datastore.impl.redis`                 redis
=========================================   ================================

'''
