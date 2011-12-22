.. datastore documentation master file, created by
   sphinx-quickstart on Thu Dec 22 01:26:05 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

========================================================
datastore - simple, unified API for multiple data stores
========================================================

Overview
--------

datastore is a generic layer of abstraction for data store and database access.
It is a **simple** API with the aim to enable application development in a
datastore-agnostic way, allowing datastores to be swapped seamlessly without
changing application code. Thus, one can leverage different datastores with
different strengths without committing the application to one datastore
throughout its lifetime.

In addition, grouped datastores significantly simplify complex data access
patterns, such as caching and sharding.


Documentation
-------------

The :ref:`api` contains documentation of the core library.

.. toctree::
   :maxdepth: 2

   api/index

:py:mod:`impl Package <datastore.impl>` contains reference to multiple datastore
implementations included with this datastore.


.. toctree::
   :maxdepth: 3

   package/datastore.impl


Install
-------

Until datastore is well-tested and in pypi, install from the git repository::

    git clone https://github.com/jbenet/datastore/
    cd datastore
    sudo python setup.py install

License
-------

datastore is under the MIT Licence


Contact
-------

datastore is written by [Juan Batiz-Benet](https://github.com/jbenet). It
was originally part of `py-dronestore <https://github.com/jbenet/py-dronestore>`_
On December 2011, it was re-written as a standalone project.

Project Homepage: https://github.com/jbenet/datastore

Feel free to contact me. But please file issues in github first. Cheers!



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
