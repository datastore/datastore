.. _api-query:

Queries
=======

In addition to the key-value store ``get`` and ``set`` semantics, datastore
provides an interface to retrieve multiple records at a time through the use
of queries. The datastore Query model gleans a common set of operations
performed when querying. To avoid pasting here years of database research,
let's summarize the operations datastore supports.

.. _api-query-operations:

Query Operations:

  * namespace - scope the query, usually by object type
  * filters - select a subset of values by applying constraints
  * orders - sort the results by applying sort conditions
  * limit - impose a numeric limit on the number of results
  * offset - skip a number of results (for efficient pagination)

datastore combines these operations into a simple Query class that allows
applications to define their constraints in a simple, generic, and pythonic
way without introducing datastore specific calls, languages, etc.

Of course, different datastores provide relational query support across a wide
spectrum, from full support in traditional databases to none at all in
key-value stores. Datastore aims to provide a common, simple interface for
the sake of application evolution over time and keeping large code bases
free of tool-specific code. It would be ridiculous to claim to support
high-performance queries on architectures that obviously do not.
Instead, datastore provides the interface, ideally translating queries to their
native form (e.g. into SQL for MySQL or a MongoDB query).

However, on the
wrong datastore, queries can potentially incur the high cost of performing the
aforemantioned :ref:`query operations <api-query-operations>` on the data set
directly in python.
It is the client's responsibility to select the right tool for the job: pick a
data storage solution that fits the application's needs now, and wrap it with a
datastore implementation. Some applications, particularly in early development
stages, can afford to incurr the cost of queries on non-relational databases
(e.g. using a :py:class:`FileSystemDatastore
<datastore.filesystem.FileSystemDatastore>` and not worry about a database
at all). When it comes time to switch the tool for performance, updating the
application code can be as simple as swapping the datastore in one place, not
all over the application code base. This gain in engineering time, both at
initial development and during later iterations, can significantly offest the
cost of the layer of abstraction.

**tl;dr:**
queries are supported across datastores. They are very cheap on top
of relational databases, and very expensive otherwise. Pick the right tool for
the job!


Query classes
_____________

Query
-----

.. autoclass:: datastore.Query
   :members:

Filter
------

.. autoclass:: datastore.query.Filter
   :members:

Order
-----

.. autoclass:: datastore.query.Order
   :members:


Cursor
------

.. autoclass:: datastore.Cursor
   :members:


Generators
----------

Note on generators: naive datastore queries use generators to delay
performing work (such as filtering). Thus, no up-front cost is paid, but rather
the cost comes at iteration. This is particularly useful in that even when
working on large datasets, the naive query implementation can work as
generators do not require having loading the entire dataset in memory upfront.
When I say they can work, I do not imply quickly, just that they can work at
all.

The **crucial exception**, of course, is orders. If
any order is placed on a query, the naive query implementation loses the benefit
of delaying the work. That is because one cannot properly sort an entire
dataset using a generator (sure, a generator could still be used to avoid
paying the cost of sorting the *entire* dataset upfront, but that could still
require putting the entire dataset in memory in the worst case).

Specific datastore implementations should keep this in mind, performing as much
work as possible in low-level, storage engine specific ways, and do the rest
using generators. In particular, always try to push ordering into the
underlying layer.

.. autofunction:: datastore.query.limit_gen

.. autofunction:: datastore.query.offset_gen

.. autofunction:: datastore.query.chain_gen

Other
-----

.. autofunction:: datastore.query._object_getattr


Examples
________

TODO
