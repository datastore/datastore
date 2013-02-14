
__version__ = '1.1'
__author__ = 'Juan Batiz-Benet <juan@benet.ai>'
__doc__ = '''
filesystem datastore implementation.

Tested with:
  * Journaled HFS+ (Mac OS X 10.7.2)

'''

import os
import datastore.core


def ensure_directory_exists(directory):
  '''Ensures `directory` exists. May make `directory` and intermediate dirs.
  Raises RuntimeError if `directory` is a file.
  '''
  if not os.path.exists(directory):
    os.makedirs(directory)
  elif os.path.isfile(directory):
    raise RuntimeError('Path %s is a file, not a directory.' % directory)



class FileSystemDatastore(datastore.Datastore):
  '''Simple flat-file datastore.

  FileSystemDatastore will store objects in independent files in the host's
  filesystem. The FileSystemDatastore is initialized with a `root` path, under
  which to store all objects. Each object will be stored under its own file:
  `root`/`key`.obj

  The `key` portion also replaces namespace parameter delimiters (:) with
  slashes, creating several nested directories. For example, storing objects
  under `root` path '/data' with the following keys::

    Key('/Comedy:MontyPython/Actor:JohnCleese')
    Key('/Comedy:MontyPython/Sketch:ArgumentClinic')
    Key('/Comedy:MontyPython/Sketch:CheeseShop')
    Key('/Comedy:MontyPython/Sketch:CheeseShop/Character:Mousebender')

  will yield the file structure::

    /data/Comedy/MontyPython/Actor/JohnCleese.obj
    /data/Comedy/MontyPython/Sketch/ArgumentClinic.obj
    /data/Comedy/MontyPython/Sketch/CheeseShop.obj
    /data/Comedy/MontyPython/Sketch/CheeseShop/Character/Mousebender.obj

  Implementation Notes:

    Separating key namespaces (and their parameters) within directories allows
    granular querying for under a specific key. For example, a query with key::

      Key('/data/Comedy:MontyPython/Sketch:CheeseShop')

    will query for all objects under `Sketch:CheeseShop` independently of
    queries for::

      Key('/data/Comedy:MontyPython/Sketch')

    Also, using the `.obj` extension gets around the ambiguity of having both a
    `CheeseShop` object and directory::

      /data/Comedy/MontyPython/Sketch/CheeseShop.obj
      /data/Comedy/MontyPython/Sketch/CheeseShop/


  Hello World:

      >>> import datastore.filesystem
      >>>
      >>> ds = datastore.filesystem.FileSystemDatastore('/tmp/.test_datastore')
      >>>
      >>> hello = datastore.Key('hello')
      >>> ds.put(hello, 'world')
      >>> ds.contains(hello)
      True
      >>> ds.get(hello)
      'world'
      >>> ds.delete(hello)
      >>> ds.get(hello)
      None

  '''

  object_extension = '.obj'
  ignore_list = list()

  def __init__(self, root, case_sensitive=True):
    '''Initialize the datastore with given root directory `root`.

    Args:
      root: A path at which to mount this filesystem datastore.
    '''
    root = os.path.normpath(root)

    if not root:
      errstr = 'root path must not be empty (\'.\' for current directory)'
      raise ValueError(errstr)

    ensure_directory_exists(root)

    self.root_path = root
    self.case_sensitive = bool(case_sensitive)


  # object pathing

  def relative_path(self, key):
    '''Returns the relative path for given `key`'''
    key = str(key)                # stringify
    key = key.replace(':', '/')   # turn namespace delimiters into slashes
    key = key[1:]                 # remove first slash (absolute)
    if not self.case_sensitive:
      key = key.lower()           # coerce to lowercase
    return os.path.normpath(key)

  def path(self, key):
    '''Returns the `path` for given `key`'''
    return os.path.join(self.root_path, self.relative_path(key))

  def relative_object_path(self, key):
    '''Returns the relative path for object pointed by `key`.'''
    return self.relative_path(key) + self.object_extension

  def object_path(self, key):
    '''return the object path for `key`.'''
    return os.path.join(self.root_path, self.relative_object_path(key))


  # object IO

  def _write_object(self, path, value):
    '''write out `object` to file at `path`'''
    ensure_directory_exists(os.path.dirname(path))

    with open(path, 'w') as f:
      f.write(value)

  def _read_object(self, path):
    '''read in object from file at `path`'''
    if not os.path.exists(path):
      return None

    if os.path.isdir(path):
      raise RuntimeError('%s is a directory, not a file.' % path)

    with open(path) as f:
      file_contents = f.read()

    return file_contents

  def _read_object_gen(self, iterable):
    '''Generator that reads objects in from filenames in `iterable`.'''
    for filename in iterable:
      yield self._read_object(filename)


  # Datastore implementation

  def get(self, key):
    '''Return the object named by key or None if it does not exist.

    Args:
      key: Key naming the object to retrieve

    Returns:
      object or None
    '''
    path = self.object_path(key)
    return self._read_object(path)


  def put(self, key, value):
    '''Stores the object `value` named by `key`.

    Args:
      key: Key naming `value`
      value: the object to store.
    '''
    path = self.object_path(key)
    self._write_object(path, value)

  def delete(self, key):
    '''Removes the object named by `key`.

    Args:
      key: Key naming the object to remove.
    '''
    path = self.object_path(key)
    if os.path.exists(path):
      os.remove(path)

    #TODO: delete dirs if empty?

  def query(self, query):
    '''Returns an iterable of objects matching criteria expressed in `query`
    FSDatastore.query queries all the `.obj` files within the directory
    specified by the query.key.

    Args:
      query: Query object describing the objects to return.

    Raturns:
      Cursor with all objects matching criteria
    '''
    path = self.path(query.key)

    if os.path.exists(path):
      filenames = os.listdir(path)
      filenames = list(set(filenames) - set(self.ignore_list))
      filenames = map(lambda f: os.path.join(path, f), filenames)
      iterable = self._read_object_gen(filenames)
    else:
      iterable = list()

    return query(iterable) # must apply filters, etc naively.

  def contains(self, key):
    '''Returns whether the object named by `key` exists.
    Optimized to only check whether the file object exists.

    Args:
      key: Key naming the object to check.

    Returns:
      boalean whether the object exists
    '''
    path = self.object_path(key)
    return os.path.exists(path) and os.path.isfile(path)
