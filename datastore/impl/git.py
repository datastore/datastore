
__version__ = '1.1'
__author__ = 'Juan Batiz-Benet <juan@benet.ai>'
__doc__ = '''
git datastore implementation.

Tested with:
  * git 1.7.5.4

'''


import os
import subprocess

from datastore.impl.filesystem import FileSystemDatastore
from datastore.serialize import prettyjson


try:
  import dulwich
except ImportError:
  import warnings
  warnings.warn('GitDatastore: error importing dulwich. defaulting to git cli')
  dulwich = None



class git_interface(object):
  '''git interface supporting GitDatastore'''

  author = 'GitDatastore <git.datastore>'

  def __init__(self, repository_path):
    '''initialize the interface with `repository_path`'''
    self.path = os.path.normpath(repository_path)
    self.init()

    git_dir = os.path.join(repository_path, '.git')
    errstr = '%s failed to initialize repository at %s'
    assert os.path.isdir(git_dir), errstr % (self, repository_path)

  def init(self):
    '''Create an empty git repository or reinitialize an existing one'''
    pass

  def commit(self, message, author=None):
    '''Record changes to the repository'''
    pass

  def add(self, path):
    '''Add file to the staging area.'''
    pass



class git_cli_interface(git_interface):
  '''git interface supporting GitDatastore using the git cli.'''

  @staticmethod
  def system(cmd):
    '''Run a command on the commandline.
    use Popen. It is faster than os.system and we can monkey patch it in gevent
    '''
    subprocess.Popen(cmd, shell=True).wait()

  def cli(self, cmd):
    '''Performs git command `cmd` on the git cli'''
    self.system('cd %s && git %s > /dev/null' % (self.path, cmd))

  def init(self):
    '''Create an empty git repository or reinitialize an existing one'''
    self.cli('init')

  def commit(self, message, author=None):
    '''Record changes to the repository'''
    author = author or self.author
    self.cli('commit --message "%s" --author "%s"' % (message, author))

  def add(self, path):
    '''Add file to the staging area.'''
    self.cli('add "%s"' % path)



class git_dulwich_interface(git_interface):
  '''git interface supporting GitDatastore using dulwich.'''

  def init(self):
    '''Create an empty git repository or reinitialize an existing one'''
    gitdir = os.path.join(self.path, '.git')
    if os.path.isdir(gitdir):
      self.repo = dulwich.repo.Repo(self.path)
    else:
      self.repo = dulwich.repo.Repo.init(self.path)

  def commit(self, message, author=None):
    '''Record changes to the repository'''
    author = author or self.author
    self.repo.do_commit(committer=author, author=author, message=message)

  def add(self, path):
    '''Add file to the staging area.'''
    self.repo.stage([path])




class GitDatastore(FileSystemDatastore):
  '''git version controlled datastore, on top of FileSystemDatastore.'''

  git_interface = git_dulwich_interface if dulwich else git_cli_interface
  ignore_list = ['.git']

  def __init__(self, root, auto_commit=True, **kwargs):
    '''Initialize the datastore with given root directory `root`.

    Args:
      root: A path at which to mount this filesystem datastore.

      serializer: An optional value serializer to use instead of prettyjson.
        Serializer must respond to `loads` and `dumps`.
    '''
    super(GitDatastore, self).__init__(root, **kwargs)

    # initialize git
    self.git = self.git_interface(self.root_path)
    self.auto_commit = auto_commit


  def commit(self, message, **kwargs):
    '''Performs a git commit'''
    message += '\n\nGitDatastore commit'
    self.git.commit(message, **kwargs)

  def stage(self, key):
    '''Stage the file for object named by `key`'''
    self.git.add(self.relative_object_path(key))


  def put(self, key, value):
    '''Stores the object.'''
    super(GitDatastore, self).put(key, value)
    self.stage(key)
    if self.auto_commit:
      self.commit('put %s' % key)

  def delete(self, key):
    '''Removes the object.'''
    path = self.object_path(key)
    if not os.path.exists(path):
      return

    super(GitDatastore, self).delete(key)
    self.stage(key)
    if self.auto_commit:
      self.commit('delete %s' % key)


'''
Hello World:

    >>> import datastore
    >>> from datastore.impl.git import GitDatastore
    >>>
    >>> ds = GitDatastore('/tmp/.test_datastore')
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
