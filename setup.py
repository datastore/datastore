#!/usr/bin/env python

import re
from setuptools import setup, find_packages

pkgname = 'datastore'

# gather the package information
main_py = open('datastore/core/__init__.py').read()
metadata = dict(re.findall("__([a-z]+)__ = '([^']+)'", main_py))
packages = [p for p in find_packages() if p.startswith(pkgname)]

# convert the readme to pypi compatible rst
try:
  try:
    import pypandoc
    readme = pypandoc.convert('README.md', 'rst')
  except ImportError:
    readme = open('README.md').read()
except:
  print('something went wrong reading the README.md file.')
  readme = ''

setup(
  name=pkgname,
  version=metadata['version'],
  description='simple, unified API for multiple data stores',
  long_description=readme,
  author=metadata['author'],
  author_email=metadata['email'],
  url='http://github.com/datastore/datastore',
  keywords=[
    'datastore',
    'unified api',
    'database',
  ],
  packages=packages,
  namespace_packages=['datastore'],
  test_suite='datastore.test',
  license='MIT License',
  classifiers=[
    'Topic :: Database',
    'Topic :: Database :: Front-Ends',
  ]
)
