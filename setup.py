#!/usr/bin/env python

import re
from setuptools import setup, find_packages

pkgname = 'datastore'

# gather the package information
main_py = open('%s/__init__.py' % pkgname).read()
metadata = dict(re.findall("__([a-z]+)__ = '([^']+)'", main_py))
packages = filter(lambda p: p.startswith(pkgname), find_packages())

# convert the readme to pypi compatible rst
try:
  try:
    import pypandoc
    readme = pypandoc.convert('README.md', 'rst')
  except ImportError:
    readme = open('README.md').read()
except:
  print 'something went wrong reading the README.md file.'
  readme = ''

setup(
  name=pkgname,
  version=metadata['version'],
  description='simple, unified API for multiple data stores',
  long_description=readme,
  author=metadata['author'],
  author_email=metadata['email'],
  url='http://github.com/jbenet/datastore',
  keywords=[
    'datastore',
    'unified api',
    'database',
  ],
  packages=packages,
  install_requires=['smhasher==0.136.2'],
  test_suite='datastore.test',
  license='MIT License',
  classifiers=[
    'Topic :: Database',
    'Topic :: Database :: Front-Ends',
  ]
)
