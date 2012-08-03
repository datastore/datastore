#!/usr/bin/env python

from setuptools import setup, find_packages

__version__ = '0.2.8'
# don't forget to update datastore/__init__.py

packages = filter(lambda p: p.startswith('datastore'), find_packages())

setup(
  name="datastore",
  version=__version__,
  description="simple, unified API for multiple data stores",
  author="Juan Batiz-Benet",
  author_email="juan@benet.ai",
  url="http://github.com/jbenet/datastore",
  keywords=["datastore", "unified api", "memcached", "redis", "git", "mongo"],
  packages=packages,
  install_requires=["smhasher"],
  license="MIT License",
  classifiers=[
    "Topic :: Database :: Front-Ends"
  ]
)
