from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)
__import__('pkg_resources').declare_namespace(__name__)

from datastore.core import *
from datastore.core import __version__
