import sys
if sys.version_info < (3, 6):
    sys.exit('Sorry, Python < 3.6 is not supported.')

from setuptools_scm import get_version

from .runtime import Runtime
from .pushd import Dir


def version():
    return get_version()
