from __future__ import absolute_import, print_function, unicode_literals
import sys
if sys.version_info < (3, 6):
    sys.exit('Sorry, Python < 3.6 is not supported.')
import io
from .runtime import Runtime
from .pushd import Dir


def version():
    from os.path import abspath, dirname, join
    filename = join(dirname(abspath(__file__)), 'VERSION')
    return io.open(filename, encoding="utf-8").read().strip()
