from __future__ import absolute_import, print_function, unicode_literals
import io
from .runtime import Runtime
from .pushd import Dir


def version():
    from os.path import abspath, dirname, join
    filename = join(dirname(abspath(__file__)), 'VERSION')
    return io.open(filename, encoding="utf-8").read().strip()
