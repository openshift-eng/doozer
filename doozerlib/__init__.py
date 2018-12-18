from .runtime import Runtime
from .pushd import Dir


def version():
    from os.path import abspath, dirname, join
    filename = join(dirname(abspath(__file__)), 'VERSION')
    return open(filename).read().strip()
