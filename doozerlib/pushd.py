"""
This file contains the definition of a class that can be used like shell
pushd and popd.

The Dir object is a context_manager that can be used with the Python 'with'
clause.  The context manager idiom allows the user to execute some commands
in a working directory other than the CWD and return without needing to
explicitly handle it.
"""
from __future__ import absolute_import, print_function, unicode_literals

import threading
import pathlib


class Dir(object):
    """
    Context manager to handle directory changes safely.

    On `__enter__`, stores a pseudo current working directory. On __exit__
    any previous Dir context's cwd is restored.

    The current directory of the process (e.g. os.getcwd()) is never altered
    as this would not be a threadsafe operation.

    The exectools library honors these contexts automatically.
    """
    _tl = threading.local()

    def __init__(self, newdir):
        self.dir = str(newdir)
        self.previous_dir = None

    def __enter__(self):
        self.previous_dir = self.getcwd()
        self._tl.cwd = self.dir
        return self.dir

    def __exit__(self, *args):
        self._tl.cwd = self.previous_dir

    @classmethod
    def getcwd(cls):
        """
        Provide a context dependent current working directory. This method
        will return the directory currently holding the lock. If not within
        a Dir context, None will be returned.
        """
        if not hasattr(cls._tl, "cwd"):
            return None
        return cls._tl.cwd

    @classmethod
    def getpath(cls):
        """
        Provide a context dependent current working directory. This method
        will return a pathlib.Path for the directory currently holding the lock.
        """
        cwd = Dir.getcwd()
        return None if not cwd else pathlib.Path(Dir.getcwd())
