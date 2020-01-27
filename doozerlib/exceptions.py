"""Common tooling exceptions. Store them in this central place to
avoid circular imports
"""
from __future__ import absolute_import, print_function, unicode_literals


class DoozerFatalError(Exception):
    """A broad exception for errors during Brew CRUD operations"""
    pass
