"""Common tooling exceptions. Store them in this central place to
avoid circular imports
"""

import sys
import traceback

from future.utils import as_native_str


class DoozerFatalError(Exception):
    """A broad exception for errors during Brew CRUD operations"""
    pass


class WrapException(Exception):
    """ https://bugs.python.org/issue13831 """
    def __init__(self):
        super(WrapException, self).__init__()
        exc_type, exc_value, exc_tb = sys.exc_info()
        self.exception = exc_value
        self.formatted = "".join(
            traceback.format_exception(exc_type, exc_value, exc_tb))

    @as_native_str()
    def __str__(self):
        return "{}\nOriginal traceback:\n{}".format(Exception.__str__(self), self.formatted)
