"""Common tooling exceptions. Store them in this central place to
avoid circular imports
"""


class DoozerFatalError(Exception):
    """A broad exception for errors during Brew CRUD operations"""
    pass
