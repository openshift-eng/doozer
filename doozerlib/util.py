from __future__ import absolute_import, print_function, unicode_literals
import click
import copy
import os
import errno


def stringify(val):
    """
    Accepts either str or bytes and returns a str
    """
    try:
        val = val.decode('utf-8')
    except (UnicodeDecodeError, AttributeError):
        pass
    return val


def red_prefix(msg, file=None):
    """Print out a message prefix in bold red letters, like for "Error: "
messages"""
    click.secho(stringify(msg), nl=False, bold=True, fg='red', file=file)


def red_print(msg, file=None):
    """Print out a message in red text"
messages"""
    click.secho(stringify(msg), nl=True, bold=False, fg='red', file=file)


def green_prefix(msg, file=None):
    """Print out a message prefix in bold green letters, like for "Success: "
messages"""
    click.secho(stringify(msg), nl=False, bold=True, fg='green', file=file)


def green_print(msg, file=None):
    """Print out a message in green text"""
    click.secho(stringify(msg), nl=True, bold=False, fg='green', file=file)


def yellow_prefix(msg, file=None):
    """Print out a message prefix in bold yellow letters, like for "Success: "
messages"""
    click.secho(stringify(msg), nl=False, bold=True, fg='yellow', file=file)


def yellow_print(msg, file=None):
    """Print out a message in yellow text"""
    click.secho(stringify(msg), nl=True, bold=False, fg='yellow', file=file)


def cprint(msg, file=None):
    """Wrapper for click.echo"""
    click.echo(stringify(msg), file=file)


def color_print(msg, color='white', nl=True, file=None):
    """Print out a message in given color"""
    click.secho(stringify(msg), nl=nl, bold=False, fg=color, file=file)


DICT_EMPTY = object()


def dict_get(dct, path, default=DICT_EMPTY):
    dct = copy.deepcopy(dct)  # copy to not modify original
    for key in path.split('.'):
        try:
            dct = dct[key]
        except KeyError:
            if default is DICT_EMPTY:
                raise Exception('Unable to follow key path {}'.format(path))
            return default
    return dct


def is_in_directory(path, directory):
    """check whether a path is in another directory

    FIXME: Use os.path.commonpath when migrated to Python 3
    """
    path = os.path.realpath(path)
    directory = os.path.realpath(directory)
    relative = os.path.relpath(os.path.dirname(path), directory)
    return relative != os.pardir and not relative.startswith(os.pardir + os.sep)


def mkdirs(path):
    """ Make sure a directory exists. Similar to shell command `mkdir -p`.

    This function will not be necessary when fully migrated to Python 3.
    """
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:  # ignore if dest_dir exists
            raise
