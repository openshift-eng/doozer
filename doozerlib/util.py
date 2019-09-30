import click
import copy
import os
import errno


def red_prefix(msg):
    """Print out a message prefix in bold red letters, like for "Error: "
messages"""
    click.secho(msg, nl=False, bold=True, fg='red')


def red_print(msg):
    """Print out a message in red text"
messages"""
    click.secho(msg, nl=True, bold=False, fg='red')


def green_prefix(msg):
    """Print out a message prefix in bold green letters, like for "Success: "
messages"""
    click.secho(msg, nl=False, bold=True, fg='green')


def green_print(msg):
    """Print out a message in green text"""
    click.secho(msg, nl=True, bold=False, fg='green')


def yellow_prefix(msg):
    """Print out a message prefix in bold yellow letters, like for "Success: "
messages"""
    click.secho(msg, nl=False, bold=True, fg='yellow')


def yellow_print(msg):
    """Print out a message in yellow text"""
    click.secho(msg, nl=True, bold=False, fg='yellow')


def cprint(msg):
    """Wrapper for click.echo"""
    click.echo(msg)


def color_print(msg, color='white', nl=True):
    """Print out a message in given color"""
    click.secho(msg, nl=nl, bold=False, fg=color)


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
