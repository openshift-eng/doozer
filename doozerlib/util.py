from __future__ import absolute_import, print_function, unicode_literals
import click
import copy
import os
import errno
import re
from datetime import datetime
from contextlib import contextmanager
from inspect import getframeinfo, stack


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


def convert_remote_git_to_https(source):
    """
    Accepts a source git URL in ssh or https format and return it in a normalized
    https format:
        - https protocol
        - no trailing /
    :param source: Git remote
    :return: Normalized https git URL
    """
    url = re.sub(
        pattern=r'[^@]+@([^:/]+)[:/]([^\.]+)',
        repl='https://\\1/\\2',
        string=source.strip(),
    )
    return re.sub(string=url, pattern=r'\.git$', repl='').rstrip('/')


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
    :param path: Str path or pathlib.Path
    """
    try:
        os.makedirs(str(path))
    except OSError as e:
        if e.errno != errno.EEXIST:  # ignore if dest_dir exists
            raise


@contextmanager
def timer(out_method, msg):
    caller = getframeinfo(stack()[2][0])  # Line that called this method
    caller_caller = getframeinfo(stack()[3][0])   # Line that called the method calling this method
    start_time = datetime.now()
    try:
        yield
    finally:
        time_elapsed = datetime.now() - start_time
        entry = f'Time elapsed (hh:mm:ss.ms) {time_elapsed} in {caller.filename}:{caller.lineno} from {caller_caller.filename}:{caller_caller.lineno}:{caller_caller.code_context[0].strip()} : {msg}'
        out_method(entry)


def analyze_debug_timing(file):
    peal = re.compile(r'^(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d),\d\d\d \w+ [(](\d+)[)] (.*)')

    thread_names = {}
    event_timings = {}  # maps internal to { <thread_name> => [event list] }
    first_interval = None

    def get_thread_name(thread):
        if thread in thread_names:
            return thread_names[thread]
        c = f'T{len(thread_names)}'
        thread_names[thread] = c
        return c

    def get_interval_map(interval):
        nonlocal first_interval
        if first_interval is None:
            first_interval = interval
        interval = interval - first_interval
        if interval in event_timings:
            return event_timings[interval]
        mapping = {}
        event_timings[interval] = mapping
        return mapping

    def get_thread_event_list(interval, thread):
        thread_name = get_thread_name(thread)
        interval_map = get_interval_map(interval)
        if thread_name in interval_map:
            return interval_map[thread_name]
        event_list = []
        interval_map[thread_name] = event_list
        return event_list

    def add_thread_event(interval, thread, event):
        get_thread_event_list(int(interval), thread).append(event)

    with open(file, 'r') as f:
        for line in f:
            m = peal.match(line.strip())
            if m:
                thread = m.group(2)  # thread id (e.g. 139770552305472)
                datestr = m.group(1)  # 2020-04-09 10:17:03,092
                event = m.group(3)
                date_time_obj = datetime.strptime(datestr, '%Y-%m-%d %H:%M:%S')
                minute_mark = int(int(date_time_obj.strftime("%s")) / 10)  # ten second intervals
                add_thread_event(minute_mark, thread, event)

    def print_em(*args):
        for a in args:
            print(str(a).ljust(5), end="")
        print('')

    print('Thread timelines')
    names = sorted(list(thread_names.values()), key=lambda e: int(e[1:]))  # sorts as T1, T2, T3, .... by removing 'T'
    print_em('*', *names)

    sorted_intervals = sorted(list(event_timings.keys()))
    for interval in range(0, sorted_intervals[-1] + 1):
        print_em(interval, *names)
        if interval in event_timings:
            interval_map = event_timings[interval]
            for i, thread_name in enumerate(names):
                events = interval_map.get(thread_name, [])
                for event in events:
                    with_event = list(names)
                    with_event[i] = thread_name + ': ' + event
                    print_em(f' {interval}', *with_event[:i + 1])
