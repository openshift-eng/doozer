from __future__ import absolute_import, print_function, unicode_literals
from pathlib import Path
import click
import copy
import os
import yaml
import re
from os.path import abspath
from typing import Dict
from datetime import datetime
from contextlib import contextmanager
from inspect import getframeinfo, stack

from sys import getsizeof, stderr
from itertools import chain
from collections import deque
try:
    from reprlib import repr
except ImportError:
    pass

from doozerlib import exectools, constants


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


def remove_prefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix):]
    else:
        return s[:]


def remove_prefixes(s: str, *args) -> str:
    for prefix in args:
        s = remove_prefix(s, prefix)
    return s


def remove_suffix(s: str, suffix: str) -> str:
    # suffix='' should not call self[:-0].
    if suffix and s.endswith(suffix):
        return s[:-len(suffix)]
    else:
        return s[:]


def convert_remote_git_to_https(source_url: str):
    """
    Accepts a source git URL in ssh or https format and return it in a normalized
    https format (:port on servers is not supported):
        - https protocol
        - no trailing /
    :param source_url: Git remote
    :return: Normalized https git URL
    """
    url = source_url.strip().rstrip('/')
    url = remove_prefixes(url, 'http://', 'https://', 'git://', 'git@', 'ssh://')
    url = remove_suffix(url, '.git')
    url = url.split('@', 1)[-1]  # Strip username@

    if url.find(':') > -1:
        server, org_repo = url.rsplit(':', 1)
    elif url.rfind('/') > -1:
        server, org_repo = url.rsplit('/', 1)
    else:
        return f'https://{url}'  # weird..

    return f'https://{server}/{org_repo}'


def split_git_url(url) -> (str, str, str):
    """
    :param url: A remote ssh or https github url
    :return: Splits a github url into the server name, org, and repo name
    """
    https_normalized = convert_remote_git_to_https(url)
    url = https_normalized[8:]  # strip https://
    server, repo = url.split('/', 1)  # e.g. 'github.com', 'openshift/origin'
    org, repo_name = repo.split('/', 1)
    return server, org, repo_name


def convert_remote_git_to_ssh(url):
    """
    Accepts a remote git URL and turns it into a git@
    ssh form.
    :param url: The initial URL
    :return: A url in git@server:repo.git
    """
    server, org, repo_name = split_git_url(url)
    return f'git@{server}:{org}/{repo_name}.git'


def setup_and_fetch_public_upstream_source(public_source_url: str, public_upstream_branch: str, source_dir: str):
    """
    Fetch public upstream source for specified Git repository. Set up public_upstream remote if needed.

    :param public_source_url: HTTPS Git URL of the public upstream source
    :param public_upstream_branch: Git branch of the public upstream source
    :param source_dir: Path to the local Git repository
    """
    out, err = exectools.cmd_assert(["git", "-C", source_dir, "remote"])
    if 'public_upstream' not in out.strip().split():
        exectools.cmd_assert(["git", "-C", source_dir, "remote", "add", "--", "public_upstream", public_source_url])
    else:
        exectools.cmd_assert(["git", "-C", source_dir, "remote", "set-url", "--", "public_upstream", public_source_url])
    exectools.cmd_assert(["git", "-C", source_dir, "fetch", "--", "public_upstream", public_upstream_branch], retries=3, set_env=constants.GIT_NO_PROMPTS)


def is_commit_in_public_upstream(revision: str, public_upstream_branch: str, source_dir: str):
    """
    Determine if the public upstream branch includes the specified commit.

    :param revision: Git commit hash or reference
    :param public_upstream_branch: Git branch of the public upstream source
    :param source_dir: Path to the local Git repository
    """
    cmd = ["git", "merge-base", "--is-ancestor", "--", revision, "public_upstream/" + public_upstream_branch]
    # The command exits with status 0 if true, or with status 1 if not. Errors are signaled by a non-zero status that is not 1.
    # https://git-scm.com/docs/git-merge-base#Documentation/git-merge-base.txt---is-ancestor
    rc, out, err = exectools.cmd_gather(cmd)
    if rc == 0:
        return True
    if rc == 1:
        return False
    raise IOError(f"Couldn't determine if the commit {revision} is in the public upstream source repo. `git merge-base` exited with {rc}, stdout={out}, stderr={err}")


def is_in_directory(path: os.PathLike, directory: os.PathLike):
    """check whether a path is in another directory
    """
    a = Path(path).parent.resolve()
    b = Path(directory).resolve()
    try:
        a.relative_to(b)
        return True
    except ValueError:
        return False


def mkdirs(path):
    """ Make sure a directory exists. Similar to shell command `mkdir -p`.
    :param path: Str path or pathlib.Path
    """
    os.makedirs(str(path), exist_ok=True)


@contextmanager
def timer(out_method, msg):
    caller = getframeinfo(stack()[2][0])  # Line that called this method
    caller_caller = getframeinfo(stack()[3][0])   # Line that called the method calling this method
    start_time = datetime.now()
    try:
        yield
    finally:
        time_elapsed = datetime.now() - start_time
        entry = f'Time elapsed (hh:mm:ss.ms) {time_elapsed} in {caller.filename}:{caller.lineno} from {caller_caller.filename}:{caller_caller.lineno}:{caller_caller.code_context[0].strip() if caller_caller.code_context else ""} : {msg}'
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


def what_is_in_master() -> str:
    """
    :return: Returns a string like "4.6" to identify which release currently resides in master branch.
    """
    # The promotion target of the openshift/images master branch defines this release master is associated with.
    ci_config_url = 'https://raw.githubusercontent.com/openshift/release/master/ci-operator/config/openshift/images/openshift-images-master.yaml'
    content = exectools.urlopen_assert(ci_config_url).read()
    ci_config = yaml.safe_load(content)
    # Look for something like: https://github.com/openshift/release/blob/251cb12e913dcde7be7a2b36a211650ed91c45c4/ci-operator/config/openshift/images/openshift-images-master.yaml#L64
    target_release = ci_config.get('promotion', {}).get('name', None)
    if not target_release:
        red_print(content)
        raise IOError('Unable to find which openshift release resides in master')
    return target_release


def extract_version_fields(version, at_least=0):
    """
    For a specified version, return a list with major, minor, patch.. isolated
    as integers.
    :param version: A version to parse
    :param at_least: The minimum number of fields to find (else raise an error)
    """
    fields = [int(f) for f in version.strip().split('-')[0].lstrip('v').split('.')]  # v1.17.1 => [ '1', '17', '1' ]
    if len(fields) < at_least:
        raise IOError(f'Unable to find required {at_least} fields in {version}')
    return fields


def get_cincinnati_channels(major, minor):
    """
    :param major: Major for release
    :param minor: Minor version for release.
    :return: Returns the Cincinnati graph channels associated with a release
             in promotion order (e.g. candidate -> stable)
    """
    major = int(major)
    minor = int(minor)

    if major != 4:
        raise IOError('Unable to derive previous for non v4 major')

    prefixes = ['candidate', 'fast', 'stable']
    if major == 4 and minor == 1:
        prefixes = ['prerelease', 'stable']

    return [f'{prefix}-{major}.{minor}' for prefix in prefixes]


def get_docker_config_json(config_dir):
    flist = os.listdir(abspath(config_dir))
    if 'config.json' in flist:
        return abspath(os.path.join(config_dir, 'config.json'))
    else:
        raise FileNotFoundError("Can not find the registry config file in {}".format(config_dir))


# https://code.activestate.com/recipes/577504/
def total_size(o, handlers={}, verbose=False):
    """ Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """
    dict_handler = lambda d: chain.from_iterable(d.items())
    all_handlers = {
        tuple: iter,
        list: iter,
        deque: iter,
        dict: dict_handler,
        set: iter,
        frozenset: iter,
    }
    all_handlers.update(handlers)  # user handlers take precedence
    seen = set()  # track which object id's have already been seen
    default_size = getsizeof(0)  # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:  # do not double count the same object
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        if verbose:
            print(s, type(o), repr(o), file=stderr)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)
