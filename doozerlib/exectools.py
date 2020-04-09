"""
This module contains a set of functions for managing shell commands
consistently. It adds some logging and some additional capabilties to the
ordinary subprocess behaviors.
"""

from __future__ import absolute_import, print_function, unicode_literals

import subprocess
import time
import shlex
import os
import threading
import platform
import sys

from fcntl import fcntl, F_GETFL, F_SETFL
from os import O_NONBLOCK, read

from . import logutil
from . import pushd
from . import assertion
from .util import red_print, green_print, yellow_print, timer

SUCCESS = 0

logger = logutil.getLogger(__name__)


class RetryException(Exception):
    """
    Provide a custom exception for retry failures
    """
    pass


def retry(retries, task_f, check_f=bool, wait_f=None):
    """
    Try a function up to n times.
    Raise an exception if it does not pass in time

    :param retries int: The number of times to retry
    :param task_f func: The function to be run and observed
    :param func()bool check_f: a function to check if task_f is complete
    :param func()bool wait_f: a function to run between checks
    """
    for attempt in range(retries):
        ret = task_f()
        if check_f(ret):
            return ret
        if attempt < retries - 1 and wait_f is not None:
            wait_f(attempt)
    raise RetryException("Giving up after {} failed attempt(s)".format(retries))


def cmd_assert(cmd, retries=1, pollrate=60, on_retry=None, set_env=None):
    """
    Run a command, logging (using exec_cmd) and raise an exception if the
    return code of the command indicates failure.
    Try the command multiple times if requested.

    :param cmd <string|list>: A shell command
    :param retries int: The number of times to try before declaring failure
    :param pollrate int: how long to sleep between tries
    :param on_retry <string|list>: A shell command to run before retrying a failure
    :param set_env: Dict of env vars to set for command (overriding existing)
    :return: (stdout,stderr) if exit code is zero
    """

    for try_num in range(0, retries):
        if try_num > 0:
            logger.debug(
                "cmd_assert: Failed {} times. Retrying in {} seconds: {}".
                format(try_num, pollrate, cmd))
            time.sleep(pollrate)
            if on_retry is not None:
                cmd_gather(on_retry, set_env)  # no real use for the result though

        result, stdout, stderr = cmd_gather(cmd, set_env)
        if result == SUCCESS:
            break

    logger.debug("cmd_assert: Final result = {} in {} tries.".format(result, try_num))

    assertion.success(
        result,
        "Error running [{}] {}. See debug log.".
        format(pushd.Dir.getcwd(), cmd))

    return stdout, stderr


cmd_counter_lock = threading.Lock()
cmd_counter = 0  # Increments atomically to help search logs for command start/stop


def cmd_gather(cmd, set_env=None, realtime=False):
    """
    Runs a command and returns rc,stdout,stderr as a tuple.

    If called while the `Dir` context manager is in effect, guarantees that the
    process is executed in that directory, even if it is no longer the current
    directory of the process (i.e. it is thread-safe).

    :param cmd: The command and arguments to execute
    :param set_env: Dict of env vars to override in the current doozer environment.
    :param realtime: If True, output stdout and stderr in realtime instead of all at once.
    :return: (rc,stdout,stderr)
    """
    global cmd_counter, cmd_counter_lock

    with cmd_counter_lock:
        my_id = cmd_counter
        cmd_counter = cmd_counter + 1

    if not isinstance(cmd, list):
        cmd_list = shlex.split(cmd)
    else:
        # convert any non-str into str
        cmd_list = [str(c) for c in cmd]

    cwd = pushd.Dir.getcwd()
    cmd_info_base = f'${my_id}: {cmd_list} - [cwd={cwd}]'
    cmd_info = cmd_info_base

    env = os.environ.copy()
    if set_env:
        cmd_info = '{} [env={}]'.format(cmd_info, set_env)
        env.update(set_env)

    # Make sure output of launched commands is utf-8
    env['LC_ALL'] = 'en_US.UTF-8'

    with timer(logger.info, f'{cmd_info}: Executed:cmd_gather'):
        logger.info(f'{cmd_info}: Executing:cmd_gather')
        try:
            proc = subprocess.Popen(
                cmd_list, cwd=cwd, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except OSError as exc:
            logger.error("{}: Errored:\nException:\n{}\nIs {} installed?".format(
                cmd_info, exc, cmd_list[0]
            ))
            return exc.errno, "", "See previous error description."

        if not realtime:
            out, err = proc.communicate()
            rc = proc.returncode
        else:
            out = b''
            err = b''

            # Many thanks to http://eyalarubas.com/python-subproc-nonblock.html
            # setup non-blocking read
            # set the O_NONBLOCK flag of proc.stdout file descriptor:
            flags = fcntl(proc.stdout, F_GETFL)  # get current proc.stdout flags
            fcntl(proc.stdout, F_SETFL, flags | O_NONBLOCK)
            # set the O_NONBLOCK flag of proc.stderr file descriptor:
            flags = fcntl(proc.stderr, F_GETFL)  # get current proc.stderr flags
            fcntl(proc.stderr, F_SETFL, flags | O_NONBLOCK)

            rc = None
            while rc is None:
                output = None
                try:
                    output = read(proc.stdout.fileno(), 256)
                    green_print(output.rstrip())
                    out += output
                except OSError:
                    pass

                error = None
                try:
                    error = read(proc.stderr.fileno(), 256)
                    yellow_print(error.rstrip())
                    out += error
                except OSError:
                    pass

                rc = proc.poll()
                time.sleep(0.0001)  # reduce busy-wait

        # We read in bytes representing utf-8 output; decode so that python recognizes them as unicode strings
        out = out.decode('utf-8')
        err = err.decode('utf-8')
        logger.debug(
            "{}: Exited with: {}\nstdout>>{}<<\nstderr>>{}<<\n".
            format(cmd_info, rc, out, err))

    return rc, out, err


def fire_and_forget(cwd, shell_cmd, quiet=True):
    """
    Executes a command in a separate process that can continue to do its work
    even after doozer terminates.
    :param cwd: Path in which to launch the command
    :param shell_cmd: A string to send to the shell
    :param quiet: Whether to sink stdout & stderr to /dev/null
    :return: N/A
    """

    if quiet:
        shell_cmd = f'{shell_cmd} > /dev/null 2>/dev/null'

    # https://stackoverflow.com/a/13256908
    kwargs = {}
    if platform.system() == 'Windows':
        # from msdn [1]
        CREATE_NEW_PROCESS_GROUP = 0x00000200  # note: could get it from subprocess
        DETACHED_PROCESS = 0x00000008  # 0x8 | 0x200 == 0x208
        kwargs.update(creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)
    elif sys.version_info < (3, 2):  # assume posix
        kwargs.update(preexec_fn=os.setsid)
    else:  # Python 3.2+ and Unix
        kwargs.update(start_new_session=True)

    p = subprocess.Popen(f'{shell_cmd}', env=os.environ.copy(), shell=True, stdin=None, stdout=None, stderr=None,
                         cwd=cwd, close_fds=True, **kwargs)
    assert not p.poll()
