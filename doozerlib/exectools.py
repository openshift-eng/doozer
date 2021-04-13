"""
This module contains a set of functions for managing shell commands
consistently. It adds some logging and some additional capabilties to the
ordinary subprocess behaviors.
"""

from __future__ import absolute_import, print_function, unicode_literals
import asyncio
from asyncio import events
import contextvars
import functools

import subprocess
import time
import shlex
import os
import threading
import platform
import sys
from typing import Dict, List, Optional, Tuple, Union
import urllib
import errno

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

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


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


def urlopen_assert(url_or_req, httpcode=200, retries=3):
    """
    Retries a URL pull several times before throwing a RetryException
    :param url_or_req: The URL to open with urllib  or a customized urllib.request.Request
    :param httpcode: The http numeric code indicating success
    :param retries: The number of retries to permit
    :return: The success result of urlopen (or a RetryException will be thrown)
    """
    return retry(retries, lambda: urllib.request.urlopen(url_or_req),
                 check_f=lambda req: req.code == httpcode,
                 wait_f=lambda x: time.sleep(30))


def cmd_assert(cmd, realtime=False, retries=1, pollrate=60, on_retry=None,
               set_env=None, strip=False, log_stdout: bool = False, log_stderr: bool = True):
    """
    Run a command, logging (using exec_cmd) and raise an exception if the
    return code of the command indicates failure.
    Try the command multiple times if requested.

    :param cmd <string|list>: A shell command
    :param realtime: If True, output stdout and stderr in realtime instead of all at once.
    :param retries int: The number of times to try before declaring failure
    :param pollrate int: how long to sleep between tries
    :param on_retry <string|list>: A shell command to run before retrying a failure
    :param set_env: Dict of env vars to set for command (overriding existing)
    :param strip: Strip extra whitespace from stdout/err before returning.
    :param log_stdout: Whether stdout should be logged into the DEBUG log.
    :param log_stderr: Whether stderr should be logged into the DEBUG log
    :return: (stdout,stderr) if exit code is zero
    """

    for try_num in range(0, retries):
        if try_num > 0:
            logger.debug(
                "cmd_assert: Failed {} times. Retrying in {} seconds: {}".
                format(try_num, pollrate, cmd))
            time.sleep(pollrate)
            if on_retry is not None:
                # Run the recovery command between retries. Nothing to collect or assert -- just try it.
                cmd_gather(on_retry, set_env)

        result, stdout, stderr = cmd_gather(cmd, set_env=set_env, realtime=realtime, strip=strip,
                                            log_stdout=log_stdout, log_stderr=log_stderr)
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


def cmd_gather(cmd, set_env=None, realtime=False, strip=False, log_stdout=False, log_stderr=True):
    """
    Runs a command and returns rc,stdout,stderr as a tuple.

    If called while the `Dir` context manager is in effect, guarantees that the
    process is executed in that directory, even if it is no longer the current
    directory of the process (i.e. it is thread-safe).

    :param cmd: The command and arguments to execute
    :param set_env: Dict of env vars to override in the current doozer environment.
    :param realtime: If True, output stdout and stderr in realtime instead of all at once.
    :param strip: Strip extra whitespace from stdout/err before returning.
    :param log_stdout: Whether stdout should be logged into the DEBUG log.
    :param log_stderr: Whether stderr should be logged into the DEBUG log
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
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.DEVNULL)
        except OSError as exc:
            description = "{}: Errored:\nException:\n{}\nIs {} installed?".format(cmd_info, exc, cmd_list[0])
            logger.error(description)
            return exc.errno, "", description

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
            stdout_complete = False
            stderr_complete = False
            while not stdout_complete or not stderr_complete or rc is None:
                output = None
                try:
                    output = read(proc.stdout.fileno(), 4096)
                    if output:
                        green_print(output.rstrip())
                        out += output
                    else:
                        stdout_complete = True
                except OSError as ose:
                    if ose.errno == errno.EAGAIN or ose.errno == errno.EWOULDBLOCK:
                        pass  # It is supposed to raise one of these exceptions
                    else:
                        raise

                error = None
                try:
                    error = read(proc.stderr.fileno(), 4096)
                    if error:
                        yellow_print(error.rstrip())
                        out += error
                    else:
                        stderr_complete = True
                except OSError as ose:
                    if ose.errno == errno.EAGAIN or ose.errno == errno.EWOULDBLOCK:
                        pass  # It is supposed to raise one of these exceptions
                    else:
                        raise

                if rc is None:
                    rc = proc.poll()
                    time.sleep(0.5)  # reduce busy-wait

        # We read in bytes representing utf-8 output; decode so that python recognizes them as unicode strings
        out = out.decode('utf-8')
        err = err.decode('utf-8')

        log_output_stdout = out
        log_output_stderr = err
        if not log_stdout and len(out) > 200:
            log_output_stdout = f'{out[:200]}\n..truncated..'
        if not log_stderr and len(err) > 200:
            log_output_stderr = f'{err[:200]}\n..truncated..'

        if rc:
            logger.debug(
                "{}: Exited with error: {}\nstdout>>{}<<\nstderr>>{}<<\n".
                format(cmd_info, rc, log_output_stdout, log_output_stderr))
        else:
            logger.debug(
                "{}: Exited with: {}\nstdout>>{}<<\nstderr>>{}<<\n".
                format(cmd_info, rc, log_output_stdout, log_output_stderr))

    if strip:
        out = out.strip()
        err = err.strip()

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


async def cmd_gather_async(cmd: Union[str, List[str]], text_mode=True, cwd: Optional[str] = None, set_env: Optional[Dict[str, str]] = None,
                           strip=False, log_stdout=False, log_stderr=True) -> Union[Tuple[int, str, str], Tuple[int, bytes, bytes]]:
    """Similar to cmd_gather, but run asynchronously

    :param cmd: The command and arguments to execute
    :param text_mode: True to decode stdout to string
    :param cwd: Set current working directory
    :param set_env: Dict of env vars to override in the current doozer environment.
    :param strip: Strip extra whitespace from stdout/err before returning. Requires text_mode = True.
    :param log_stdout: Whether stdout should be logged into the DEBUG log.
    :param log_stderr: Whether stderr should be logged into the DEBUG log
    :return: (rc, stdout, stderr)
    """
    if strip and not text_mode:
        raise ValueError("Can't strip if text_mode is False.")

    if not isinstance(cmd, list):
        cmd_list = shlex.split(cmd)
    else:
        cmd_list = [str(c) for c in cmd]
    if not cwd:
        cwd = pushd.Dir.getcwd()
    cmd_info = f"[cwd={cwd}]: {cmd_list}"

    logger.debug("Executing:cmd_gather %s", cmd_info)
    proc = await asyncio.create_subprocess_exec(
        *cmd_list,
        cwd=cwd,
        env=set_env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=subprocess.DEVNULL)

    out, err = await proc.communicate()
    rc = proc.returncode

    out_str = out.decode(encoding="utf-8") if text_mode else out.hex()
    err_str = err.decode(encoding="utf-8")

    log_output_stdout = out_str if log_stdout else f'{out_str[:200]}\n..truncated..'
    log_output_stderr = err_str if log_stderr else f'{err_str[:200]}\n..truncated..'

    if rc:
        logger.debug(
            "%s: Exited with error: %s\nstdout>>%s<<\nstderr>>%s<<\n",
            cmd_info, rc, log_output_stdout, log_output_stderr)
    else:
        logger.debug(
            "{}: Exited with: {}\nstdout>>{}<<\nstderr>>{}<<\n".
            format(cmd_info, rc, log_output_stdout, log_output_stderr))

    if text_mode:
        return (rc, out_str, err_str) if not strip else (rc, out_str.strip(), err_str.strip())
    else:
        return rc, out, err


async def cmd_assert_async(cmd: Union[str, List[str]], text_mode=True, retries=1, pollrate=60, on_retry: Optional[Union[str, List[str]]] = None,
                           cwd: Optional[str] = None, set_env: Optional[Dict[str, str]] = None, strip=False,
                           log_stdout: bool = False, log_stderr: bool = True) -> Union[Tuple[str, str], Tuple[bytes, bytes]]:
    """
    Similar to cmd_assert, but run asynchronously

    :param cmd: A shell command
    :param text_mode: True to decode stdout to string
    :param retries: The number of times to try before declaring failure
    :param pollrate: How long to sleep between tries
    :param on_retry: A shell command to run before retrying a failure
    :param cwd: Set current working directory
    :param set_env: Dict of env vars to set for command (overriding existing)
    :param strip: Strip extra whitespace from stdout/err before returning.
    :param log_stdout: Whether stdout should be logged into the DEBUG log.
    :param log_stderr: Whether stderr should be logged into the DEBUG log
    :return: (stdout,stderr) if exit code is zero
    """
    if retries <= 0:
        raise ValueError("`retries` must be greater than 0.")
    cmd_list = [str(c) for c in cmd] if isinstance(cmd, list) else shlex.split(cmd)
    if not cwd:
        cwd = pushd.Dir.getcwd()

    for try_num in range(0, retries):
        if try_num > 0:
            logger.debug(
                "cmd_assert: Failed %s times. Retrying in %s seconds: %s",
                try_num, pollrate, cmd_list)
            await asyncio.sleep(pollrate)
            if on_retry is not None:
                # Run the recovery command between retries. Nothing to collect or assert -- just try it.
                await cmd_gather_async(cmd_list, cwd=cwd, set_env=set_env)
        result, out, err = await cmd_gather_async(cmd_list, text_mode=text_mode, cwd=cwd, set_env=set_env, strip=strip, log_stdout=log_stdout, log_stderr=log_stderr)
        if result == SUCCESS:
            break

    logger.debug("cmd_assert: Final result = %s in %s tries.", result, try_num)
    assertion.success(result, "Error running [{}] {}. See debug log.".format(cwd, cmd_list))
    return out, err


async def to_thread(func, *args, **kwargs):
    """Asynchronously run function *func* in a separate thread.

    This function is a backport of asyncio.to_thread from Python 3.9.

    Any *args and **kwargs supplied for this function are directly passed
    to *func*. Also, the current :class:`contextvars.Context` is propogated,
    allowing context variables from the main thread to be accessed in the
    separate thread.

    Return a coroutine that can be awaited to get the eventual result of *func*.
    """
    loop = asyncio.get_event_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)


def limit_concurrency(limit=5):
    """A decorator to limit the number of parallel tasks with asyncio.

    It should be noted that when the decorator function is executed, the created Semaphore is bound to the default event loop.
    https://stackoverflow.com/a/66289885
    """
    # use asyncio.BoundedSemaphore(5) instead of Semaphore to prevent accidentally increasing the original limit (stackoverflow.com/a/48971158/6687477)
    sem = asyncio.BoundedSemaphore(limit)

    def executor(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            async with sem:
                return await func(*args, **kwargs)

        return wrapper

    return executor
