import subprocess
import os
import tempfile
import atexit
import shutil
import sys
import logging

try:
    DEVNULL = subprocess.DEVNULL  # Python 3
except AttributeError:
    DEVNULL = open(os.devnull)  # Python 2
try:
    ChildProcessError   # Python 3
except NameError:
    ChildProcessError = IOError  # Python 2

DOOZER_CMD = [sys.executable, "-m", "doozerlib.cli", "--debug"]
DOOZER_ENV = {  # never prompt on the terminal
    "GIT_SSH_COMMAND": "ssh -oBatchMode=yes -oStrictHostKeyChecking=no",
    "GIT_TERMINAL_PROMPT": "0",
}

BREW_HUB = "https://brewhub.engineering.redhat.com/brewhub"


def run_doozer(doozer_args=[], checked=True):
    """ Executes Doozer as a subprocess with current Python interpreter.

    :param doozer_args: List of additional Doozer arguments
    :param checked: True to raise an Error if exit code is not zero
    """
    args = DOOZER_CMD \
        + ["--working-dir=" + get_working_dir()] \
        + doozer_args

    set_env = os.environ.copy()
    set_env.update(DOOZER_ENV)
    p = subprocess.Popen(
        args,
        env=set_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=DEVNULL,
    )
    out, err = p.communicate()
    out = out.decode("utf-8")
    err = err.decode("utf-8")
    exit_code = p.wait()
    if checked and exit_code != 0:
        fmt = "Process {} exited with code {}, stdout={}, stderr={}"
        raise ChildProcessError(fmt.format(args, exit_code, out, err))
    return exit_code, out, err


working_dir = None


def _cleanup_working_dir():
    global working_dir
    if working_dir:
        shutil.rmtree(working_dir)


def get_working_dir():
    global working_dir
    if not working_dir:
        working_dir = tempfile.mkdtemp(prefix="doozertest-working-")
        atexit.register(_cleanup_working_dir)
    return working_dir
