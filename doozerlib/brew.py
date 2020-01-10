"""
Utility functions for general interactions with Brew and Builds
"""
from __future__ import absolute_import, print_function, unicode_literals

# stdlib
import ast
import time
import datetime
import subprocess
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import cpu_count
from multiprocessing import Lock
import shlex
import traceback

from . import exectools
from . import logutil

# 3rd party
import click
import koji
import koji_cli.lib

logger = logutil.getLogger(__name__)

# ============================================================================
# Brew/Koji service interaction functions
# ============================================================================

# Populated by watch_task. Each task_id will be a key in the dict and
# each value will be a TaskInfo: https://github.com/openshift/enterprise-images/pull/178#discussion_r173812940
watch_task_info = {}
# Protects threaded access to watch_task_info
watch_task_lock = Lock()


def get_watch_task_info_copy():
    """
    :return: Returns a copy of the watch_task info dict in a thread safe way. Each key in this dict
     is a task_id and each value is a koji TaskInfo with potentially useful data.
     https://github.com/openshift/enterprise-images/pull/178#discussion_r173812940
    """
    with watch_task_lock:
        return dict(watch_task_info)


def watch_task(brew_hub, log_f, task_id, terminate_event):
    end = time.time() + 4 * 60 * 60
    watcher = koji_cli.lib.TaskWatcher(
        task_id,
        koji.ClientSession(brew_hub, opts={'serverca': '/etc/pki/brew/legacy.crt'}),
        quiet=True)
    error = None
    except_count = 0
    while error is None:
        try:
            watcher.update()
            except_count = 0

            # Keep around metrics for each task we watch
            with watch_task_lock:
                watch_task_info[task_id] = dict(watcher.info)

            if watcher.is_done():
                return None if watcher.is_success() else watcher.get_failure()
            log_f("Task state: " + koji.TASK_STATES[watcher.info['state']])
        except:
            except_count += 1
            # possible for watcher.update() to except during connection issue, try again
            log_f('watcher.update() exception. Trying again in 60s.\n{}'.format(traceback.format_exc()))
            if except_count >= 10:
                log_f('watcher.update() excepted 10 times. Giving up.')
                error = traceback.format_exc()
                break

        if terminate_event.wait(timeout=3 * 60):
            error = 'Interrupted'
        elif time.time() > end:
            error = 'Timeout building image'

    log_f(error + ", canceling build")
    subprocess.check_call(("brew", "cancel", str(task_id)))
    return error


def check_rpm_buildroot(name, branch, arch='x86_64'):
    """
    Query the buildroot used by ODCS to determine if a given RPM name
    is provided by ODCS for the given arch.
    :param str name: RPM name
    :param str branch: Current building branch, such as rhaos-3.10-rhel-7
    :param str arch: CPU architecture to search
    """
    args = locals()
    query = 'repoquery --repofrompath foo,"http://download-node-02.eng.bos.redhat.com/brewroot/repos/{branch}-ppc64le-container-build/latest/{arch}" --repoid=foo --arch {arch},noarch --whatprovides {name}'
    rc, stdout, stderr = exectools.cmd_gather(query.format(**args))
    if rc == 0:
        result = []
        stdout = stdout.strip()
        for rpm in stdout.strip().splitlines():
            n = rpm.split(':')[0]
            n = '-'.join(n.split('-')[0:-1])
            result.append(n)

        return result
    else:
        raise ValueError(stderr)
