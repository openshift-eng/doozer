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
from typing import List, Tuple, Dict, Optional

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


def get_build_objects(ids_or_nvrs, session):
    """Get information of multiple Koji/Brew builds

    :param ids_or_nvrs: list of build nvr strings or numbers.
    :param session: instance of :class:`koji.ClientSession`
    :return: a list Koji/Brew build objects
    """
    logger.debug(
        "Fetching build info for {} from Koji/Brew...".format(ids_or_nvrs))
    # Use Koji multicall interface to boost performance. See https://pagure.io/koji/pull-request/957
    tasks = []
    with session.multicall(strict=True) as m:
        for b in ids_or_nvrs:
            tasks.append(m.getBuild(b))
    return [task.result for task in tasks]


def get_latest_builds(tag_component_tuples: List[Tuple[str, str]], session: koji.ClientSession) -> List[Optional[List[Dict]]]:
    """ Get latest builds for multiple Brew components

    :param tag_component_tuples: List of (tag, component_name) tuples
    :param session: instance of Brew session
    :return: a list Koji/Brew build objects
    """
    tasks = []
    with session.multicall(strict=True) as m:
        for tag, component_name in tag_component_tuples:
            tasks.append(m.getLatestBuilds(tag, package=component_name))
    return [task.result for task in tasks]


def list_archives_by_builds(build_ids: List[int], build_type: str, session: koji.ClientSession) -> List[Optional[List[Dict]]]:
    """ Retrieve information about archives by builds
    :param build_ids: List of build IDs
    :param build_type: build type, such as "image"
    :param session: instance of Brew session
    :return: a list of Koji/Brew archive lists
    """
    tasks = []
    with session.multicall(strict=True) as m:
        for build_id in build_ids:
            if not build_id:
                tasks.append(None)
            tasks.append(m.listArchives(buildID=build_id, type=build_type))
    return [task.result if task else None for task in tasks]
