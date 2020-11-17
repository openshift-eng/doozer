"""
Utility functions for general interactions with Brew and Builds
"""
from __future__ import absolute_import, print_function, unicode_literals

# stdlib
import time
import threading
import subprocess
from multiprocessing import Lock
import traceback
import requests
import json
from typing import List, Tuple, Dict, Optional

from . import logutil

# 3rd party
import koji
import koji_cli.lib

from .model import Missing
from .util import red_print, total_size

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


def watch_task(session, log_f, task_id, terminate_event):
    end = time.time() + 4 * 60 * 60
    watcher = koji_cli.lib.TaskWatcher(
        task_id,
        session,
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
    canceled = session.cancelTask(task_id, recurse=True)
    if canceled:
        log_f(f"Brew task {task_id} was canceled.")
    else:
        log_f(f"Brew task {task_id} was NOT canceled.")
    return error


def watch_tasks(session, log_f, task_ids, terminate_event):
    """ Watch Koji Tasks for completion
    :param session: Koji client session
    :param log_f: a log function
    :param task_ids: a list of task IDs
    :param terminate_event: terminate event
    :return: a dict of task ID and error message mappings
    """
    if not task_ids:
        return
    end = time.time() + 4 * 60 * 60
    watchers = {}
    errors = {}
    except_counts = {}
    for task_id in task_ids:
        watchers[task_id] = koji_cli.lib.TaskWatcher(task_id, session, quiet=True)
        except_counts[task_id] = 0
    tasks_to_poll = set(watchers.keys())
    tasks_to_cancel = set()
    while True:
        for task_id in tasks_to_poll.copy():
            watcher = watchers[task_id]
            try:
                watcher.update()
                except_counts[task_id] = 0
                # Keep around metrics for each task we watch
                with watch_task_lock:
                    watch_task_info[task_id] = dict(watcher.info)
                if watcher.is_done():
                    errors[task_id] = None if watcher.is_success() else watcher.get_failure()
                    tasks_to_poll.remove(task_id)
                log_f(f"Task {task_id} state: {koji.TASK_STATES[watcher.info['state']]}")
            except:
                except_counts[task_id] += 1
                # possible for watcher.update() to except during connection issue, try again
                log_f('watcher.update() exception. Trying again in 60s.\n{}'.format(traceback.format_exc()))
                if except_counts[task_id] >= 10:
                    log_f('watcher.update() excepted 10 times. Giving up.')
                    errors[task_id] = traceback.format_exc()
                    tasks_to_cancel.add(task_id)
                    tasks_to_poll.remove(task_id)
        if not tasks_to_poll:
            break
        if terminate_event.wait(timeout=3 * 60):
            for task_id in tasks_to_poll:
                tasks_to_cancel.add(task_id)
                errors[task_id] = 'Interrupted'
            break
        if time.time() > end:
            for task_id in tasks_to_poll:
                tasks_to_cancel.add(task_id)
                errors[task_id] = 'Timeout watching task'
            break
    if tasks_to_cancel:
        log_f(f"{errors}, canceling builds")
        for task_id in tasks_to_cancel:
            log_f(f"Error waiting for Brew task {task_id}: {errors[task_id]}. Canceling...")
            canceled = session.cancelTask(task_id, recurse=True)
            if canceled:
                log_f(f"Brew task {task_id} was canceled.")
            else:
                log_f(f"Brew task {task_id} was NOT canceled.")
    return errors


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


def get_latest_builds(tag_component_tuples: List[Tuple[str, str]], build_type: Optional[str], event: Optional[int], session: koji.ClientSession) -> List[Optional[List[Dict]]]:
    """ Get latest builds for multiple Brew components as of given event

    :param tag_component_tuples: List of (tag, component_name) tuples
    :param build_type: if given, only retrieve specified build type (rpm, image)
    :param event: Brew event ID, or None for now.
    :param session: instance of Brew session
    :return: a list of lists of Koji/Brew build dicts
    """
    tasks = []
    with session.multicall(strict=True) as m:
        for tag, component_name in tag_component_tuples:
            if not tag:
                tasks.append(None)
                continue
            tasks.append(m.getLatestBuilds(tag, event=event, package=component_name, type=build_type))
    return [task.result if task else None for task in tasks]


def get_tagged_builds(tags: List[str], build_type: Optional[str], event: Optional[int], session: koji.ClientSession) -> List[Optional[List[Dict]]]:
    """ Get tagged builds for multiple Brew tags

    :param tag_component_tuples: List of (tag, component_name) tuples
    :param build_type: if given, only retrieve specified build type (rpm, image)
    :param event: Brew event ID, or None for now.
    :param session: instance of Brew session
    :return: a list of lists of Koji/Brew build dicts
    """
    tasks = []
    with session.multicall(strict=True) as m:
        for tag in tags:
            tasks.append(m.listTagged(tag, event=event, type=build_type))
    return [task.result if task else None for task in tasks]


def list_archives_by_builds(build_ids: List[int], build_type: str, session: koji.ClientSession) -> List[Optional[List[Dict]]]:
    """ Retrieve information about archives by builds
    :param build_ids: List of build IDs
    :param build_type: build type, such as "image"
    :param session: instance of Brew session
    :return: a list of Koji/Brew archive lists (augmented with "rpms" entries for RPM lists)
    """
    tasks = []
    with session.multicall(strict=True) as m:
        for build_id in build_ids:
            if not build_id:
                tasks.append(None)
                continue
            tasks.append(m.listArchives(buildID=build_id, type=build_type))
    archives_list = [task.result if task else None for task in tasks]

    # each archives record contains an archive per arch; look up RPMs for each
    archives = [ar for rec in archives_list for ar in rec or []]
    archives_rpms = list_image_rpms([ar["id"] for ar in archives], session)
    for archive, rpms in zip(archives, archives_rpms):
        archive["rpms"] = rpms

    return archives_list


def get_builds_tags(build_nvrs, session=None):
    """Get tags of multiple Koji/Brew builds

    :param builds_nvrs: list of build nvr strings or numbers.
    :param session: instance of :class:`koji.ClientSession`
    :return: a list of Koji/Brew tag lists
    """
    tasks = []
    with session.multicall(strict=True) as m:
        for nvr in build_nvrs:
            tasks.append(m.listTags(build=nvr))
    return [task.result for task in tasks]


def list_image_rpms(image_ids: List[int], session: koji.ClientSession) -> List[Optional[List[Dict]]]:
    """ Retrieve RPMs in given images
    :param image_ids: image IDs list
    :param session: instance of Brew session
    :return: a list of Koji/Brew RPM lists
    """
    tasks = []
    with session.multicall(strict=True) as m:
        tasks = [m.listRPMs(imageID=image_id) for image_id in image_ids]
    return [task.result for task in tasks]


def list_build_rpms(build_ids: List[int], session: koji.ClientSession) -> List[Optional[List[Dict]]]:
    """ Retrieve RPMs in given package builds (not images)
    :param build_ids: list of build IDs
    :param session: instance of Brew session
    :return: a list of Koji/Brew RPM lists
    """
    tasks = []
    with session.multicall(strict=True) as m:
        tasks = [m.listBuildRPMs(build) for build in build_ids]
    return [task.result for task in tasks]


# Map that records tagId -> dict of latest package tagging event associated with that tag
latest_tag_change_events = {}
cache_lock = Lock()


def has_tag_changed_since_build(runtime, koji_client, build, tag, inherit=True) -> Dict:
    """
    :param runtime:  The doozer runtime
    :param koji_client: A koji ClientSession.
    :param build:  A build information dict returned from koji getBuild
    :param tag: A tag name or tag id which should be assessed.
    :param inherit: If True, uses tag inheritance.
    :return: None if the tag has not changed since build OR a change event dict for the build that was tagged
            after the build argument was built.
    """
    build_nvr = build['nvr']
    build_event_id = build['creation_event_id']

    result = []
    with cache_lock:
        latest_tag_change_event = latest_tag_change_events.get(tag, None)

    if latest_tag_change_event is None:
        latest_tag_change_event = {}

        # Note: There is an API 'tagHistory' that can do this, but (1) it is supposed to be
        # deprecated and (2) it does not allow event=# to constrain its search (required
        # to support --brew-event). So we use a combination of listTagged and queryHistory.

        # The listTagged API will do much of this work for us. The reason is that it will report updates to
        # a tag newest->oldest: https://pagure.io/koji/blob/fedf3ee9f9238ed74c34d51c5458a834732b3346/f/hub/kojihub.py#_1351
        # In other words, listTagged('rhaos-4.7-rhel-8-build', latest=True, inherit=True)[0] describes a bulid that was
        # most recently tagged into this tag (or inherited tags).
        last_tagged_builds = koji_client.listTagged(tag, latest=True, inherit=inherit)
        if last_tagged_builds:
            last_tagged_build = last_tagged_builds[0]
            # We now have the build that was tagged, but not WHEN it was tagged. It could have been built
            # a long time ago, and recently tagged into the tag we care about. To figure this out, we
            # need to query the tag_listing table.

            found_in_tag_name = last_tagged_build['tag_name']  # If using inheritance, this can differ from tag
            # Example result of full queryHistory: https://gist.github.com/jupierce/943b845c07defe784522fd9fd76f4ab0
            tag_listing = koji_client.queryHistory(table='tag_listing',
                                                   tag=found_in_tag_name, build=last_tagged_build['build_id'])['tag_listing']
            latest_tag_change_event = tag_listing[0]  # Order is by increasing age, so 0 is the most recent time this build was tagged

        with cache_lock:
            latest_tag_change_events[tag] = latest_tag_change_event

    if latest_tag_change_event and latest_tag_change_event['create_event'] > build_event_id:
        runtime.logger.debug(f'Found that build of {build_nvr} (event={build_event_id}) occurred before tag changes: {latest_tag_change_event}')
        return latest_tag_change_event

    return result


class KojiWrapperOpts(object):
    """
    A structure to carry special options into KojiWrapper API invocations. When using
    a KojiWrapper instance, any koji api call (or multicall) can include a KojiWrapperOpts
    as a positional parameter. It will be interpreted by the KojiWrapper and removed
    prior to sending the request on to the koji server.
    """

    def __init__(self, logger=None, caching=False, brew_event_aware=False, return_metadata=False):
        """
        :param logger: The koji API inputs and outputs will be logged at info level.
        :param caching: The result of the koji api call will be cached. Identical koji api calls (with caching=True)
                        will hit the cache instead of the server.
        :param brew_event_aware: Denotes that the caller is aware that the koji call they are making is NOT
                        constrainable with an event= or beforeEvent= kwarg. The caller should only be making such
                        a call if they know it will not affect the idempotency of the execution of tests. If not
                        specified, non-constrainable koji APIs will cause an exception to be thrown.
        :param return_metadata: If true, the API call will return KojiWrapperMetaReturn instead of the raw result.
                        This is for testing purposes (e.g. to see if caching is working). For multicall work, the
                        metadata wrapper will be returned from call_all()
        """
        self.logger = logger
        self.caching: bool = caching
        self.brew_event_aware: bool = brew_event_aware
        self.return_metadata: bool = return_metadata


class KojiWrapperMetaReturn(object):

    def __init__(self, result, cache_hit=False):
        self.result = result
        self.cache_hit = cache_hit


class KojiWrapper(koji.ClientSession):
    """
    Using KojiWrapper adds the following to the normal ClientSession:
    - Calls are retried if requests.exceptions.ConnectionError is encountered.
    - If the koji api call has a KojiWrapperOpts as a positional parameter:
        - If opts.logger is set, e.g. wrapper.getLastEvent(KojiWrapperOpts(logger=runtime.logger)), the invocation
          and results will be logged (the positional argument will not be passed to the koji server).
        - If opts.cached is True, the result will be cached and an identical invocation (also with caching=True)
          will return the cached value.
    """

    koji_wrapper_lock = threading.Lock()
    koji_call_counter = 0  # Increments atomically to help search logs for koji api calls
    # Used by the KojiWrapper to cache API calls, when a call's args include a KojiWrapperOpts with caching=True.
    # The key is either None or a brew event id to which queries are locked. The value is another dict whose
    # key is a string respresentation of the method to be invoked and the value is the cached value returned
    # from the server.
    koji_wrapper_result_cache = {}

    # A list of methods which support receiving an event kwarg. See --brew-event CLI argument.
    methods_with_event = set([
        'getBuildConfig',
        'getBuildTarget',
        'getBuildTargets',
        'getExternalRepo',
        'getExternalRepoList',
        'getFullInheritance',
        'getGlobalInheritance',
        'getHost',
        'getInheritanceData',
        'getLatestBuilds',
        'getLatestMavenArchives',
        'getLatestRPMS',
        'getPackageConfig',
        'getRepo',
        'getTag',
        'getTagExternalRepos',
        'getTagGroups',
        'listChannels',
        'listExternalRepos',
        'listPackages',
        'listTagged',
        'listTaggedArchives',
        'listTaggedRPMS',
        'newRepo',
    ])

    # Methods which cannot be constrained, but are considered safe to allow even wthen brew-event is set.
    # Why? If you know the parameters, those parameters should have already been constrained by another
    # koji API call.
    safe_methods = set([
        'getEvent',
        'getBuild',
        'listArchives',
        'listRPMs',
    ])

    def __init__(self, koji_session_args, brew_event=None):
        """
        See class description on what this wrapper provides.
        :param koji_session_args: list to pass as *args to koji.ClientSession superclass
        :param brew_event: If specified, all koji queries (that support event=...) will be called with this
                event. This allows you to lock all calls to this client in time. Make sure the method is in
                KojiWrapper.methods_with_event if it is a new koji method (added after 2020-9-22).
        """
        self.___brew_event = None if not brew_event else int(brew_event)
        super(KojiWrapper, self).__init__(*koji_session_args)

    @classmethod
    def clear_global_cache(cls):
        with cls.koji_wrapper_lock:
            cls.koji_wrapper_result_cache.clear()

    @classmethod
    def get_cache_size(cls):
        with cls.koji_wrapper_lock:
            return total_size(cls.koji_wrapper_result_cache)

    @classmethod
    def get_next_call_id(cls):
        global koji_call_counter, koji_wrapper_lock
        with cls.koji_wrapper_lock:
            cid = cls.koji_call_counter
            cls.koji_call_counter = cls.koji_call_counter + 1
            return cid

    def _get_cache_bucket_unsafe(self):
        """Call while holding lock!"""
        cache_bucket = KojiWrapper.koji_wrapper_result_cache.get(self.___brew_event, None)
        if cache_bucket is None:
            cache_bucket = {}
            KojiWrapper.koji_wrapper_result_cache[self.___brew_event] = cache_bucket
        return cache_bucket

    def _cache_result(self, api_repr, result):
        with KojiWrapper.koji_wrapper_lock:
            cache_bucket = self._get_cache_bucket_unsafe()
            cache_bucket[api_repr] = result

    def _get_cache_result(self, api_repr, return_on_miss):
        with KojiWrapper.koji_wrapper_lock:
            cache_bucket = self._get_cache_bucket_unsafe()
            return cache_bucket.get(api_repr, return_on_miss)

    def modify_koji_call_kwargs(self, method_name, kwargs, kw_opts: KojiWrapperOpts):
        """
        For a given koji api method, modify kwargs by inserting an event key if appropriate
        :param method_name: The koji api method name
        :param kwargs: The kwargs about to passed in
        :param kw_opts: The KojiWrapperOpts that can been determined for this invocation.
        :return: The actual kwargs to pass to the superclass
        """
        brew_event = self.___brew_event
        if brew_event:
            if method_name == 'queryHistory':
                if 'beforeEvent' not in kwargs and 'before' not in kwargs:
                    # Only set the kwarg if the caller didn't
                    kwargs = kwargs or {}
                    kwargs['beforeEvent'] = brew_event + 1
            elif method_name in KojiWrapper.methods_with_event:
                if 'event' not in kwargs:
                    # Only set the kwarg if the caller didn't
                    kwargs = kwargs or {}
                    kwargs['event'] = brew_event
            elif method_name in KojiWrapper.safe_methods:
                # Let it go through
                pass
            elif not kw_opts.brew_event_aware:
                # If --brew-event has been specified and non-constrainable API call is invoked, raise
                # an exception if the caller has not made clear that are ok with that via brew_event_aware option.
                raise IOError(f'Non-constrainable koji api call ({method_name}) with --brew-event set; you must use KojiWrapperOpts with brew_event_aware=True')

        return kwargs

    def modify_koji_call_params(self, method_name, params, aggregate_kw_opts: KojiWrapperOpts):
        """
        For a given koji api method, scan a tuple of arguments being passed to that method.
        If a KojiWrapperOpts is detected, interpret it. Return a (possible new) tuple with
        any KojiWrapperOpts removed.
        :param method_name: The koji api name
        :param params: The parameters for the method. In a standalone API call, this will just
                        be normal positional arguments. In a multicall, params will look
                        something like: (1328870, {'__starstar': True, 'strict': True})
        :param aggregate_kw_opts: The KojiWrapperOpts to be populated with KojiWrapperOpts instances found in the parameters.
        :return: The params tuple to pass on to the superclass call
        """
        new_params = list()
        for param in params:
            if isinstance(param, KojiWrapperOpts):
                kwOpts: KojiWrapperOpts = param

                # If a logger is specified, use that logger for the call. Only the most last logger
                # specific in a multicall will be used.
                aggregate_kw_opts.logger = kwOpts.logger or aggregate_kw_opts.logger

                # Within a multicall, if any call requests caching, the entire multiCall will use caching.
                # This may be counterintuitive, but avoids having the caller carefully setting caching
                # correctly for every single call.
                aggregate_kw_opts.caching |= kwOpts.caching

                aggregate_kw_opts.brew_event_aware |= kwOpts.brew_event_aware
                aggregate_kw_opts.return_metadata |= kwOpts.return_metadata
            else:
                new_params.append(param)

        return tuple(new_params)

    def _callMethod(self, name, args, kwargs=None, retry=True):
        """
        This method is invoked by the superclass as part of a normal koji_api.<apiName>(...) OR
        indirectly after koji.multicall() calls are aggregated and executed (this calls
        the 'multiCall' koji API).
        :param name: The name of the koji API.
        :param args:
            - When part of an ordinary invocation: a tuple of args. getBuild(1328870, strict=True) -> args=(1328870,)
            - When part of a multicall, contains methods, args, and kwargs. getBuild(1328870, strict=True) ->
                args=([{'methodName': 'getBuild','params': (1328870, {'__starstar': True, 'strict': True})}],)
        :param kwargs:
            - When part of an ordinary invocation, a map of kwargs. getBuild(1328870, strict=True) -> kwargs={'strict': True}
            - When part of a multicall, contains nothing? with multicall including getBuild(1328870, strict=True) -> {}
        :param retry: passed on to superclass retry
        :return: The value returned from the koji API call.
        """

        aggregate_kw_opts: KojiWrapperOpts = KojiWrapperOpts()

        if name == 'multiCall':
            # If this is a multiCall, we need to search through and modify each bundled invocation
            """
            Example args:
            ([  {'methodName': 'getBuild', 'params': (1328870, {'__starstar': True, 'strict': True})},
                {'methodName': 'getLastEvent', 'params': ()}],)
            """
            multiArg = args[0]   # args is a tuple, the first should be our listing of method invocations.
            for call_dict in multiArg:  # For each method invocation in the multicall
                method_name = call_dict['methodName']
                params = self.modify_koji_call_params(method_name, call_dict['params'], aggregate_kw_opts)
                if params:
                    params = list(params)
                    # Assess whether we need to inject event of beforeEvent into the koji call kwargs
                    possible_kwargs = params[-1]  # last element could be normal arg or kwargs dict
                    if isinstance(possible_kwargs, dict) and possible_kwargs.get('__starstar', None):
                        # __starstar is a special identifier added by the koji library indicating
                        # the entry is kwargs and not normal args.
                        params[-1] = self.modify_koji_call_kwargs(method_name, possible_kwargs, aggregate_kw_opts)
                call_dict['params'] = tuple(params)
        else:
            args = self.modify_koji_call_params(name, args, aggregate_kw_opts)
            kwargs = self.modify_koji_call_kwargs(name, kwargs, aggregate_kw_opts)

        my_id = KojiWrapper.get_next_call_id()

        logger = aggregate_kw_opts.logger
        return_metadata = aggregate_kw_opts.return_metadata
        use_caching = aggregate_kw_opts.caching

        retries = 4
        while retries > 0:
            try:
                if logger:
                    logger.info(f'koji-api-call-{my_id}: {name}(args={args}, kwargs={kwargs})')

                def package_result(result, cache_hit: bool):
                    ret = result
                    if return_metadata:
                        # If KojiWrapperOpts asked for information about call metadata back,
                        # return the results in a wrapper containing that information.
                        if name == 'multiCall':
                            # Results are going to be returned as [ [result1], [result2], ... ] if there is no fault.
                            # If there is a fault, the fault entry will be a dict.
                            ret = []
                            for entry in result:
                                # A fault was entry will not carry metadata, so only package when we see a list
                                if isinstance(entry, list):
                                    ret.append([KojiWrapperMetaReturn(entry[0], cache_hit=cache_hit)])
                                else:
                                    # Pass on fault without modification.
                                    ret.append(entry)
                        else:
                            ret = KojiWrapperMetaReturn(result, cache_hit=cache_hit)
                    return ret

                caching_key = None
                if use_caching:
                    # We need a reproducible immutable key from a dict with nest dicts. json.dumps
                    # and sorting keys is a deterministic way of achieving this.
                    caching_key = json.dumps({
                        'method_name': name,
                        'args': args,
                        'kwargs': kwargs
                    }, sort_keys=True)
                    result = self._get_cache_result(caching_key, Missing)
                    if result is not Missing:
                        if logger:
                            logger.info(f'CACHE HIT: koji-api-call-{my_id}: {name} returned={result}')
                        return package_result(result, True)

                result = super()._callMethod(name, args, kwargs=kwargs, retry=retry)

                if use_caching:
                    self._cache_result(caching_key, result)

                if logger:
                    logger.info(f'koji-api-call-{my_id}: {name} returned={result}')

                return package_result(result, False)
            except requests.exceptions.ConnectionError as ce:
                if logger:
                    logger.warning(f'koji-api-call-{my_id}: {method_name}(...) failed="{ce}""; retries remaining {retries - 1}')
                time.sleep(5)
                retries -= 1
                if retries == 0:
                    raise
