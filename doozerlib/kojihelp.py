from __future__ import absolute_import, print_function, unicode_literals
from multiprocessing import Lock

# Map that records the most recent change event_id for a tag.
# tag_id => brew_event_id(an int)
latest_tag_change_cache = {}
cache_lock = Lock()


def tags_changed_since_build(runtime, koji_client, build, tag_ids):
    """
    :param build:  A build information dict returned from koji getBuild
    :param tag_ids: The tag id (or tag name) which should be assessed
    :return: If any of the tags have changed since the specified event, returns a
                list of information about the tags. If no tags have changed since
                event, returns and empty list
    """
    build_nvr = build['nvr']
    build_event_id = build['creation_event_id']

    result = []
    for tid in tag_ids:
        tag_change_event_id = latest_tag_change_cache.get(tid, None)
        if not tag_change_event_id:
            # koji returns in reverse chronological order. So we are retrieving the most recent.
            tag_changes = koji_client.tagHistory(tag=tid, queryOpts={'limit': 1})
            if tag_changes:
                tag_change = tag_changes[0]
                tag_change_event_id = tag_change['create_event']
                if tag_change_event_id > build_event_id:
                    result.append(tag_change)

    runtime.logger.debug(f'Found that build of {build_nvr} (event={build_event_id}) occurred before tag changes: {result}')
    return result

