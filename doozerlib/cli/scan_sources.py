import click
import yaml

from doozerlib import brew
from doozerlib.cli import cli, pass_runtime
from doozerlib.exceptions import DoozerFatalError


@cli.command("config:scan-sources", short_help="Determine if any rpms / images need to be rebuilt.")
@click.option("--yaml", "as_yaml", default=False, is_flag=True, help='Print results in a yaml block')
@pass_runtime
def config_scan_source_changes(runtime, as_yaml):
    """
    Determine if any rpms / images need to be rebuilt.

    \b
    The method will report RPMs in this group if:
    - Their source git hash no longer matches their upstream source.
    - The buildroot used by the previous RPM build has changed.

    \b
    It will report images if the latest build:
    - Contains an RPM that is about to be rebuilt based on the RPM check above.
    - If the source git hash no longer matches the upstream source.
    - Contains any RPM (from anywhere in Red Hat) which has likely changed since the image was built.
        - This indirectly detects non-member parent image changes.
    - Was built with a buildroot that has now changed (probably not useful for images, but was cheap to add).
    - Used a builder image (from anywhere in Red Hat) that has changed.
    - Used a builder image from this group that is about to change.
    - If the associated member is a descendant of any image that needs change.
    """
    runtime.initialize(mode='both', clone_distgits=True)

    all_rpm_metas = set(runtime.rpm_metas())
    all_image_metas = set(runtime.image_metas())

    changing_rpm_metas = set()
    changing_image_metas = set()
    changing_rpm_packages = set()
    assessment_reason = dict()  # maps metadata qualified_key => message describing change

    def add_assessment_reason(meta, changing, reason):
        # qualify by whether this is a True or False for change so that we can store both in the map.
        key = f'{meta.qualified_key}+{changing}'
        # If the key is already there, don't replace the message as it is likely more interesting
        # than subsequent reasons (e.g. changing because of ancestry)
        if key not in assessment_reason:
            assessment_reason[key] = reason

    with runtime.shared_koji_client_session() as koji_api:
        runtime.logger.info(f'Running scan with true latest koji/brew event: {koji_api.getLastEvent()}')

        # Different branches have different build tags & inheritances; cache the results to
        # limit brew queries.
        build_root_tag_id_cache = dict()

        def get_build_root_inherited_tags(config_meta):
            # Create a {map of tags => tag names} which contribute RPMs to a given build root
            build_tag = config_meta.branch() + '-build'
            if build_tag in build_root_tag_id_cache:
                return build_root_tag_id_cache[build_tag]
            build_tag_ids = [n['parent_id'] for n in koji_api.getFullInheritance(build_tag)]
            final_build_tag_ids = {}
            for bid in build_tag_ids:
                tag_name = koji_api.getTag(bid)['name']
                if tag_name.endswith(('-candidate', '-pending')):
                    # We don't want our candidate tag to be taken in consideration. It will always
                    # be changing as after any build.
                    continue

                # There are many tags in the buildroots that are unlikely to affect our builds.
                # (e.g. rhel-7.8-z). Filter these down to those which reasonably affect us.
                if any(('devtools' in tag_name,  # e.g. devtools-2019.4-rhel-7
                        '-build' in tag_name,
                        '-override' in tag_name)):
                    final_build_tag_ids[bid] = tag_name

            runtime.logger.info(f'For {config_meta.distgit_key}, found buildroot inheritance: {build_tag_ids}; Filtered down to {final_build_tag_ids}')
            build_root_tag_id_cache[build_tag] = final_build_tag_ids
            return final_build_tag_ids

        # First, scan for any upstream source code changes. If found, these are guaranteed rebuilds.
        for meta, change_info in runtime.scan_distgit_sources():
            needs_rebuild, reason = change_info
            dgk = meta.distgit_key
            if not (meta.enabled or meta.mode == "disabled" and runtime.load_disabled):
                # An enabled image's dependents are always loaded. Ignore disabled configs unless explicitly indicated
                continue

            if meta.meta_type == 'rpm':
                package_name = meta.get_package_name(default=None)
                if needs_rebuild is False and package_name:  # If we are currently matching, check buildroots to see if it unmatches us
                    for latest_rpm_build in koji_api.getLatestRPMS(tag=meta.branch() + '-candidate', package=package_name)[1]:
                        # Detect if our buildroot changed since the last build of this rpm
                        rpm_build_root_tag_ids = get_build_root_inherited_tags(meta).keys()
                        build_root_changes = brew.tags_changed_since_build(runtime, koji_api, latest_rpm_build, rpm_build_root_tag_ids)
                        if build_root_changes:
                            changing_tag_names = [brc['tag_name'] for brc in build_root_changes]
                            reason = f'Latest rpm was built before buildroot changes: {changing_tag_names}'
                            runtime.logger.info(f'{dgk} ({latest_rpm_build}) will be rebuilt because it has not been built since a buildroot change: {build_root_changes}')
                            needs_rebuild = True

                if reason:
                    add_assessment_reason(meta, needs_rebuild, reason=reason)

                if needs_rebuild:
                    changing_rpm_metas.add(meta)
                    if package_name:
                        changing_rpm_packages.add(package_name)
                    else:
                        runtime.logger.warning(f"Appears that {dgk} as never been built before; can't determine package name")
            elif meta.meta_type == 'image':
                if reason:
                    add_assessment_reason(meta, needs_rebuild, reason=reason)
                if needs_rebuild:
                    changing_image_metas.add(meta)
            else:
                raise IOError(f'Unsupported meta type: {meta.meta_type}')

        def add_image_meta_change(meta, msg):
            nonlocal changing_image_metas
            changing_image_metas.add(meta)
            add_assessment_reason(meta, True, msg)
            for descendant_meta in meta.get_descendants():
                changing_image_metas.add(descendant_meta)
                add_assessment_reason(descendant_meta, True, f'Ancestor {meta.distgit_key} is changing')

        # To limit the size of the queries we are going to make, find the oldest image
        eldest_image_event_ts = koji_api.getLastEvent()['ts']
        for image_meta in runtime.image_metas():
            info = image_meta.get_latest_build(default=None)
            if info is not None:
                create_event_ts = koji_api.getEvent(info['creation_event_id'])['ts']
                if create_event_ts < eldest_image_event_ts:
                    eldest_image_event_ts = create_event_ts

        for image_meta in runtime.image_metas():
            image_change, msg = image_meta.does_image_need_change(eldest_image_event_ts, changing_rpm_packages, get_build_root_inherited_tags(image_meta).keys())
            if image_change:
                add_image_meta_change(image_meta, msg)

        # does_image_name_change() checks whether its non-member builder images have changed
        # but cannot determine whether member builder images have changed until anticipated
        # changes have been calculated. The following loop does this. It uses while True,
        # because technically, we could keep finding changes in intermediate builder images
        # and have to ensure we pull in images that rely on them in the next iteration.
        # fyi, changes in direct parent images should be detected by RPM changes, which
        # does does_image_name_change will detect.
        while True:
            changing_image_dgks = [meta.distgit_key for meta in changing_image_metas]
            for image_meta in all_image_metas:
                dgk = image_meta.distgit_key
                if dgk in changing_image_dgks:  # Already in? Don't look any further
                    continue

                for builder in image_meta.config['from'].builder:
                    if builder.member and builder.member in changing_image_dgks:
                        runtime.logger.info(f'{dgk} will be rebuilt due to change in builder member ')
                        add_image_meta_change(image_meta, f'Builder group member has changed: {builder.member}')

            if len(changing_image_metas) == len(changing_image_dgks):
                # The for loop didn't find anything new, we can break
                break

        # We have our information. Now build the output report..
        image_results = []
        changing_image_dgks = [meta.distgit_key for meta in changing_image_metas]
        for image_meta in all_image_metas:
            dgk = image_meta.distgit_key
            is_changing = dgk in changing_image_dgks
            image_results.append({
                'name': dgk,
                'changed': is_changing,
                'reason': assessment_reason.get(f'{image_meta.qualified_key}+{is_changing}', 'No change detected'),
            })

        rpm_results = []
        changing_rpm_dgks = [meta.distgit_key for meta in changing_rpm_metas]
        for rpm_meta in all_rpm_metas:
            dgk = rpm_meta.distgit_key
            is_changing = dgk in changing_rpm_dgks
            rpm_results.append({
                'name': dgk,
                'changed': is_changing,
                'reason': assessment_reason.get(f'{rpm_meta.qualified_key}+{is_changing}', 'No change detected'),
            })

        results = dict(
            rpms=rpm_results,
            images=image_results
        )

        if as_yaml:
            click.echo('---')
            click.echo(yaml.safe_dump(results, indent=4))
            return

        for kind, items in results.items():
            if not items:
                continue
            click.echo(kind.upper() + ":")
            for item in items:
                click.echo('  {} is {} (reason: {})'.format(item['name'],
                                                            'changed' if item['changed'] else 'the same',
                                                            item['reason']))


cli.add_command(config_scan_source_changes)
