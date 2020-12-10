from pathlib import Path

import click
import yaml

from doozerlib import brew, exectools, rhcos
from doozerlib.cli import cli, pass_runtime
from doozerlib.cli import release_gen_payload as rgp
from doozerlib.exceptions import DoozerFatalError


@cli.command("config:scan-sources", short_help="Determine if any rpms / images need to be rebuilt.")
@click.option("--ci-kubeconfig", metavar='KC_PATH', required=False,
              help="File containing kubeconfig for looking at release-controller imagestreams")
@click.option("--yaml", "as_yaml", default=False, is_flag=True, help='Print results in a yaml block')
@pass_runtime
def config_scan_source_changes(runtime, ci_kubeconfig, as_yaml):
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

    \b
    It will report machine-os-content updates available per imagestream.
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

    def add_image_meta_change(meta, msg):
        nonlocal changing_image_metas
        changing_image_metas.add(meta)
        add_assessment_reason(meta, True, msg)
        for descendant_meta in meta.get_descendants():
            changing_image_metas.add(descendant_meta)
            add_assessment_reason(descendant_meta, True, f'Ancestor {meta.distgit_key} is changing')

    with runtime.shared_koji_client_session() as koji_api:
        runtime.logger.info(f'scan-sources coordinate: brew_event: {koji_api.getLastEvent(brew.KojiWrapperOpts(brew_event_aware=True))}')
        runtime.logger.info(f'scan-sources coordinate: emulated_brew_event: {runtime.brew_event}')

        # First, scan for any upstream source code changes. If found, these are guaranteed rebuilds.
        for meta, change_info in runtime.scan_distgit_sources():
            needs_rebuild, reason = change_info
            dgk = meta.distgit_key
            if not (meta.enabled or meta.mode == "disabled" and runtime.load_disabled):
                # An enabled image's dependents are always loaded. Ignore disabled configs unless explicitly indicated
                continue

            if meta.meta_type == 'rpm':
                package_name = meta.get_package_name(default=None)
                if needs_rebuild is False and package_name:  # If no change has been detected, check buildroots to see if it has changed

                    # A package may contain multiple RPMs; find the oldest one in the latest package build.
                    eldest_rpm_build = None
                    for latest_rpm_build in koji_api.getLatestRPMS(tag=meta.branch() + '-candidate', package=package_name)[1]:
                        if not eldest_rpm_build or latest_rpm_build['creation_event_id'] < eldest_rpm_build['creation_event_id']:
                            eldest_rpm_build = latest_rpm_build

                    # Detect if our buildroot changed since the oldest rpm of the latest build of the package was built.
                    build_root_change = brew.has_tag_changed_since_build(runtime, koji_api, eldest_rpm_build, meta.build_root_tag(), inherit=True)

                    if build_root_change:
                        reason = 'Oldest package rpm build was before buildroot change'
                        runtime.logger.info(f'{dgk} ({eldest_rpm_build}) in {package_name} is older than more recent buildroot change: {build_root_change}')
                        needs_rebuild = True

                if reason:
                    add_assessment_reason(meta, needs_rebuild, reason=reason)

                if needs_rebuild:
                    changing_rpm_metas.add(meta)
                    if package_name:
                        changing_rpm_packages.add(package_name)
                    else:
                        runtime.logger.warning(f"Appears that {dgk} has never been built before; can't determine package name")
            elif meta.meta_type == 'image':
                if not needs_rebuild:  # If no change has been detected, check configurations like image meta, repos, and streams to see if they have changed
                    # We detect config changes by comparing their digest changes.
                    # The config digest of the previous build is stored at .oit/config_digest on distgit repo.
                    dgr = meta.distgit_repo()
                    path = Path(dgr.distgit_dir).joinpath(".oit", "config_digest")
                    if not path.exists():  # alway request a buiild if config_digest hasn't been stored
                        pass  # TODO: uncomment the following after a week of natural rebuilds
                        # runtime.logger.info('%s config_digest does not exist; request a build', dgk)
                        # needs_rebuild = True
                        # reason = '.oit/config_digest does not exist'
                    else:
                        with path.open('r') as f:
                            prev_digest = f.read()
                        current_digest = meta.calculate_config_digest(runtime.group_config, runtime.streams)
                        if current_digest.strip() != prev_digest.strip():
                            runtime.logger.info('%s config_digest %s is differing from %s', dgk, prev_digest, current_digest)
                            needs_rebuild = True
                            reason = 'Config changed'
                if needs_rebuild:
                    add_image_meta_change(meta, reason)
            else:
                raise IOError(f'Unsupported meta type: {meta.meta_type}')

        # To limit the size of the queries we are going to make, find the oldest and newest image
        oldest_image_event_ts = None
        newest_image_event_ts = 0
        for image_meta in runtime.image_metas():
            info = image_meta.get_latest_build(default=None)
            if info is not None:
                create_event_ts = koji_api.getEvent(info['creation_event_id'])['ts']
                if oldest_image_event_ts is None or create_event_ts < oldest_image_event_ts:
                    oldest_image_event_ts = create_event_ts
                if create_event_ts > newest_image_event_ts:
                    newest_image_event_ts = create_event_ts

    runtime.logger.debug(f'Will be assessing tagging changes between newest_image_event_ts:{newest_image_event_ts} and oldest_image_event_ts:{oldest_image_event_ts}')
    change_results = runtime.parallel_exec(
        f=lambda image_meta, terminate_event: image_meta.does_image_need_change(changing_rpm_packages, image_meta.build_root_tag(), newest_image_event_ts, oldest_image_event_ts),
        args=runtime.image_metas(),
        n_threads=20
    )

    for change_result in change_results.get():
        meta, changing, msg = change_result
        if changing:
            add_image_meta_change(meta, msg)

    # does_image_need_change() checks whether its non-member builder images have changed
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

    runtime.logger.debug(f'scan-sources coordinate: results:\n{yaml.safe_dump(results, indent=4)}')

    if ci_kubeconfig:  # we can determine m-os-c needs updating if we can look at imagestreams
        results['rhcos'] = _detect_rhcos_status(runtime, ci_kubeconfig)

    if as_yaml:
        click.echo('---')
        click.echo(yaml.safe_dump(results, indent=4))
    else:
        for kind, items in results.items():
            if not items:
                continue
            click.echo(kind.upper() + ":")
            for item in items:
                click.echo('  {} is {} (reason: {})'.format(item['name'],
                                                            'changed' if item['changed'] else 'the same',
                                                            item['reason']))

    runtime.logger.info(f'KojiWrapper cache size: {int(brew.KojiWrapper.get_cache_size() / 1024)}KB')


def _detect_rhcos_status(runtime, kubeconfig) -> list:
    """
    gather the existing machine-os-content tags and compare them to latest rhcos builds
    @return a list of status entries like:
        {
            'name': "4.2-x86_64-priv",
            'changed': False,
            'reason': "could not find an RHCOS build to sync",
        }
    """
    statuses = []

    version = runtime.get_minor_version()
    for arch in runtime.arches:
        for private in (False, True):
            name = f"{version}-{arch}{'-priv' if private else ''}"
            try:
                tagged_mosc_id = _tagged_mosc_id(kubeconfig, version, arch, private)
                latest_rhcos_id = _latest_rhcos_build_id(version, arch, private)
                status = dict(name=name)
                if not latest_rhcos_id:
                    status['changed'] = False
                    status['reason'] = "could not find an RHCOS build to sync"
                elif tagged_mosc_id == latest_rhcos_id:
                    status['changed'] = False
                    status['reason'] = f"latest RHCOS build is still {latest_rhcos_id} -- no change from istag"
                else:
                    status['changed'] = True
                    status['reason'] = f"latest RHCOS build is {latest_rhcos_id} which differs from istag {tagged_mosc_id}"
                statuses.append(status)
            except Exception as ex:
                # don't let flakiness in rhcos lookups prevent us from scanning regular builds;
                # if anything else changed it will sync anyway.
                runtime.logger.warning(f"could not determine RHCOS status for {name}: {ex}")

    return statuses


def _tagged_mosc_id(kubeconfig, version, arch, private) -> str:
    """determine what the most recently tagged machine-os-content is in given imagestream"""
    base_name = rgp.default_is_base_name(version)
    base_namespace = rgp.default_is_base_namespace()
    name, namespace = rgp.is_name_and_space(base_name, base_namespace, arch, private)
    stdout, _ = exectools.cmd_assert(
        f"oc --kubeconfig '{kubeconfig}' --namespace '{namespace}' get istag '{name}:machine-os-content'"
        " --template '{{.image.dockerImageMetadata.Config.Labels.version}}'",
        retries=3,
        pollrate=5,
        strip=True,
    )
    return stdout if stdout else None


def _latest_rhcos_build_id(version, arch, private) -> str:
    """wrapper to return None if anything goes wrong, which will be taken as no change"""
    try:
        return rhcos.latest_rhcos_build_id(version, arch, private)
    except Exception:
        return None


cli.add_command(config_scan_source_changes)
