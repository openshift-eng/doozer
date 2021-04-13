import asyncio
import os
import sys
from functools import update_wrapper

import click

from doozerlib import dotconfig, version
from doozerlib.cli import cli_opts
from doozerlib.runtime import Runtime
from doozerlib.util import yellow_print

CTX_GLOBAL = None
pass_runtime = click.make_pass_decorator(Runtime)
context_settings = dict(help_option_names=['-h', '--help'])
VERSION_QUOTE = """
The Doozers don't mind their buildings being eaten;
if the Fraggles didn't eat the constructions,
the Doozers would run out of building space,
and if they ran out of building space,
they would have to move away from Fraggle Rock
or else they would die.
"""


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo('Doozer v{}'.format(version()))
    click.echo('Python v{}'.format(sys.version))
    click.echo(VERSION_QUOTE)
    ctx.exit()


# ============================================================================
# GLOBAL OPTIONS: parameters for all commands
# ============================================================================
@click.group(context_settings=context_settings)
@click.option('--version', is_flag=True, callback=print_version,
              expose_value=False, is_eager=True)
@click.option("--data-path", metavar='PATH', default=None,
              help="Git repo or directory containing groups metadata")
@click.option("--working-dir", metavar='PATH', default=None,
              help="Existing directory in which file operations should be performed.\n Env var: DOOZER_WORKING_DIR")
@click.option("--upcycle", default=False, is_flag=True,
              help="Reclaims working directory; deletes doozer metadata and updates git repos to latest upstream content.")
@click.option("--registry-config-dir", metavar='PATH', default=None,
              help="Directory containing docker config.json authentication; defaults to DOCKER_CONFIG env var if set, or `~/.docker/` if not.\n Env var: DOCKER_CONFIG")
@click.option("--user", metavar='USERNAME', default=None,
              help="Username for rhpkg. Env var: DOOZER_USER")
@click.option("-g", "--group", default=None, metavar='NAME[@commitish]',
              help="The group of images on which to operate. Env var: DOOZER_GROUP")
@click.option("--branch", default=None, metavar='BRANCH',
              help="DistGit to override any default in group.yml.")
@click.option('--stage', default=False, is_flag=True, help='Force checkout stage branch for sources in group.yml.')
@click.option("-i", "--images", default=[], metavar='NAME', multiple=True,
              help="Name of group image member to include in operation (all by default). Can be comma delimited list.")
@click.option("-r", "--rpms", default=[], metavar='NAME', multiple=True,
              help="Name of group rpm member to include in operation (all by default). Can be comma delimited list.")
@click.option("-a", "--arches", default=[], metavar='ARCH', multiple=True,
              help="CPU arches to operate on (group.yaml provides default). Can be comma delimited list.")
@click.option('--load-wip', default=False, is_flag=True, help='Load WIP RPMs/Images in addition to those specified, if any')
@click.option("-x", "--exclude", default=[], metavar='NAME', multiple=True,
              help="Name of group image or rpm member to exclude in operation (none by default). Can be comma delimited list.")
@click.option('--ignore-missing-base', default=False, is_flag=True,
              help='If a base image is not included, proceed and do not update FROM.')
@click.option('--latest-parent-version', default=False, is_flag=True,
              help='If a base image is not included, lookup latest FROM tag for parent. Implies --ignore-missing-base')
@click.option("--quiet", "-q", default=False, is_flag=True, help="Suppress non-critical output")
@click.option('--debug', default=False, is_flag=True, help='Show debug output on console.')
@click.option("--stream", metavar="STREAM_NAME PULLSPEC", nargs=2, multiple=True,
              help="Override a stream.yml entry with a specific pullspec.  [multiple]")
@click.option("--lock-upstream", "upstreams", metavar="DISTGIT_KEY COMMIT-ISH", multiple=True, nargs=2,
              help="Override upstream source commits. [multiple]")
@click.option("--lock-downstream", "downstreams", metavar="DISTGIT_KEY COMMIT-ISH", multiple=True, nargs=2,
              help="Checkout non-HEAD of distgit. This is primarily for testing and cannot be used to build images. [multiple]")
@click.option("--lock-runtime-uuid", metavar="UUID", default=None, nargs=1,
              help="Fixes the otherwise randomly determined run UUI")
@click.option("--source", metavar="ALIAS PATH", nargs=2, multiple=True,
              help="Associate a path with a given source alias.  [multiple]")
@click.option("--sources", metavar="YAML_PATH",
              help="YAML dict associating sources with their alias. Same as using --source multiple times.")
@click.option('--odcs-mode', default=False, is_flag=True,
              help='Process Dockerfiles in ODCS mode. HACK for the time being.')
@click.option('--load-disabled', default=False, is_flag=True,
              help='Treat disabled images/rpms as if they were enabled')
@click.option('--local/--osbs', default=False, is_flag=True, help='--local to run in local-only mode, --osbs to run on build cluster (default)')
@click.option("--rhpkg-config", metavar="RHPKG_CONFIG",
              help="Path to rhpkg config file to use instead of system default")
@click.option("--cache-dir", metavar="DIR", required=False, default=None,
              help="A directory in which reference git repos can be stored for caching purposes")
@click.option("--datastore", metavar="ENV", required=False, default=None,
              help="Whether to store & retrieve data in int / stage / prod database environment")
@click.option("--profile", metavar="NAME", default="", help="Name of build profile")
@click.option("--brew-event", metavar='EVENT', default=None,
              help="Lock koji clients from runtime to this brew event.")
@click.pass_context
def cli(ctx, **kwargs):
    global CTX_GLOBAL
    kwargs['global_opts'] = None  # can only be set in settings.yaml, add manually

    # This section mostly for containerizing doozer
    # It allows the user to simply place settings.yaml into their working dir
    # and then mount that working dir into the container.
    # The container automatically sets DOOZER_WORKING_DIR
    # Having settings.yaml in the user directory would overcomplicate this
    # Note: This means that having working_dir in that config would override everything
    wd = None
    wd_env = cli_opts.CLI_OPTS['working_dir']['env']
    config_path_override = None

    # regardless of the container using the ENV var, always respect
    # --working-dir above all else
    if kwargs['working_dir']:
        wd = kwargs['working_dir']
    elif wd_env in os.environ:
        wd = os.environ[wd_env]

    # only if settings.yaml exists in the workspace force dotconfig
    # to override the usual flow. Otherwise this will fall back to
    # potentially getting working-dir from ~/.config/doozer/settings.yaml
    if wd and os.path.isfile(os.path.join(wd, 'settings.yaml')):
        config_path_override = wd

    cfg = dotconfig.Config('doozer', 'settings',
                           template=cli_opts.CLI_CONFIG_TEMPLATE,
                           envvars=cli_opts.CLI_ENV_VARS,
                           cli_args=kwargs,
                           path_override=config_path_override)

    if cli_opts.config_is_empty(cfg.full_path):
        msg = (
            "It appears you may be using Doozer for the first time.\n"
            "Be sure to setup Doozer using the user config file:\n"
            "{}\n"
        ).format(cfg.full_path)
        yellow_print(msg)

    # set global option defaults
    runtime_args = cfg.to_dict()
    global_opts = runtime_args['global_opts']
    if global_opts is None:
        global_opts = {}
    for k, v in cli_opts.GLOBAL_OPT_DEFAULTS.items():
        if k not in global_opts or global_opts[k] is None:
            global_opts[k] = v
    runtime_args['global_opts'] = global_opts

    ctx.obj = Runtime(cfg_obj=cfg, command=ctx.invoked_subcommand, **runtime_args)
    CTX_GLOBAL = ctx
    return ctx


def click_coroutine(f):
    """ A wrapper to allow to use asyncio with click.
    https://github.com/pallets/click/issues/85
    """
    f = asyncio.coroutine(f)

    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwargs))
    return update_wrapper(wrapper, f)


def validate_semver_major_minor_patch(ctx, param, version):
    """
    For non-None, non-auto values, ensures that the incoming parameter meets the criteria vX.Y.Z or X.Y.Z.
    If minor or patch is not supplied, the value is modified to possess
    minor.major to meet semver requirements.
    :param ctx: Click context
    :param param: The parameter specified on the command line
    :param version: The version specified on the command line
    :return:
    """
    if version == 'auto' or version is None:
        return version

    vsplit = version.split(".")
    try:
        int(vsplit[0].lstrip('v'))
        minor_version = int('0' if len(vsplit) < 2 else vsplit[1])
        patch_version = int('0' if len(vsplit) < 3 else vsplit[2])
    except ValueError:
        raise click.BadParameter('Expected integers in version fields')

    if len(vsplit) > 3:
        raise click.BadParameter('Expected X, X.Y, or X.Y.Z (with optional "v" prefix)')

    return f'{vsplit[0]}.{minor_version}.{patch_version}'
