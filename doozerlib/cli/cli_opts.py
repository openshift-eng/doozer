from __future__ import absolute_import, print_function, unicode_literals
import yaml
import io

GLOBAL_OPT_DEFAULTS = {
    'distgit_threads': 20,
    'rhpkg_clone_timeout': 900,
    'rhpkg_push_timeout': 1200,

    # 0 for unlimited. At present, distgit doesn't support shallow push. So leave this unlimited for now.
    'rhpkg_clone_depth': 0,
}


def global_opt_default_string():
    res = '\n'
    for k in GLOBAL_OPT_DEFAULTS.keys():
        res += '  {}:\n'.format(k)
    return res


CLI_OPTS = {
    'data_path': {
        'env': 'DOOZER_DATA_PATH',
        'help': 'Git URL or File Path to build data',
    },
    'group': {
        'env': 'DOOZER_GROUP',
        'help': 'Sub-group directory or branch to pull build data'
    },
    'working_dir': {
        'env': 'DOOZER_WORKING_DIR',
        'help': 'Persistent working directory to use'
    },
    'user': {
        'env': 'DOOZER_USER',
        'help': 'Username for running rhpkg / brew / tito'
    },
    'global_opts': {
        'help': 'Global option overrides that can only be set from settings.yaml',
        'default': global_opt_default_string()
    },
    'rhpkg_config': {
        'env': 'DOOZER_RHPKG_CONFIG',
        'help': 'Config file path for rhpkg calls'
    },
    'registry_config_dir': {
        'env': 'DOCKER_CONFIG',
        'help': 'Config directory for the image registry'
    }
}


CLI_ENV_VARS = {k: v['env'] for (k, v) in CLI_OPTS.items() if 'env' in v}

CLI_CONFIG_TEMPLATE = '\n'.join(['#{}\n{}:{}\n'.format(v['help'], k, v['default'] if 'default' in v else '') for (k, v) in CLI_OPTS.items()])


def config_is_empty(path):
    with io.open(path, 'r', encoding="utf-8") as f:
        cfg = f.read()
        return (cfg == CLI_CONFIG_TEMPLATE)
