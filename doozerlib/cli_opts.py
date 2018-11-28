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
    }
}

CLI_ENV_VARS = {k: v['env'] for (k, v) in CLI_OPTS.iteritems()}

CLI_CONFIG_TEMPLATE = '\n'.join(['#{}\n{}:\n'.format(v['help'], k) for (k, v) in CLI_OPTS.iteritems()])


def config_is_empty(path):
    with open(path, 'r') as f:
        cfg = f.read()
        return (cfg == CLI_CONFIG_TEMPLATE)
