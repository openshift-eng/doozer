import glob
import logging
import os
import re
import shutil
import sys
import yaml

from dockerfile_parse import DockerfileParser
from doozerlib import exectools, pushd


def setup_logger():
    """Set log level to DEBUG & add a handler to print it on STDOUT

    :return: A logging.RootLogger instance
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s %(levelname)s %(message)s'))

    logger.addHandler(handler)

    return logger


def log(func):
    """Logging decorator, wraps a decorated function and log the arguments passed
    to the call and the function return value

    :param function func: Function to be decorated
    :return: Return mixed, whatever is returned by the decorated function
    """
    def wrap(*args, **kwargs):
        logger.debug('running: {}, with args {} {}'.format(func.__name__, args, kwargs))
        return_val = func(*args, **kwargs)
        logger.debug('{} returned {}'.format(func.__name__, return_val))
        return return_val
    return wrap


# @TODO: keeping global state looks awful, convert this module to a class
working_dir = ''
rhpkg_user = ''
logger = setup_logger()


@log
def update_metadata_repo(nvr, operator_branch='rhaos-4.2-rhel-7', metadata_branch='dev'):
    """Update the corresponding metadata repository of an operator

    :param string nvr: The operator component NVR (i.e.: my-operator-container-v1.2.3-201901010000)
    :param string operator_branch: Which branch of the operator repository should be used
    :param string metadata_branch: Which branch of the metadata repository should be updated
    :return: A string with the name of the updated metadata repository
    """
    component, tag = split_nvr(nvr)
    operator = component.replace('-container', '')
    metadata = '{}-metadata'.format(operator)
    channel = get_channel_from_tag(tag)

    exectools.cmd_assert('mkdir -p {}'.format(working_dir))
    # @TODO: reuse repo if already present (doozer does that somewhere else)
    try:
        shutil.rmtree(os.path.join(working_dir, operator))
        shutil.rmtree(os.path.join(working_dir, metadata))
    except:
        pass

    # @TODO: operator_branch is hardcoded, should we ask it to the user / infer from NVR?
    clone_repo(operator, operator_branch)
    clone_repo(metadata, metadata_branch)

    commit_hash = retrieve_commit_hash_from_brew(nvr)
    checkout_repo(operator, commit_hash)

    update_metadata_manifests_dir(operator, metadata, channel)
    merge_streams_on_top_level_package_yaml(metadata, channel)
    create_symlink_to_latest_csv(metadata)
    create_metadata_dockerfile(operator, metadata)

    commit_and_push_repo(metadata)
    return metadata


@log
def build_metadata_container(repo, target):
    """Build the metadata container using rhpkg

    :param string repo: Name of the metadata repository to be built
    :param string target: Container build target
    """
    with pushd.Dir(os.path.join(working_dir, repo)):
        exectools.cmd_assert('rhpkg --user {} container-build --target {}'.format(rhpkg_user, target))


@log
def split_nvr(nvr):
    """Extract the component name and a build tag from an NVR

    :param string nvr: The component NVR (i.e.: my-operator-container-v1.2.3-201901010000)
    :return: A tuple with the operator component name and a build tag
    """
    name, version, release = nvr.rsplit('-', 2)
    tag = "{}-{}".format(version, release)
    return name, tag


@log
def clone_repo(name, branch):
    """Clone a repository using rhpkg

    :param string name: Name of the repository to be cloned
    :param string branch: Which branch of the repository should be cloned
    """
    with pushd.Dir(working_dir):
        exectools.cmd_assert('rhpkg --user {} clone containers/{} --branch {}'.format(rhpkg_user, name, branch))


@log
def retrieve_commit_hash_from_brew(nvr):
    """Obtain the commit hash of a build (from brew buildinfo)

    :param string nvr: The component NVR (i.e.: my-operator-container-v1.2.3-201901010000)
    :return: A string with the commit hash from which the build was based
    """
    _rc, stdout, _stderr = exectools.cmd_gather('brew buildinfo {}'.format(nvr))
    return re.search(r'Source:[^#]+#(\w+)', stdout).group(1)


@log
def checkout_repo(repo, commit_hash):
    """Checkout a repository to a particular commit hash

    :param string repo: The repository in which the checkout operation will be performed
    :param string commit_hash: The desired point to checkout the repository
    """
    with pushd.Dir(os.path.join(working_dir, repo)):
        exectools.cmd_assert('git checkout {}'.format(commit_hash))


@log
def get_channel_from_tag(tag):
    """Extract the channel name from a build tag

    :param string tag: A build tag (i.e.: v1.2.3-201901010000)
    :return: A string with the channel name (i.e.: 1.2)

    :raises AttributeError: If tag is not in the expected format
    """
    return re.search(r'^v(\d\.\d).+', tag).group(1)


@log
def update_metadata_manifests_dir(operator, metadata, channel):
    """Update channel-specific manifests in the metadata repository with the
    latest manifests found on the operator repository

    If the metadata repository is empty, bring the top-level package YAML file also

    :param string operator: Name of the operator that is the source for latest manifests
    :param string metadata: Name of the metadata repository
    :param string channel: Which manifests channel should be copied to the metadata repository
    """
    with pushd.Dir(os.path.join(working_dir)):
        exectools.cmd_assert('rm -rf {}/manifests/{}'.format(metadata, channel))
        exectools.cmd_assert('mkdir -p {}/manifests'.format(metadata))
        exectools.cmd_assert('cp -r {}/manifests/{} {}/manifests'.format(operator, channel, metadata))

        if not package_yaml_exists(metadata):
            exectools.cmd_assert('cp {} {}/manifests'.format(package_yaml_filename(operator), metadata))


@log
def package_yaml_exists(repo):
    """Checks presence of a package YAML file on the manifests of a particular repo

    :param string repo: Repository in which the presence of a package YAML should be checked
    :return: A bool, True if a package YAML is present, False otherwise
    """
    return len(glob.glob('{}/manifests/*.package.yaml'.format(os.path.join(working_dir, repo)))) > 0


@log
def package_yaml_filename(repo):
    """Return the full path to a package YAML file, since it usually is prefixed by the operator name

    :param string repo: Name of the repository in which the package YAML file is present
    :return: A string with the full path to the repository package YAML file
    """
    return glob.glob('{}/manifests/*.package.yaml'.format(os.path.join(working_dir, repo)))[0]


@log
def merge_streams_on_top_level_package_yaml(metadata, channel_to_update):
    """Update (or create) a channel entry on the package YAML file, pointing to the current CSV

    :param string metadata: Name of the metadata repository that should have its package YAML updated
    :param string channel_to_update: Name of the channel to have its currentCSV value updated
    """
    csv_yaml = yaml.safe_load(open(csv_yaml_filename(metadata, channel_to_update)))
    current_csv = csv_yaml['metadata']['name']

    package_yaml = yaml.safe_load(open(package_yaml_filename(metadata)))

    channel_index = find_channel(channel_to_update, package_yaml)

    if channel_index is not None:
        package_yaml['channels'][channel_index]['currentCSV'] = current_csv
    else:
        package_yaml['channels'].append({'name': str(channel_to_update),
                                         'currentCSV': str(current_csv)})

    with open(package_yaml_filename(metadata), 'w') as file:
        yaml.dump(package_yaml, file)


@log
def csv_yaml_filename(repo, channel):
    """Return the full path to a CSV YAML file, since it is usually prefixed by the operator name

    :param string repo: Name of the repository in which the CSV YAML file is present
    :param string channel: Name of the channel directory in which the CSV YAML file is present
    :return: A string with the full path to the CSV YAML file of a particular channel
    """
    pattern = '{}/manifests/{}/*.clusterserviceversion.yaml'.format(os.path.join(working_dir, repo), channel)
    return glob.glob(pattern)[0]


@log
def find_channel(desired_channel, package_yaml):
    """Search if there is already a specific channel entry in the list of channels of a given package YAML

    :param string desired_channel: Name of the channel to be searched
    :param dict package_yaml: Parsed structure of a package YAML file
    :return: An int with the list index of the desired channel if found, None otherwise
    """
    for index, channel in enumerate(package_yaml['channels']):
        if str(channel['name']) == str(desired_channel):
            return index
    return None


@log
def create_symlink_to_latest_csv(metadata):
    """Create a "latest" symlink inside the manifests directory, pointing to
    the latest manifest CSV file.

    :param string metadata: Name of the metadata repository in which the symlink should be created
    """
    channel = '4.2'  # hardcoded for now
    relative_csv_yaml_filename = (csv_yaml_filename(metadata, channel)
                                  .replace('{}/manifests/'.format(os.path.join(working_dir, metadata)), ''))

    with pushd.Dir(os.path.join(working_dir, metadata, 'manifests')):
        exectools.cmd_assert('rm -f latest')
        exectools.cmd_assert('ln -s {} latest'.format(relative_csv_yaml_filename))


@log
def create_metadata_dockerfile(operator, metadata):
    """Create a minimal Dockerfile on the metadata repository, copying all manifests
    inside the image and having the same labels as its corresponding operator Dockerfile

    Except the component label, that should not conflict with the operator, otherwise
    brew fails with a "build already exists" error

    :param string operator: Name of the operator repository from which Dockerfile labels will be copied
    :param string metadata: Name of the metadata repository in which the minimal Dockerfile will be created
    """
    with pushd.Dir(working_dir):
        operator_dockerfile = DockerfileParser('{}/Dockerfile'.format(operator))
        metadata_dockerfile = DockerfileParser('{}/Dockerfile'.format(metadata))
        metadata_dockerfile.content = 'FROM scratch\nCOPY ./manifests /manifests'
        metadata_dockerfile.labels = operator_dockerfile.labels
        metadata_dockerfile.labels['com.redhat.component'] = '{}-container'.format(metadata)
        metadata_dockerfile.labels['name'] = 'openshift/ose-{}'.format(metadata)


@log
def commit_and_push_repo(metadata_repo):
    """Commit and push changes made on the metadata_repo using rhpkg

    :param string metadata_repo: Name of the metadata repository with changes to be committed and pushed
    """
    with pushd.Dir(os.path.join(working_dir, metadata_repo)):
        try:
            exectools.cmd_assert('git add .')
            exectools.cmd_assert('rhpkg --user {} commit -m "Update operator metadata"'.format(rhpkg_user))
            exectools.cmd_assert('rhpkg --user {} push'.format(rhpkg_user))
        except:
            # The metadata repo might be already up to date, so we don't have
            # anything new to commit
            pass
