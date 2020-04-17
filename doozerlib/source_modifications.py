from __future__ import absolute_import, print_function, unicode_literals
import os
import io
from future.standard_library import install_aliases
install_aliases()
from urllib.parse import urlparse
import requests

from doozerlib.logutil import getLogger
from doozerlib.util import mkdirs, is_in_directory
from doozerlib.model import Missing
from doozerlib.exceptions import DoozerFatalError
from doozerlib.exectools import cmd_assert
from doozerlib.pushd import Dir

LOGGER = getLogger(__name__)


class SourceModifierFactory(object):
    """A factory class for creating source modifier objects."""
    MODIFICATIONS = {}

    @classmethod
    def supports(cls, action_name):
        """Test if specified modification action is supported"""
        return action_name in cls.MODIFICATIONS

    def create(self, *args, **kwargs):
        """Create a source modifier based on action.

        For example, create a source modifier for adding an out-of-tree file:

            factory = SourceModifierFactory()
            modifier = factory.create(action='add', source='http://example.com/gating_yaml', dest='gating.yaml', overwrite=True)
            modifier.modify()
        """
        action = kwargs["action"]
        if not self.supports(action):
            raise KeyError("Unknown modification action: {}.".format(action))
        return self.MODIFICATIONS[action](*args, **kwargs)


class AddModifier(object):
    """ A source modifier that supports adding an out-of-tree source to dist-git.

    An `add` action has the following valid fields:

    - `action`: must be `add`
    - `source`: URL to the out-of-tree source
    - `path`: Path in dist-git to write the source to
    - `overwriting`: Allow to overwrite if `path` exists

    For example, to add an out-of-tree source https://gitlab.cee.redhat.com/aosqe/ocp-build-data-gating/raw/master/openshift-3.11/atomic-openshift-cluster-autoscaler/gating_yaml to dist-git and save as `gating.yaml`:

        content:
          source:
            git:
              branch:
                fallback: master
                target: release-{MAJOR}.{MINOR}
              url: git@github.com:openshift/kubernetes-autoscaler.git
            modifications:
            - action: replace
              match: origin-cluster-autoscaler
              replacement: atomic-openshift-cluster-autoscaler
            - action: add
              source: https://gitlab.cee.redhat.com/aosqe/ocp-build-data-gating/raw/master/openshift-3.11/atomic-openshift-cluster-autoscaler/gating_yaml
              path: gating.yaml
              overwriting: true
            path: images/cluster-autoscaler
        # omitted
    """

    SUPPORTED_URL_SCHEMES = ["http", "https"]

    def __init__(self, *args, **kwargs):
        """ Initialize an "add" Modifier.
        :param source: URL to the out-of-tree source.
        :param path: Destination path to the dist-git repo.
        :param overwriting: True to allow to overwrite if path exists.
          Setting to false to prevent from accidently overwriting files from in-tree source.

        """
        self.source = kwargs["source"]
        self.path = kwargs["path"]
        self.overwriting = kwargs.get("overwriting", False)

    def act(self, *args, **kwargs):
        """ Run the modification action

        :param ceiling_dir: If not None, prevent from writing to a directory that is out of ceiling_dir.
        :param session: If not None, a requests.Session object for HTTP requests
        """
        LOGGER.debug("Running 'add' modification action...")
        context = kwargs["context"]
        distgit_path = context['distgit_path']
        source = urlparse(self.source)
        if source.scheme not in self.SUPPORTED_URL_SCHEMES:
            raise ValueError(
                "Unsupported URL scheme {} used in 'add' action.".format(source.scheme))
        source_url = source.geturl()  # normalized URL
        path = str(distgit_path.joinpath(self.path))
        ceiling_dir = kwargs.get("ceiling_dir")
        session = kwargs.get("session") or requests.session()
        if ceiling_dir and not is_in_directory(path, ceiling_dir):
            raise ValueError("Writing to a file out of {} is not allowed.".format(ceiling_dir))
        # NOTE: `overwriting` is checked before writing.
        # Data race might happen but it should suffice for prevent from accidently overwriting in-tree sources.
        if not self.overwriting and os.path.exists(path):
            raise IOError(
                "Destination path {} exists. Use 'overwriting: true' to overwrite.".format(self.path))
        LOGGER.debug("Getting out-of-tree source {}...".format(source_url))
        response = session.get(source_url)
        response.raise_for_status()
        mkdirs(os.path.dirname(path))
        with io.open(path, "wb") as dest_file:
            dest_file.write(response.content)
        LOGGER.debug("Out-of-tree source saved: {} -> {}".format(source_url, path))


SourceModifierFactory.MODIFICATIONS["add"] = AddModifier


class ReplaceModifier(object):
    """ A source modifier that supports replacing a substring in Dockerfile or RPM spec file.
    """

    def __init__(self, *args, **kwargs):
        """ Initialize ReplaceModifier
        :param match: This is old substring to be replaced.
        :param replacement: This is new substring, which would replace old substring.
        """
        self.match = kwargs["match"]
        self.replacement = kwargs["replacement"]

    def act(self, *args, **kwargs):
        """ Run the modification action

        :param context: A context dict. `context.component_name` is the dist-git repo name,
            and `context.content` is the content of Dockerfile or RPM spec file.
        """
        context = kwargs["context"]
        content = context["content"]
        component_name = context["component_name"]
        match = self.match
        assert (match is not Missing)
        replacement = self.replacement
        assert (replacement is not Missing)
        if replacement is None:  # Nothing follows colon in config yaml; user attempting to remove string
            replacement = ""
        pre = content
        post = pre.replace(match, replacement)
        if post == pre:
            raise DoozerFatalError("{}: Replace ({}->{}) modification did not make a change to the Dockerfile content"
                                   .format(component_name, match, replacement))
        LOGGER.debug(
            "Performed string replace '%s' -> '%s':\n%s\n" %
            (match, replacement, post))
        context["content"] = post


SourceModifierFactory.MODIFICATIONS["replace"] = ReplaceModifier


class CommandModifier(object):
    """ A source modifier that supports running a custom command to modify the source.
    """

    def __init__(self, *args, **kwargs):
        """ Initialize CommandModifier
        :param command: a `str` or `list` of the command with arguments
        """
        self.command = kwargs["command"]

    def act(self, *args, **kwargs):
        """ Run the command
        :param context: A context dict. `context.set_env` is a `dict` of env vars to set for command (overriding existing).
        """
        context = kwargs["context"]
        set_env = context["set_env"]
        with Dir(context['distgit_path']):
            cmd_assert(self.command, set_env=set_env)


SourceModifierFactory.MODIFICATIONS["command"] = CommandModifier
