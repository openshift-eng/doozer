from __future__ import unicode_literals, print_function
import os
import io
from future.standard_library import install_aliases
install_aliases()
from urllib.parse import urlparse
import requests

from doozerlib.logutil import getLogger
from doozerlib.util import mkdirs, is_in_directory

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
        source = urlparse(self.source)
        if source.scheme not in self.SUPPORTED_URL_SCHEMES:
            raise ValueError(
                "Unsupported URL scheme {} used in 'add' action.".format(source.scheme))
        source_url = source.geturl()  # normalized URL
        path = os.path.abspath(self.path)
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
