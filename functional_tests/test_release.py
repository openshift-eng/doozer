import unittest
import io
import os
import pygit2
import yaml
import tempfile
from . import run_doozer, get_working_dir


class TestRelease(unittest.TestCase):
    def test_release_gen_multiarch_payload(self):
        run_doozer([
            "--group=openshift-4.2",
            "--images=cluster-version-operator",
            "release:gen-multiarch-payload",
            "--is-name=4.2-art-latest",
            "--is-namespace=ocp",
            "--organization=openshift-release-dev",
            "--repository=ocp-v4.0-art-dev",
        ])
        with io.open("src_dest.x86_64", encoding="utf-8") as mirror_file:
            src, dest = mirror_file.read().split("=")
            self.assertIn("registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-cluster-version-operator", src)
            self.assertIn("quay.io/openshift-release-dev/ocp-v4.0-art-dev", dest)

        with io.open("image_stream.s390x.yaml", encoding="utf-8") as is_file:
            is_obj = yaml.safe_load(is_file)
            self.assertEqual("4.2-art-latest-s390x", is_obj["metadata"]["name"])
            self.assertEqual("ocp-s390x", is_obj["metadata"]["namespace"])
            self.assertIn("quay.io/openshift-release-dev/ocp-v4.0-art-dev", is_obj["spec"]["tags"][0]["from"]["name"])
            self.assertIn("cluster-version-operator", is_obj["spec"]["tags"][0]["name"])

    def test_release_gen_payload(self):
        isb_content = """
        kind: ImageStream
        apiVersion: image.openshift.io/v1
        metadata:
            name: 4.1-art-latest
            namespace: ocp
        spec:
            tags: []
        """
        with tempfile.NamedTemporaryFile(prefix="doozer-test-isb-", suffix=".yaml", mode="w") as f:
            f.write(isb_content)
            f.flush()
            run_doozer([
                "--group=openshift-4.1",
                "--images=cluster-version-operator",
                "release:gen-payload",
                "--src-dest=mirror.txt",
                "--image-stream=release-is.yaml",
                "--is-base", f.name,
                "registry-proxy.engineering.redhat.com/rh-osbs/openshift-{image_name_short}=quay.io/openshift-release-dev/ocp-v4.0-art-dev:{version}-{release}-{image_name_short}",
            ])
        with io.open("release-is.yaml", encoding="utf-8") as f:
            release_is = yaml.safe_load(f)
            self.assertIn("4.1-art-latest", release_is["metadata"]["name"])

        with io.open("mirror.txt", encoding="utf-8") as mirror_file:
            src, dest = mirror_file.read().split("=")
            self.assertIn("registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-cluster-version-operator", src)
            self.assertIn("quay.io/openshift-release-dev/ocp-v4.0-art-dev", dest)

        with io.open("release-is.yaml", encoding="utf-8") as is_file:
            is_obj = yaml.safe_load(is_file)
            self.assertEqual("4.1-art-latest", is_obj["metadata"]["name"])
            self.assertEqual("ocp", is_obj["metadata"]["namespace"])
            self.assertIn("quay.io/openshift-release-dev/ocp-v4.0-art-dev", is_obj["spec"]["tags"][0]["from"]["name"])
            self.assertIn("cluster-version-operator", is_obj["spec"]["tags"][0]["name"])
