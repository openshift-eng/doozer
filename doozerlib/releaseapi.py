import asyncio
from doozerlib.image import ImageMetadata
from openshift_release import ApiClient

from openshift_release.models.io_k8s_apimachinery_pkg_apis_meta_v1_object_meta \
    import IoK8sApimachineryPkgApisMetaV1ObjectMeta as ObjectMeta

from openshift_release.models.com_github_vfreex_release_apiserver_pkg_apis_art_v1alpha1_source_revision \
    import ComGithubVfreexReleaseApiserverPkgApisArtV1alpha1SourceRevision as SourceRevision
from openshift_release.models.com_github_vfreex_release_apiserver_pkg_apis_art_v1alpha1_source_revision_spec \
    import ComGithubVfreexReleaseApiserverPkgApisArtV1alpha1SourceRevisionSpec as SourceRevisionSpec


DEFAULT_API_VERSION = "art.openshift.io/v1alpha1"


def sanitize_for_serialization(objects):
    api_client = ApiClient(pool_threads=0)
    for obj in objects:
        yield api_client.sanitize_for_serialization(obj)


def create_source_revision_for_image(group_name: str, release_name: str, image_meta: ImageMetadata, commit_hash: str) -> SourceRevision:
    sv = SourceRevision(
        api_version=DEFAULT_API_VERSION,
        kind="SourceRevision",
        metadata=ObjectMeta(
            name=f"{release_name}-image-{image_meta.image_name_short}",
            labels={
                "release-stream": group_name,
                "release": release_name,
                "kind": "image",
                "distgit-key": image_meta.distgit_key,
                "image-name": image_meta.image_name_short,
            },
        ),
        spec=SourceRevisionSpec(
            component_name=f"{group_name}-image-{image_meta.image_name_short}",
            commit_hash=commit_hash,
        )
    )
    return sv
