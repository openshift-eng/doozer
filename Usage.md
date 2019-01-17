# Main workflow

The main purpose of doozer is to run brew builds based on upstream sources. This workflow consists of cloning the internal dist-git repo, cloning the upstream source repo, and rebasing the source into the dist-git with minor alterations. Both RPMs and container images are supported. We will mostly discuss image builds here.

# How to get doozer

Installing doozer is easy, just run `pip install rh-doozer` in the Python 2 environment of your choosing. We highly recommend using a virtual environment.

You will also need to install various tools used by doozer. Refer to [README.md#installation|the README].

Finally, you'll need a private ssh key with access to clone sources from github.com, and probably a kerberos ticket for use with dist-git.

# Doozer setup

After installing you will need to setup your doozer config, by adding the file `~/.config/doozer/settings.yaml` with the following contents:

```
#Persistent working directory to use
working_dir: <path_to_dir>

#Git URL or File Path to build data
data_path: https://github.com/openshift/ocp-build-data.git

#Sub-group directory or branch to pull build data
group: openshift-4.0
```

Note, all three options above can be set at the CLI with `doozer --working-dir <path> --data-path <url> --group <group>` but we highly recommend setting them in `settings.yaml` to save typing evertime you run.
Also, please note that `group` in `settings.yaml` only makes sense if you only ever work on that one version, otherwise specify it on the CLI at runtime.

# Dependencies

You will also need to make sure that you have `imagebuilder` installed and that `docker` is properly configured to point to the internal registry. Please follow the instructions on the [Doozer Setup doc](https://github.com/openshift/doozer/blob/master/README.md#local-image-builds).

# Image Configuration

First, please note that you will need to have your image configured in a `doozer` compatible config repo, like [ocp-build-data](https://github.com/openshift/ocp-build-data/) as shown in the `doozer` config above. If your image is already built as part of the OCP release then it already exists in that repo and you are good to go.

If your image is not already part of the OCP build, we recommend forking `ocp-build-data` and adding it for the sake of testing. Just remember to update `settings.yaml` to point to your version.

Alternatively, you can also clone `ocp-build-data` locally and make changes without commit to it. Then specify the file path to that cloned repo instead of the URL to use that local version.
If you have questions about the layout and format of `ocp-build-data` please reach out to ART via [aos-team-art@redhat.com](mailto:aos-team-art@redhat.com)

# Running a Local Build

Once all the above is ready, you can build your image! 

*Please note that yes, you need to refer to your image to `doozer` with the **distgit** name which is different than the common name for the image. In the near future ART will likely update `doozer` to take either name, but for the time being this is a historical usage quirk that's held over. So, for example, if you were to build `openshift/ose-ansible` you would need to specify `aos3-installation` which is defined by the yaml config [with the same name](https://github.com/openshift/ocp-build-data/blob/openshift-4.0/images/aos3-installation.yml) on `ocp-build-data`. All image config files in `ocp-build-data` have names that match their dist-git repo name, without exception.*

It requires 2 steps:

First, run:

`doozer --local --latest-parent-version --group openshift-4.0 -i <distgit_name> images:rebase`

There are options available for `images:rebase` like the version and release strings, but they are not strictly necessary for a local build. This command will clone down your image source from the configured repo and copy the necessary files to a build directory while applying a few modifications required for the OCP build process.

Note: `--latest-parent-version` works around the assumption that doozer makes about parent-child relationships of images, which is that parent and child will be built at the same version in a single run. You will need this option almost all the time unless you are building a base image or all of the images at once.

Note that it will only pull from the configured repo and branch which is typically the released code, which for obvious reasons may be undesireable for the sake of testing. If you already have the repo with test code locally you can override that remote clone by running:

`doozer --local --latest-parent-version --group openshift-4.0 --source <distgit_name> <path_to_repo> -i <distgit_name> images:rebase`

This will then only use the local code for running the build. This operation will in no way modify the given repo, only copy the files into the `doozer` working directory.

Next, to build the image run:

`doozer --local --group openshift-4.0 -i <distgit_name> images:build`

It will now build the image using either `imagebuilder` or `docker build` as configured in the image's config yaml. The stdout of the build process will be written out to the console in realtime so that you can monitor the progress.

That's it. Once done, the image will be available to docker locally.

# Cleaning the Workspace

Please note that the given working directory (as noted above) is intended to be persistent between `doozer` steps. You can even run subequent builds of the same or other images with the same persistent working directory. However, if you switch versions of OCP you are testing against you will need to delete the contents of the directory before running `doozer`. 
