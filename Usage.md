# Main workflow

The main purpose of doozer is to run brew builds based on upstream sources. This workflow consists of cloning the internal dist-git repo, cloning the upstream source repo, and rebasing the source into the dist-git with minor alterations. Both RPMs and container images are supported. We will mostly discuss image builds here.

# How to get doozer

Follow the [Installation Instructions](Container.md)

# Doozer Configuration

After installing you will need to setup your doozer config, by adding the config file with the contents below:
For containerized doozer edit `~/.config/rundoozer/settings.yaml`
For local doozer edit `~/.config/doozer/settings.yaml`

```
#Persistent working directory to use
working_dir: <path_to_dir>

#Git URL or File Path to build data
data_path: https://github.com/openshift-eng/ocp-build-data.git

#Sub-group directory or branch to pull build data
group: openshift-4.0

#Username for running rhpkg / brew / tito
user: ahaile

# URLs for internal systems
hosts:
  prodsec_git: ...

#Global option overrides that can only be set from settings.yaml
global_opts:
  # num of concurrent distgit pull/pushes
  distgit_threads: 20
  # timeout for `rhpkg clone` operation
  rhpkg_clone_timeout: 900
  # timeout for `rhpkg push` operation
  rhpkg_push_timeout: 1200
```

See the [containerized doozer doc](Container.md) for more details on its specific config options.

Note, the options `working_dir`, `data_path`, `group`, and `user` above can be set at the CLI with `doozer --working-dir <path> --data-path <url> --group <group> --user <user>` but we highly recommend setting at least `working_dir` and `data_path` in `settings.yaml` to save typing every time you run.
Setting `group` in `settings.yaml` only makes sense if you only ever work on that one version, otherwise specify it on the CLI at runtime.
Finally, `global_opts` is used to set some internal options and can only be configured from `settings.yaml`. This is intentional as they are meant to be global to the system running doozer.

# Image Configuration

First, please note that you will need to have your image configured in a `doozer` compatible config repo, like [ocp-build-data](https://github.com/openshift-eng/ocp-build-data/) as shown in the `doozer` config above. If your image is already built as part of the OCP release then it already exists in that repo and you are good to go.

If your image is not already part of the OCP build, we recommend forking `ocp-build-data` and adding it for the sake of testing. Just remember to update `settings.yaml` to point to your version.

Alternatively, you can also clone `ocp-build-data` locally and make changes without commit to it. Then specify the file path to that cloned repo instead of the URL to use that local version.
If you have questions about the layout and format of `ocp-build-data` please reach out to ART via [aos-team-art@redhat.com](mailto:aos-team-art@redhat.com)

# Running a Local Build

For local builds you will need to make sure that you have `imagebuilder` installed and that you can access a properly-configured `docker`. Please follow the instructions on the [Doozer Setup doc](https://github.com/openshift-eng/doozer/blob/master/README.md#local-image-builds).

Once all the above is ready, you can build your image!

*Please note that yes, you need to refer to your image to `doozer` with the **dist-git** name which is different than the common name for the image. In the near future ART will likely update `doozer` to take either name, but for the time being this is a historical usage quirk that's held over. So, for example, if you were to build `openshift/ose-ansible` you would need to specify `aos3-installation` which is defined by the yaml config [with the same name](https://github.com/openshift-eng/ocp-build-data/blob/openshift-4.0/images/aos3-installation.yml) on `ocp-build-data`. All image config files in `ocp-build-data` have names that match their dist-git repo name, without exception.*

It requires 2 steps.

## Rebasing (copying from source)

First, run:

`doozer --local --latest-parent-version --group openshift-4.0 -i <distgit_name> images:rebase`

There are options available for `images:rebase` like the version and release strings, but they are not strictly necessary for a local build. This command will clone your image source from the configured repo and copy the necessary files to a build directory while applying a few modifications required for the OCP build process.

Note: `--latest-parent-version` works around the assumption that doozer makes about parent-child relationships of images for official builds; you will need this option for most local builds.

Note that it will only pull from the configured repo and branch, typically already-merged code, while you may need a different fork and/or branch testing. If you already have the repo with test code checked out locally you can override that remote clone by running:

`doozer --local --latest-parent-version --group openshift-4.0 --source <distgit_name> <path_to_repo> -i <distgit_name> images:rebase`

Then doozer uses the local code for running the build. This operation will in no way modify the given repo, only copy the files into the `doozer` working directory.

Important: after a source is cloned, doozer never updates it. If you make changes to the source in github, rebase will not pull in those changes. You can either update it manually or just delete it so it is cloned again.

## Building

Next, to build the image run:

`doozer --local --group openshift-4.0 -i <distgit_name> images:build`

It will now build the image using either `imagebuilder` or `docker build` as configured in the image's config yaml. The stdout of the build process will be written out to the console in realtime so that you can monitor the progress.

That's it. Once successful, the image is available in docker locally.

# Cleaning the Workspace

Please note that the given working directory is intended to be persistent between `doozer` steps. You can run subequent builds of the same or other images with the same persistent working directory; previously cloned repos will remain. If you switch versions of OCP you are testing against, or want to pull updates to repos already cloned, it is best to delete the contents of the working directory before running `doozer`.

# Operator metadata

    $ doozer operator:metadata cluster-logging-operator-container-v4.2.0-201908070219 --merge-branch dev
