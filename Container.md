# Build Container

The included script will build the doozer container locally using podman, tagged as `doozer:latest`.

`./build_doozer`

# Running the Doozer Container

You of course need a container runtime for this and we officially support `podman`. While `docker` may work we do not guarantee it or support it.
We highly recommend that you run rootless `podman`.

Install the `rundoozer` wrapper:

`pip install rundoozer`

Or, if you are developing `rundoozer`:

`cd rundoozer; python setup.py develop; cd ..`

*It's likely that eventually this will be renamed to just `doozer` as it will be the official way to use the tool. But this will require some directory rework, so it's being left until later.*

From there on you can run the doozer container just as you would run doozer with all the same CLI options. The usage should be 100% seemless to the point of not being obvious that it's running as a container.

The first time you run it will create `~/.config/rundoozer/settings.yaml`, see next section for details.

# `rundoozer` configuration

The `rundoozer` wrapper uses a config (defaults to `~/.config/rundoozer/settings.yaml`) with the same options as `doozer`. Please note that you can also set the environment variable `DOOZER_SETTINGS` to a yaml config path if you want to not use the default location.
This config has the following options:

- `data_path`: Git URL to build data repo
- `group`: Branch to pull build data from. Can also just specify `--group <group>`
- `working_dir`: Persisten working directory to use.
- `user`: Username to use for running tools. Automatically detected if unset but otherwise needs to match the user used for kerberos auth (if not system user).
- `global_opts`: Global overrides for things like timeout values.
- `kerb_cache`: The path to a custom kerberos ticket cache. The system default will be used if unspecified. If using custom you need to call `kinit -c <path>`
- `ssh_path`: Path to `.ssh` directory if not system default. This is required mainly for git auth and know_hosts.
- `gitconfig`: Path to .gitconfig if not system default. Required git commit operations like those in `images:rebase`.
- `container_bin`: Let's you choose between using `podman` or `docker` to run the container. If not given it will attempt to find what is installed, preferring `podman`.
- `container_opts`: A list of optional commands to send to `podman` such as `-v /misc:/misc`. This feature is mostly experimental at the moment and exists to provide an easy way to add options without changing `rundoozer`.

`rundoozer` will take all of these options, finding sane defaults if not given, and then generate a `settings.yaml` file in `working_dir` containing only `data_path`, `group`, `global_opts`, and `user`. Every other option for `doozer` command inside the container is set automatically.

More importantly, `rundoozer` uses the other given options to setup the container run call, automatically mapping everything to the correct place inside the container. Those mounts are as follows:

- kerberos ticket cache -> `/kerb`
- working directory -> `/working`
- .ssh -> `/root/.ssh/`
- .gitconfig -> `/root/.gitconfig`
