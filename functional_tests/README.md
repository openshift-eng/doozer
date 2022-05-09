 # Functional Tests for Doozer

This directory contains functional test suite for Doozer.
The test suite works by invoking `doozer` as a subprocess then checking for the exit code and output.

## Run Tests

Currently the test suite doesn't mock any services, such as dist-git, Brew, etc, but interacts with real production services. You have to obtain all required credentials before running the test suite:

``` sh
kinit $username
bugzilla login
# OCP pullspec in your $HOME/.docker/config.json
```

Then run the test suite using any tool that is compatible with Python unittest framework:

``` sh
# Go to project root directory

# Run with Python 3
python3 -m unittest discover -s functional_tests/
# Use Pytest with Python 3
python3 -m pytest functional_tests/
```

## Known Issues
1. Some test functions are not currently implemented but only a stub with `FIXME` marked. This is because those Doozer commands are either hard to test or they are already broken.

2. Because the test interacts with real production services, the test may be flaky due to service outage, network issues, or production data changes.
