# Runs a suite of functional tests with no side effects.

## Setup
It is suggested that you run these tests on buildvm. 

### Ensure and ssh-agent with clone permissions is running
```
$ eval $(ssh-agent)
$ ssh-add ...
```

### Set environment variables
Set `DOOZER_WORKING_DIR` to a directory that does not exist. This will be used as the working directory
for the tests. Ensure the mount on which it resides has sufficient space.
```
$ export DOOZER_WORKING_DIR=$PWD/test-wd
``` 

Set `DOOZER_CACHE_DIR` appropriately
```
# on buildvm
$ export DOOZER_CACHE_DIR=/mnt/workspace/jenkins/doozer_cache
```

## Run all the tests
```
$ python3 -m unittest discover -s tests_functional
```

## Run a specific suite
```
$  python3 -m unittest tests_functional/test_basic_rebase.py
```