import unittest
import os
import sys
import shutil
import pathlib
import logging
from doozerlib import exectools

DOOZER_CMD = [sys.executable, "-m", "doozerlib.cli"]

print('Logs will be written to tests_functional.log')
logging.basicConfig(filename='tests_functional.log', filemode='w+', level=logging.DEBUG)


class DoozerRunnerTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(DoozerRunnerTestCase, self).__init__(*args, **kwargs)
        self.dz_working_dir = os.environ.get('DOOZER_WORKING_DIR', None)
        if not self.dz_working_dir:
            raise IOError('Please set DOOZER_WORKING_DIR to a non-existent directory on a mount with sufficient storage')

        self.dz_cache_dir = os.environ.get('DOOZER_CACHE_DIR', None)
        if not self.dz_cache_dir:
            raise IOError('Please (for your own good), set DOOZER_CACHE_DIR appropriately before running')

        self.dz_user = os.environ.get('DOOZER_USER', None)
        self.logger = logging.getLogger(str(type(self).__name__))

    def run_doozer(self, *args):
        user_arg = []
        if self.dz_user:
            user_arg = ['--user', self.dz_user]

        cmd = [
            *DOOZER_CMD,
            *user_arg,
            '--cache-dir', self.dz_cache_dir,
            '--working-dir', self.dz_working_dir,
            *args
        ]

        print(f'Running doozer with: {cmd}', file=sys.stderr)

        rc, out, err = exectools.cmd_gather(cmd, strip=True)
        if rc:
            self.logger.error('Doozer executed with non-zero exit status.')
            self.logger.error(f'Stderror: {err}')
            raise IOError('Doozer exited with error; see tests_functional.log')

        return out.strip(), err.strip()

    def distgit_image_path(self, distgit_name):
        wd_path = pathlib.Path(self.dz_working_dir)
        return wd_path.joinpath(f'distgits/containers/{distgit_name}')

    def setUp(self):
        if os.path.exists(self.dz_working_dir):
            # Since we rm -rf this directory, let's make sure the testing framework creates it
            raise IOError(f'Please delete env.DOOZER_WORKING_DIR before running tests: {self.dz_working_dir}')
        pass

    def tearDown(self):
        dlog = pathlib.Path(self.dz_working_dir).joinpath('debug.log')
        if dlog.exists():
            content = dlog.read_text()
            self.logger.debug('----DEBUG LOG CONTENT ---\n' + content + '\n--------------------\n')

        if os.path.isdir(self.dz_working_dir):
            shutil.rmtree(self.dz_working_dir)
