import unittest
import os
import sys
import shutil
import pathlib
import logging
from doozerlib import exectools

DOOZER_CMD = [sys.executable, "-m", "doozerlib.cli"]

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

    def run_doozer(self, *args):
        out, err = exectools.cmd_assert(
            [
                *DOOZER_CMD,
                '--cache-dir', self.dz_cache_dir,
                '--working-dir', self.dz_working_dir,
                *args
            ],
            strip=True
        )
        return out.strip(), err.strip()

    def distgit_image_path(self, distgit_name):
        wd_path = pathlib.Path(self.dz_working_dir)
        return wd_path.joinpath(f'distgits/containers/{distgit_name}')

    def setUp(self):
        if os.path.exists(self.dz_working_dir):
            # Since we rm -rf this directory, let's make sure the testing framework creates it
            raise IOError(f'Did not expect: {self.dz_working_dir}')
        pass

    def tearDown(self):
        dlog = pathlib.Path(self.dz_working_dir).joinpath('debug.log')
        if dlog.exists():
            content = dlog.read_text()
            logging.debug('----DEBUG LOG CONTENT ---\n' + content + '\n--------------------\n')

        shutil.rmtree(self.dz_working_dir)

