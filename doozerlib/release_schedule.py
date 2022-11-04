from datetime import datetime
import os
import threading

import yaml

from doozerlib import gitdata


class ReleaseSchedule:
    """
    This class is a Singleton. Only at first object construction, it will clone ocp-release-schedule repo inside Doozer
    working directory, parse and store yaml data related to the current OCP version
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, runtime):
        if cls._instance is None:
            with cls._lock:
                # Another thread could have created the instance before the lock was acquired
                # So check that the instance is still nonexistent.
                if cls._instance is None:
                    instance = super(ReleaseSchedule, cls).__new__(cls)
                    instance.initialize(runtime)
                    cls._instance = instance
        return cls._instance

    def initialize(self, runtime):
        if 'GITLAB_TOKEN' not in os.environ:
            raise ValueError('A GITLAB_TOKEN env var must be defined')

        # Clone ocp-release-schedule in doozer working dir
        git_data = gitdata.GitData(
            data_path=f'https://oauth2:{os.environ["GITLAB_TOKEN"]}@gitlab.cee.redhat.com/ocp-release-schedule/schedule.git',
            clone_dir=runtime.working_dir
        )

        # Parse and store relevant yaml
        major = runtime.group_config.vars['MAJOR']
        minor = runtime.group_config.vars['MINOR']
        config_file = f'{git_data.data_dir}/schedules/{major}.{minor}.yaml'
        with open(config_file) as f:
            self.schedule_data = yaml.safe_load(f.read())

    def get_ff_date(self) -> datetime:
        event = next(item for item in self.schedule_data['events'] if item["name"] == "feature-freeze")
        return datetime.strptime(event['date'], '%Y-%m-%dT%H:00:00-00:00')
