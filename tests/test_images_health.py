import unittest
from unittest.mock import MagicMock

from doozerlib.cli import images_health


def add_record(image, state, task_id=123, build_url='link', time='2000', name='name'):
    '''Helper function to set up data for querying'''
    if image not in data:
        data[image] = []
    data[image].append({
        'Name': name,
        'Attributes': [
            {'Name': 'brew.task_id', 'Value': str(task_id)},
            {'Name': 'brew.task_state', 'Value': state},
            {'Name': 'jenkins.build_url', 'Value': build_url},
            {'Name': 'build.time.unix', 'Value': time}]
    })


def get_concerns(image):
    '''Interface for the real get_concerns function, with inconsequential options filled in'''
    return images_health.get_concerns(image, {}, 5, 'slack')


data = {}
add_record('containers/openshift-state-metrics', 'success')
add_record('containers/openshift-state-metrics', 'success')
add_record('containers/openshift-state-metrics', 'success')

add_record('new_failure', 'fail')
add_record('new_failure', 'success')

add_record('all_fail', 'fail')
add_record('all_fail', 'fail')

add_record('old_success', 'success', time=500)


class TestImagesHealthCli(unittest.TestCase):
    def mock_query(name, runtime={}, limit=100):
        return data[name]

    def mock_older_than_two_weeks(task_record):
        return task_record.ts < 1000

    images_health.query = MagicMock(side_effect=mock_query)
    images_health.older_than_two_weeks = MagicMock(side_effect=mock_older_than_two_weeks)

    def test_no_concerns(self):
        self.assertEqual(get_concerns('containers/openshift-state-metrics'), [])

    def test_new_failure(self):
        concerns = get_concerns('new_failure')
        self.assertEqual(len(concerns), 1)
        self.assertRegex(concerns[0], '^Latest attempt')
        self.assertIn('was 1 attempts ago', concerns[0])

    def test_all_fail(self):
        concerns = get_concerns('all_fail')
        self.assertEqual(len(concerns), 1)
        self.assertRegex(concerns[0], '^Latest attempt')
        self.assertIn('Failing for at least the last 2 attempts', concerns[0])

    def test_old_success(self):
        concerns = get_concerns('old_success')
        self.assertEqual(len(concerns), 1)
        self.assertIn('was over two weeks ago', concerns[0])


if __name__ == '__main__':
    unittest.main()
