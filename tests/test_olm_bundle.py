import unittest

from flexmock import flexmock
from mock import MagicMock

from doozerlib.olm.bundle import OLMBundle


class TestOLMBundle(unittest.TestCase):

    def test_get_bundle_image_name_no_ose_prefix(self):
        obj = flexmock(OLMBundle(None, dry_run=False, brew_session=MagicMock()), bundle_name='foo')
        self.assertEqual(obj.get_bundle_image_name(), 'openshift/ose-foo')

    def test_get_bundle_image_name_with_ose_prefix(self):
        obj = flexmock(OLMBundle(None, dry_run=False, brew_session=MagicMock()), bundle_name='ose-foo')
        self.assertEqual(obj.get_bundle_image_name(), 'openshift/ose-foo')

    def test_valid_subscription_label_is_present(self):
        expected_key = 'operators.openshift.io/valid-subscription'
        expected_val = '["My", "Subscription", "Label"]'

        # mocking
        config = {'update-csv': {'valid-subscription-label': expected_val}}
        runtime = flexmock(
            group_config=flexmock(operator_channel_stable='default'),
            image_map={'myimage': flexmock(config=config)}
        )
        obj = flexmock(OLMBundle(runtime, dry_run=False, brew_session=MagicMock()), operator_name='myimage')
        obj.channel = '...'
        obj.package = '...'

        self.assertIn(expected_key, obj.operator_framework_tags.keys())
        actual_val = obj.operator_framework_tags[expected_key]
        self.assertEqual(expected_val, actual_val)
