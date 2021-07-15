import unittest
import flexmock
from doozerlib.olm.bundle import OLMBundle


class TestOLMBundle(unittest.TestCase):

    def test_get_bundle_image_name_no_ose_prefix(self):
        obj = flexmock(OLMBundle(None), bundle_name='foo')
        self.assertEqual(obj.get_bundle_image_name(), 'openshift/ose-foo')

    def test_get_bundle_image_name_with_ose_prefix(self):
        obj = flexmock(OLMBundle(None), bundle_name='ose-foo')
        self.assertEqual(obj.get_bundle_image_name(), 'openshift/ose-foo')
