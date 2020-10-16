#!/usr/bin/env python3

import unittest
from . import DoozerRunnerTestCase

import koji
import logging

from doozerlib import image, exectools, model
from doozerlib import brew


class TestKojiWrapper(DoozerRunnerTestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_koji_wrapper_caching(self):
        brew.KojiWrapper.clear_global_cache()

        # Get a non brew-event locked wrapper
        k = brew.KojiWrapper(['https://brewhub.engineering.redhat.com/brewhub'])
        call_meta: brew.KojiWrapperMetaReturn = k.getLastEvent(brew.KojiWrapperOpts(caching=True, return_metadata=True))
        self.assertFalse(call_meta.cache_hit)  # The first query should be a miss
        last_event = call_meta.result
        # The value is cached, so a non-meta return it should certainly be the same when we ask for it again
        self.assertEqual(k.getLastEvent(brew.KojiWrapperOpts(caching=True)), last_event)

        # Now ask for metadata to ensure it was cached
        call_meta: brew.KojiWrapperMetaReturn = k.getLastEvent(brew.KojiWrapperOpts(caching=True, return_metadata=True))
        self.assertTrue(call_meta.cache_hit)
        self.assertEqual(call_meta.result, last_event)

        # Now make the same request without caching and ensure cache_hit is not True
        call_meta: brew.KojiWrapperMetaReturn = k.getLastEvent(brew.KojiWrapperOpts(return_metadata=True))
        self.assertFalse(call_meta.cache_hit)

        # Repeat the above with a multicall
        def run_multicall():
            m = k.multicall()
            call_1 = m.getLastEvent(brew.KojiWrapperOpts(caching=True, return_metadata=True))
            call_2 = m.getTag('rhaos-4.7-rhel-8-candidate')  # Note that if caching/metadata is true for one call in multicall, it is True for all
            m.call_all()
            call_1_meta: brew.KojiWrapperMetaReturn = call_1.result
            call_2_meta: brew.KojiWrapperMetaReturn = call_2.result
            return call_1_meta, call_2_meta

        c1_meta, c2_meta = run_multicall()
        self.assertFalse(c1_meta.cache_hit)  # Though getLastEvent was cached above a single call, it has to be cached uniquely as a multiCall
        self.assertFalse(c2_meta.cache_hit)  # cache_hit should be identical for every call
        self.assertEqual(c2_meta.result['id'], 70115)  # The numeric id for the tag should not change

        # Now try it again and we should hit the cache
        _c1_meta, _c2_meta = run_multicall()
        self.assertTrue(_c1_meta.cache_hit)
        self.assertEqual(_c1_meta.result, c1_meta.result)
        self.assertEqual(_c2_meta.result, c2_meta.result)
        self.assertEqual(_c2_meta.result['id'], 70115)  # The numeric id for the tag should not change

        cache_size = brew.KojiWrapper.get_cache_size()
        brew.KojiWrapper.clear_global_cache()  # Test clearing cache
        self.assertGreater(cache_size, brew.KojiWrapper.get_cache_size())

        # After clearing the cache, we should miss again
        c1_meta, c2_meta = run_multicall()
        self.assertFalse(c1_meta.cache_hit)
        self.assertFalse(c2_meta.cache_hit)
        self.assertEqual(c2_meta.result['id'], 70115)  # The numeric id for the tag should not change

    def test_koji_wrapper_event_constraint(self):
        brew.KojiWrapper.clear_global_cache()

        lock_on_event = 34966523
        test_tag = 'rhaos-4.7-rhel-8-candidate'

        # Get a non brew-event locked wrapper
        k = brew.KojiWrapper(['https://brewhub.engineering.redhat.com/brewhub'], brew_event=lock_on_event)

        try:
            # If events are locked and user calls a non-constrainable koji API, we should raise an exception
            k.getLastEvent()
            self.fail('An exception should have been raised when invoking a non-constainable api call')
        except IOError:
            pass

        # However, if you tell the wrapper you are aware of the lock, you may perform the call
        k.getLastEvent(brew.KojiWrapperOpts(brew_event_aware=True))

        results = k.listTagged(test_tag, brew.KojiWrapperOpts(caching=True), package='openshift')  # This should be transparently locked in brew time by the wrapper
        self.assertEqual(results[0]['task_id'], 31840691)  # since we are locked in the time. this task_id is locked in time
        call_meta: brew.KojiWrapperMetaReturn = k.listTagged(test_tag, brew.KojiWrapperOpts(caching=True, return_metadata=True), package='openshift')  # Test to ensure caching is working with a brew event constraint.
        self.assertTrue(call_meta.cache_hit)
        self.assertEqual(call_meta.result[0]['task_id'], 31840691)

        # Now perform the same query, with caching on, without constraining the brew event.
        # The cache for constrained query vs an unconstrained query share different namespaces
        # and thus we expect a different result.
        unlocked_k = brew.KojiWrapper(['https://brewhub.engineering.redhat.com/brewhub'])
        unlocked_results = unlocked_k.listTagged(test_tag, brew.KojiWrapperOpts(caching=True), package='openshift')
        self.assertNotEqual(unlocked_results[0]['task_id'], 31840691)  # This was an unlocked query and we know the latest task has moved on


if __name__ == "__main__":
    unittest.main()
