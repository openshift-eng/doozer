#!/usr/bin/env python
import unittest
from flexmock import flexmock
from doozerlib import runtime, exectools, logutil, model


def stub_runtime():
    rt = runtime.Runtime(
        latest_parent_version=False,
        logger=logutil.getLogger(__name__),
        stage=False,
        branch='test-branch',
        rhpkg_config="",
    )
    rt.group_config = model.Model()
    return rt


class RuntimeTestCase(unittest.TestCase):
    def test_get_remote_branch_ref(self):
        rt = stub_runtime()
        flexmock(exectools).should_receive("cmd_assert").once().and_return("spam", "")
        res = rt._get_remote_branch_ref("giturl", "branch")
        self.assertEqual(res, "spam")

        flexmock(exectools).should_receive("cmd_assert").once().and_return("", "")
        self.assertIsNone(rt._get_remote_branch_ref("giturl", "branch"))

        flexmock(exectools).should_receive("cmd_assert").once().and_raise(Exception("whatever"))
        self.assertIsNone(rt._get_remote_branch_ref("giturl", "branch"))

    def test_detect_remote_source_branch(self):
        rt = stub_runtime()
        source_details = dict(
            url='some_git_repo',
            branch=dict(
                target='main_branch',
                fallback='fallback_branch',
                stage='stage_branch',
            ),
        )

        # got a hit on the first branch
        flexmock(rt).should_receive("_get_remote_branch_ref").once().and_return("spam")
        self.assertEqual(("main_branch", "spam"), rt.detect_remote_source_branch(source_details))

        # got a hit on the fallback branch
        (flexmock(rt).
            should_receive("_get_remote_branch_ref").
            and_return(None).
            and_return("eggs"))
        self.assertEqual(("fallback_branch", "eggs"), rt.detect_remote_source_branch(source_details))

        # no target or fallback branch
        flexmock(rt).should_receive("_get_remote_branch_ref").and_return(None)
        with self.assertRaises(runtime.DoozerFatalError):
            rt.detect_remote_source_branch(source_details)

        # request stage branch, get it
        rt.stage = True
        flexmock(rt).should_receive("_get_remote_branch_ref").once().and_return("spam")
        self.assertEqual(("stage_branch", "spam"), rt.detect_remote_source_branch(source_details))

        # request stage branch, not there
        rt.stage = True
        flexmock(rt).should_receive("_get_remote_branch_ref").once().and_return(None)
        with self.assertRaises(runtime.DoozerFatalError):
            rt.detect_remote_source_branch(source_details)


if __name__ == "__main__":
    unittest.main()
