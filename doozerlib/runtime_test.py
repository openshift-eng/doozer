#!/usr/bin/env python
import unittest
import flexmock
import runtime
import exectools
import logutil

from model import Model, Missing

def stub_runtime():
    rt = runtime.Runtime(
        latest_parent_version=False,
        logger=logutil.getLogger(__name__),
        stage=False,
        branch=None,
    )
    rt.group_config=Model()
    return rt


class RuntimeTestCase(unittest.TestCase):
    def test_parallel_exec(self):
        ret = runtime.Runtime._parallel_exec(lambda x: x * 2, xrange(5), n_threads=20)
        self.assertEqual(ret.get(), [0, 2, 4, 6, 8])

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
            and_return("eggs")
        )
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

    def test_builds_for_group_branch(self):
        rt = stub_runtime()
        list_tagged = """
            somepackage-1.2.3-4.5.el7    some-tag    somebody
            some-container-v1.2.3-4.5    some-tag    somebody
        """
        expected = {
            "somepackage": ("1.2.3", "4.5.el7"),
            "some-container": ("v1.2.3", "4.5"),
        }
        flexmock(exectools).should_receive("cmd_assert").once().and_return(list_tagged, "")
        self.assertEqual(expected, rt.builds_for_group_branch())

        flexmock(exectools).should_receive("cmd_assert").once().and_return("\n", "")
        self.assertEqual({}, rt.builds_for_group_branch())


if __name__ == "__main__":
    unittest.main()
