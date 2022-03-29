import imp
from unittest import TestCase
from doozerlib import rpm_utils


class TestRPMUtils(TestCase):
    def test_rpmverCmp(self):
        # Tests are extracted from
        # https://github.com/rpm-software-management/rpm/blob/9e4caf0fc536d1244b298abd9dc4c535b6560d69/tests/rpmvercmp.at#L1
        self.assertEqual(rpm_utils._rpmvercmp("1.0", "1.0"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("1.0", "2.0"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("2.0", "1.0"), 1)

        self.assertEqual(rpm_utils._rpmvercmp("2.0.1", "2.0.1"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("2.0", "2.0.1"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("2.0.1", "2.0"), 1)

        self.assertEqual(rpm_utils._rpmvercmp("2.0.1a", "2.0.1a"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("2.0.1a", "2.0.1"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("2.0.1", "2.0.1a"), -1)

        self.assertEqual(rpm_utils._rpmvercmp("5.5p1", "5.5p1"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("5.5p1", "5.5p2"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("5.5p2", "5.5p1"), 1)

        self.assertEqual(rpm_utils._rpmvercmp("5.5p10", "5.5p10"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("5.5p1", "5.5p10"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("5.5p10", "5.5p1"), 1)

        self.assertEqual(rpm_utils._rpmvercmp("xyz10", "xyz10"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("xyz10", "xyz10.1"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("xyz10.1", "xyz10"), 1)

        self.assertEqual(rpm_utils._rpmvercmp("xyz.4", "xyz.4"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("xyz.4", "8"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("8", "xyz.4"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("xyz.4", "2"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("2", "xyz.4"), 1)

        self.assertEqual(rpm_utils._rpmvercmp("5.5p2", "5.6p1"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("5.6p1", "5.5p2"), 1)

        self.assertEqual(rpm_utils._rpmvercmp("5.6p1", "6.5p1"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("6.5p1", "5.6p1"), 1)

        self.assertEqual(rpm_utils._rpmvercmp("6.0.rc1", "6.0"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("6.0", "6.0.rc1"), -1)

        self.assertEqual(rpm_utils._rpmvercmp("10b2", "10a1"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("10a2", "10b2"), -1)

        self.assertEqual(rpm_utils._rpmvercmp("1.0aa", "1.0aa"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("1.0a", "1.0aa"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0aa", "1.0a"), 1)

        self.assertEqual(rpm_utils._rpmvercmp("10.0001", "10.0001"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("10.0001", "10.1"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("10.1", "10.0001"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("10.10.0001", "10.0039"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("10.10.0039", "10.0001"), 1)

        self.assertEqual(rpm_utils._rpmvercmp("4.999.9", "5.0"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("5.0", "4.999.9"), 1)

        self.assertEqual(rpm_utils._rpmvercmp("20101121", "20101121"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("20101121", "20101122"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("20101122", "20101121"), 1)

        self.assertEqual(rpm_utils._rpmvercmp("2_0", "2_0"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("2.0", "2_0"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("2_0", "2.0"), 0)

        self.assertEqual(rpm_utils._rpmvercmp("a", "a"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("a+", "a+"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("a+", "a_"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("a_", "a+"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("+a", "+a"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("+a", "_a"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("_a", "+a"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("+_", "+_"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("_+", "+_"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("_+", "_+"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("+", "_"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("_", "+"), 0)

        self.assertEqual(rpm_utils._rpmvercmp("1.0~rc1", "1.0~rc1"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("1.0~rc1", "1.0"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0", "1.0~rc1"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0~rc1", "1.0~rc2"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0~rc2", "1.0~rc1"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0~rc1~git123", "1.0~rc1~git123"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("1.0~rc1~git123", "1.0~rc1"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0~rc1", "1.0~rc1~git123"), 1)

        self.assertEqual(rpm_utils._rpmvercmp("1.0^", "1.0^"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^", "1.0"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0", "1.0^"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^git1", "1.0^git1"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^git1", "1.0"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0", "1.0^git1"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^git1", "1.0^git2"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^git2", "1.0^git1"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^git1", "1.01"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("1.01", "1.0^git1"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^20160101", "1.0^20160101"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^20160101", "1.0.1"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0.1", "1.0^20160101"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^20160101^git1", "1.0^20160101^git1"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^20160102", "1.0^20160101^git1"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^20160101^git1", "1.0^20160102"), -1)

        self.assertEqual(rpm_utils._rpmvercmp("1.0~rc1^git1", "1.0~rc1^git1"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("1.0~rc1^git1", "1.0~rc1"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0~rc1", "1.0~rc1^git1"), -1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^git1~pre", "1.0^git1~pre"), 0)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^git1", "1.0^git1~pre"), 1)
        self.assertEqual(rpm_utils._rpmvercmp("1.0^git1~pre", "1.0^git1"), -1)
