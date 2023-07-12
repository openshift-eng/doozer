from typing import Dict, Optional, Tuple


NVR = Dict[str, Optional[str]]


def split_nvr_epoch(nvre: str):
    """Split nvre to N-V-R and E.

    This function is backported from `kobo.rpmlib.split_nvr_epoch`.

    @param nvre: E:N-V-R or N-V-R:E string
    @type nvre: str
    @return: (N-V-R, E)
    @rtype: (str, str)
    """

    if ":" in nvre:
        if nvre.count(":") != 1:
            raise ValueError("Invalid NVRE: %s" % nvre)

        nvr, epoch = nvre.rsplit(":", 1)
        if "-" in epoch:
            if "-" not in nvr:
                # switch nvr with epoch
                nvr, epoch = epoch, nvr
            else:
                # it's probably N-E:V-R format, handle it after the split
                nvr, epoch = nvre, ""
    else:
        nvr, epoch = nvre, ""

    return (nvr, epoch)


def parse_nvr(nvre: str):
    """Split N-V-R into a dictionary.

    This function is backported from `kobo.rpmlib.parse_nvr`.

    @param nvre: N-V-R:E, E:N-V-R or N-E:V-R string
    @type nvre: str
    @return: {name, version, release, epoch}
    @rtype: dict
    """

    if "/" in nvre:
        nvre = nvre.split("/")[-1]

    nvr, epoch = split_nvr_epoch(nvre)

    nvr_parts = nvr.rsplit("-", 2)
    if len(nvr_parts) != 3:
        raise ValueError("Invalid NVR: %s" % nvr)

    # parse E:V
    if epoch == "" and ":" in nvr_parts[1]:
        epoch, nvr_parts[1] = nvr_parts[1].split(":", 1)

    # check if epoch is empty or numeric
    if epoch != "":
        try:
            int(epoch)
        except ValueError:
            raise ValueError("Invalid epoch '%s' in '%s'" % (epoch, nvr))

    result = dict(zip(["name", "version", "release"], nvr_parts))
    result["epoch"] = epoch
    return result


def to_nevr(d: Dict):
    """ Converts an NEVR dict to N-E:V-R string
    """
    n = d["name"]
    e = d.get("epoch") or 0
    v = d["version"]
    r = d["release"]
    return f"{n}-{e}:{v}-{r}"


def to_nevra(d: Dict):
    """ Converts an NEVRA dict to N-E:V-R.A string
    """
    arch = d["arch"]
    return to_nevr(d) + f".{arch}"


def compare_nvr(nvr_dict1: NVR, nvr_dict2: NVR, ignore_epoch: bool = False):
    """Compare two N-V-R dictionaries.

    This function is backported from `kobo.rpmlib.compare_nvr`.

    @param nvr_dict1: {name, version, release, epoch}
    @type nvr_dict1: dict
    @param nvr_dict2: {name, version, release, epoch}
    @type nvr_dict2: dict
    @param ignore_epoch: ignore epoch during the comparison
    @type ignore_epoch: bool
    @return: nvr1 newer than nvr2: 1, same nvrs: 0, nvr1 older: -1, different names: ValueError
    @rtype: int
    """

    nvr1 = nvr_dict1.copy()
    nvr2 = nvr_dict2.copy()

    nvr1["epoch"] = nvr1.get("epoch", None)
    nvr2["epoch"] = nvr2.get("epoch", None)

    if nvr1["name"] != nvr2["name"]:
        raise ValueError("Package names doesn't match: %s, %s" % (nvr1["name"], nvr2["name"]))

    if ignore_epoch:
        nvr1["epoch"] = 0
        nvr2["epoch"] = 0

    if nvr1["epoch"] is None:
        nvr1["epoch"] = ""

    if nvr2["epoch"] is None:
        nvr2["epoch"] = ""

    return labelCompare((str(nvr1["epoch"]), str(nvr1["version"]), str(nvr1["release"])), (str(nvr2["epoch"]), str(nvr2["version"]), str(nvr2["release"])))


EVR = Tuple[Optional[str], str, str]


def labelCompare(a: EVR, b: EVR):
    """ This function is backported from C function `rpmverCmp`.
    https://github.com/rpm-software-management/rpm/blob/9e4caf0fc536d1244b298abd9dc4c535b6560d69/rpmio/rpmver.c#L115

    :return: 1: a is newer than b; 0: a and b are the same version; -1: b is newer than a
    """
    e1 = "0" if a[0] is None else a[0]
    e2 = "0" if b[0] is None else b[0]
    rc = _compare_values(e1, e2)
    if rc == 0:  # a.epoch == b.epoch
        rc = _compare_values(a[1], b[1])
        if rc == 0:  # a.version == b.version
            rc = _compare_values(a[2], b[2])
    return rc


def _compare_values(a: Optional[str], b: Optional[str]):
    """ This function is backported from C function `compare_values`.
    https://github.com/rpm-software-management/rpm/blob/9e4caf0fc536d1244b298abd9dc4c535b6560d69/rpmio/rpmver.c#L104
    """
    if a is None and b is None:
        return 0
    if a is not None and b is None:
        return 1
    if a is None and b is not None:
        return -1
    return _rpmvercmp(a, b)


def _rpmvercmp(a: str, b: str):
    """ This function is backported from C function `rpmvercmp`.
    https://github.com/rpm-software-management/rpm/blob/9e4caf0fc536d1244b298abd9dc4c535b6560d69/rpmio/rpmvercmp.c#L16
    """
    if a == b:
        return 0
    str1 = a + "\0"
    str2 = b + "\0"
    one = 0
    two = 0
    while str1[one] != "\0" or str2[two] != "\0":
        while str1[one] != "\0" and not str1[one].isalnum() and str1[one] not in ["~", "^"]:
            one += 1
        while str2[two] != "\0" and not str2[two].isalnum() and str2[two] not in ["~", "^"]:
            two += 1
        # handle the tilde separator, it sorts before everything else
        if str1[one] == "~" or str2[two] == "~":
            if str1[one] != "~":
                return 1
            if str2[two] != "~":
                return -1
            one += 1
            two += 1
            continue
        # Handle caret separator. Concept is the same as tilde,
        # except that if one of the strings ends (base version),
        # the other is considered as higher version.
        if str1[one] == "^" or str2[two] == "^":
            if str1[one] == "\0":
                return -1
            if str2[two] == "\0":
                return 1
            if str1[one] != "^":
                return 1
            if str2[two] != "^":
                return -1
            one += 1
            two += 1
            continue
        # If we ran to the end of either, we are finished with the loop
        if not (str1[one] != "\0" and str2[two] != "\0"):
            break

        p = one
        q = two

        # grab first completely alpha or completely numeric segment
        # leave one and two pointing to the start of the alpha or numeric
        # segment and walk str1 and str2 to end of segment
        if str1[p].isdigit():
            while (str1[p] != "\0" and str1[p].isdigit()):
                p += 1
            while (str2[q] != "\0" and str2[q].isdigit()):
                q += 1
            isnum = 1
        else:
            while (str1[p] != "\0" and str1[p].isalpha()):
                p += 1
            while (str2[q] != "\0" and str2[q].isalpha()):
                q += 1
            isnum = 0

        # this cannot happen, as we previously tested to make sure that
        # the first string has a non-null segment
        if one == p:
            return -1  # arbitrary

        # take care of the case where the two version segments are
        # different types: one numeric, the other alpha (i.e. empty)
        # numeric segments are always newer than alpha segments
        if two == q:
            return 1 if isnum else -1

        if isnum:
            # throw away any leading zeros - it's a number, right?
            while str1[one] == "0":
                one += 1
            while str2[two] == "0":
                two += 1
            # whichever number has more digits wins
            onelen = p - one
            twolen = q - two
            if onelen > twolen:
                return 1
            if onelen < twolen:
                return -1

        # strcmp will return which one is greater - even if the two
        # segments are alpha or if they are numeric.  don't return
        # if they are equal because there might be more segments to
        # compare

        if str1[one:p] < str2[two:q]:
            return -1
        if str1[one:p] > str2[two:q]:
            return 1

        one = p
        two = q

    # this catches the case where all numeric and alpha segments have
    # compared identically but the segment sepparating characters were
    # different

    if str1[one] == "\0" and str2[two] == "\0":
        return 0

    # whichever version still has characters left over wins
    return -1 if str1[one] == "\0" else 1
