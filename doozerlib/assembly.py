import copy
import typing
from enum import Enum

from doozerlib.model import ListModel, Missing, Model


class AssemblyTypes(Enum):
    STREAM = "stream"  # Default assembly type - indicates continuous build and no basis event
    STANDARD = "standard"  # All constraints / checks enforced (e.g. consistent RPMs / siblings)
    CANDIDATE = "candidate"  # Indicates release candidate or feature candidate
    CUSTOM = "custom"  # No constraints enforced
    PREVIEW = "preview"  # Indcates a .next (internal name) or preview release


class AssemblyIssueCode(Enum):

    # A class of issue so severe that we should not allow permits around it.
    IMPERMISSIBLE = 0

    # Expected member images to contain an assembly-specified override
    # dependency at a specified NVR, but it was found installed at a
    # different version, indicating that image needs a rebuild or a
    # reconfiguration.
    CONFLICTING_INHERITED_DEPENDENCY = 1

    # Different members of the assembly installed different versions of the
    # same group RPM build, so may omit bug fixes or introduce subtle bugs.
    CONFLICTING_GROUP_RPM_INSTALLED = 2

    # Two containers built from the same source are not built from the same
    # commit. Devs warn this can introduce subtle compatibility bugs.
    MISMATCHED_SIBLINGS = 3

    # The container includes a different version of an RPM than the assembly
    # so there is a risk of not officially shipping the RPM we are using, or
    # of misreporting bug fixes. We may either rebuild the container or change
    # the assembly to resolve.
    OUTDATED_RPMS_IN_STREAM_BUILD = 4

    # The arch-specific builds of RHCOS did not all install the same RPM
    # versions (if they installed at all).
    INCONSISTENT_RHCOS_RPMS = 5

    # A dependency was defined explicitly for an assembly member, but it is not
    # installed, i.e. the ARTists expected something to be installed, but it
    # wasn't found in the final image.
    # In rare circumstances the specified RPM may be used by a build stage of
    # the Dockerfile and not installed in the final stage, in which case, the
    # assembly definition should be altered to permit it.
    MISSING_INHERITED_DEPENDENCY = 6

    # group config specifies an RHCOS container expected in the RHCOS build
    # metadata, but it is not present for the current build. We experienced
    # this when RHCOS was introducing a new container, but it was not yet
    # configured in the RHCOS pipeline for all arches.
    MISSING_RHCOS_CONTAINER = 7

    # A consistency requirement for packages between members (currently only
    # specifiable in group.yml rhcos config) failed -- the members did not have
    # the same version installed.
    FAILED_CONSISTENCY_REQUIREMENT = 8

    # A consistency requirement for different packages failed
    # Currently this failure only occurs if kernel and kernel-rt rpms
    # in x86_64 RHCOS have different versions
    FAILED_CROSS_RPM_VERSIONS_REQUIREMENT = 9

    # An rpm defined in rpm_deliveries group config is missing the ship-ok Brew tag.
    # Note this issue code only occurs in pre-GA releases (ECs or RCs).
    # For GA releases, IMPERMISSIBLE code will be reported.
    MISSING_SHIP_OK_TAG = 10


class AssemblyIssue:
    """
    An encapsulation of an issue with an assembly. Some issues are critical for any
    assembly and some are on relevant for assemblies we intend for GA.
    """
    def __init__(self, msg: str, component: str, code: AssemblyIssueCode = AssemblyIssueCode.IMPERMISSIBLE):
        """
        :param msg: A human readable message describing the issue.
        :param component: The name of the entity associated with the issue, if any.
        :param code: A code that that classifies the issue. Assembly definitions can optionally permit some of these codes.
        """
        self.msg: str = msg
        self.component: str = component or 'general'
        self.code = code

    def __str__(self):
        return self.msg

    def __repr__(self):
        return self.msg


def merger(a, b):
    """
    Merges two, potentially deep, objects into a new one and returns the result.
    Conceptually, 'a' is layered over 'b' and is dominant when
    necessary. The output is 'c'.
    1. if 'a' specifies a primitive value, regardless of depth, 'c' will contain that value.
    2. if a key in 'a' specifies a list and 'b' has the same key/list, a's list will be appended to b's for c's list.
       Duplicates entries will be removed and primitive (str, int, ..) lists will be returned in sorted order).
    3. if a key ending with '!' in 'a' specifies a value, c's key (sans !) will be to set that value exactly.
    4. if a key ending with a '?' in 'a' specifies a value, c's key (sans ?) will be set to that value IF 'c' does not contain the key.
    4. if a key ending with a '-' in 'a' specifies a value, c's will not be populated with the key (sans -) regardless of 'a' or  'b's key value.
    """

    if type(a) in [bool, int, float, str, bytes, type(None)]:
        return a

    c = copy.deepcopy(b)

    if isinstance(a, Model):
        a = a.primitive()

    if type(a) is list:
        if type(c) is not list:
            return a
        for entry in a:
            if entry not in c:  # do not include duplicates
                c.append(entry)

        if c and type(c[0]) in [str, int, float]:
            return sorted(c)
        return c

    if type(a) is dict:
        if type(c) is not dict:
            return a
        for k, v in a.items():
            if k.endswith('!'):  # full dominant key
                k = k[:-1]
                c[k] = v
            elif k.endswith('?'):  # default value key
                k = k[:-1]
                if k not in c:
                    c[k] = v
            elif k.endswith('-'):  # remove key entirely
                k = k[:-1]
                c.pop(k, None)
            else:
                if k in c:
                    c[k] = merger(a[k], c[k])
                else:
                    c[k] = a[k]
        return c

    raise TypeError(f'Unexpected value type: {type(a)}: {a}')


def _check_recursion(releases_config: Model, assembly: str):
    found = []
    next_assembly = assembly
    while next_assembly and isinstance(releases_config, Model):
        if next_assembly in found:
            raise ValueError(f'Infinite recursion in {assembly} detected; {next_assembly} detected twice in chain')
        found.append(next_assembly)
        target_assembly = releases_config.releases[next_assembly].assembly
        next_assembly = target_assembly.basis.assembly


def assembly_type(releases_config: Model, assembly: typing.Optional[str]) -> AssemblyTypes:
    # If the assembly is not defined in releases.yml, it defaults to stream
    if not assembly or not isinstance(releases_config, Model):
        return AssemblyTypes.STREAM
    target_assembly = releases_config.releases[assembly].assembly
    if not target_assembly:
        return AssemblyTypes.STREAM

    str_type = _assembly_config_struct(releases_config, assembly, 'type', 'standard')
    for assem_type in AssemblyTypes:
        if str_type == assem_type.value:
            return assem_type

    raise ValueError(f'Unknown assembly type: {str_type}')


def assembly_group_config(releases_config: Model, assembly: typing.Optional[str], group_config: Model) -> Model:
    """
    Returns a group config based on the assembly information
    and the input group config.
    :param releases_config: A Model for releases.yaml.
    :param assembly: The name of the assembly
    :param group_config: The group config to merge into a new group config (original Model will not be altered)
    """
    if not assembly or not isinstance(releases_config, Model):
        return group_config

    _check_recursion(releases_config, assembly)
    target_assembly = releases_config.releases[assembly].assembly

    if target_assembly.basis.assembly:  # Does this assembly inherit from another?
        # Recursively apply ancestor assemblies
        group_config = assembly_group_config(releases_config, target_assembly.basis.assembly, group_config)

    target_assembly_group = target_assembly.group
    if not target_assembly_group:
        return group_config

    return Model(dict_to_model=merger(target_assembly_group.primitive(), group_config.primitive()))


def assembly_streams_config(releases_config: Model, assembly: typing.Optional[str], streams_config: Model) -> Model:
    """
    Returns a streams config based on the assembly information
    and the input group config.
    :param releases_config: A Model for releases.yaml.
    :param assembly: The name of the assembly
    :param streams_config: The streams config to merge into a new streams config (original Model will not be altered)
    """
    target_assembly_streams = _assembly_config_struct(releases_config, assembly, 'streams', {})
    if not target_assembly_streams:
        return streams_config

    return Model(dict_to_model=merger(target_assembly_streams.primitive(), streams_config.primitive()))


def assembly_metadata_config(releases_config: Model, assembly: typing.Optional[str], meta_type: str, distgit_key: str, meta_config: Model) -> Model:
    """
    Returns a group member's metadata configuration based on the assembly information
    and the initial file-based config.
    :param releases_config: A Model for releases.yaml.
    :param assembly: The name of the assembly
    :param meta_type: 'rpm' or 'image'
    :param distgit_key: The member's distgit_key
    :param meta_config: The meta's config object
    :return: Returns a computed config for the metadata (e.g. value for meta.config).
    """
    if not assembly or not isinstance(releases_config, Model):
        return meta_config

    _check_recursion(releases_config, assembly)
    target_assembly = releases_config.releases[assembly].assembly

    if target_assembly.basis.assembly:  # Does this assembly inherit from another?
        # Recursive apply ancestor assemblies
        meta_config = assembly_metadata_config(releases_config, target_assembly.basis.assembly, meta_type, distgit_key, meta_config)

    config_dict = meta_config.primitive()

    component_list = target_assembly.members[f'{meta_type}s']
    for component_entry in component_list:
        if component_entry.distgit_key == '*' or component_entry.distgit_key == distgit_key and component_entry.metadata:
            config_dict = merger(component_entry.metadata.primitive(), config_dict)

    return Model(dict_to_model=config_dict)


def _assembly_config_struct(releases_config: Model, assembly: typing.Optional[str], key: str, default):
    """
    If a key is directly under the 'assembly' (e.g. rhcos), then this method will
    recurse the inheritance tree to build you a final version of that key's value.
    The key may refer to a list or dict (set default value appropriately).
    """
    if not assembly or not isinstance(releases_config, Model):
        return Missing

    _check_recursion(releases_config, assembly)
    target_assembly = releases_config.releases[assembly].assembly

    if target_assembly.basis.assembly:  # Does this assembly inherit from another?
        # Recursive apply ancestor assemblies
        parent_config_struct = _assembly_config_struct(releases_config, target_assembly.basis.assembly, key, default)
        if key in target_assembly:
            key_struct = target_assembly[key]
            if hasattr(key_struct, "primitive"):
                key_struct = key_struct.primitive()
            key_struct = merger(key_struct, parent_config_struct.primitive() if hasattr(parent_config_struct, "primitive") else parent_config_struct)
        else:
            key_struct = parent_config_struct
    else:
        key_struct = target_assembly.get(key, default)
    if isinstance(default, dict):
        return Model(dict_to_model=key_struct)
    elif isinstance(default, list):
        return ListModel(list_to_model=key_struct)
    elif isinstance(default, (bool, int, float, str, bytes, type(None))):
        return key_struct
    else:
        raise ValueError(f'Unknown how to derive for default type: {type(default)}')


def assembly_rhcos_config(releases_config: Model, assembly: typing.Optional[str]) -> Model:
    """
    :param releases_config: The content of releases.yml in Model form.
    :param assembly: The name of the assembly to assess
    Returns the a computed rhcos config model for a given assembly.
    """
    return _assembly_config_struct(releases_config, assembly, 'rhcos', {})


def assembly_basis_event(releases_config: Model, assembly: typing.Optional[str]) -> typing.Optional[int]:
    """
    :param releases_config: The content of releases.yml in Model form.
    :param assembly: The name of the assembly to assess
    Returns the basis event for a given assembly. If the assembly has no basis event,
    None is returned.
    """
    if not assembly or not isinstance(releases_config, Model):
        return None

    _check_recursion(releases_config, assembly)
    target_assembly = releases_config.releases[assembly].assembly
    if target_assembly.basis.brew_event:
        return int(target_assembly.basis.brew_event)

    return assembly_basis_event(releases_config, target_assembly.basis.assembly)


def assembly_basis(releases_config: Model, assembly: typing.Optional[str]) -> Model:
    """
    :param releases_config: The content of releases.yml in Model form.
    :param assembly: The name of the assembly to assess
    Returns the basis dict for a given assembly. If the assembly has no basis,
    Model({}) is returned.
    """
    return _assembly_config_struct(releases_config, assembly, 'basis', {})


def assembly_permits(releases_config: Model, assembly: typing.Optional[str]) -> ListModel:
    """
    :param releases_config: The content of releases.yml in Model form.
    :param assembly: The name of the assembly to assess
    Returns computed permits config model for a given assembly. If no
    permits are defined ListModel([]) is returned.
    """

    defined_permits = _assembly_config_struct(releases_config, assembly, 'permits', [])

    # Do some basic validation here to fail fast
    if assembly_type(releases_config, assembly) == AssemblyTypes.STANDARD:
        if defined_permits:
            raise ValueError(f'STANDARD assemblies like {assembly} do not allow "permits"')

    for permit in defined_permits:
        if permit.code == AssemblyIssueCode.IMPERMISSIBLE.name:
            raise ValueError(f'IMPERMISSIBLE cannot be permitted in any assembly (assembly: {assembly})')
        if permit.code not in ['*', *[aic.name for aic in AssemblyIssueCode]]:
            raise ValueError(f'Undefined permit code in assembly {assembly}: {permit.code}')
    return defined_permits
