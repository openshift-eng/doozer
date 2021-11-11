import typing
import copy

from enum import Enum

from doozerlib.model import Missing, Model, ListModel


class AssemblyTypes(Enum):
    STREAM = "stream"  # Default assembly type - indicates continuous build and no basis event
    STANDARD = "standard"  # All constraints / checks enforced (e.g. consistent RPMs / siblings)
    CANDIDATE = "candidate"  # Indicates release candidate or feature candidate
    CUSTOM = "custom"  # No constraints enforced


class AssemblyIssueCode(Enum):
    IMPERMISSIBLE = 0
    CONFLICTING_INHERITED_DEPENDENCY = 1
    CONFLICTING_GROUP_RPM_INSTALLED = 2
    MISMATCHED_SIBLINGS = 3
    OUTDATED_RPMS_IN_STREAM_BUILD = 4
    INCONSISTENT_RHCOS_RPMS = 5


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

    if not assembly or not isinstance(releases_config, Model):
        return AssemblyTypes.STREAM

    target_assembly = releases_config.releases[assembly].assembly

    if not target_assembly:
        # If the assembly is not defined in releases.yml, it defaults to stream
        return AssemblyTypes.STREAM

    str_type = target_assembly['type']
    if not str_type:
        # Assemblies which are defined in releases.yml default to standard
        return AssemblyTypes.STANDARD

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
    if not assembly or not isinstance(releases_config, Model):
        return streams_config

    _check_recursion(releases_config, assembly)
    target_assembly = releases_config.releases[assembly].assembly

    if target_assembly.basis.assembly:  # Does this assembly inherit from another?
        # Recursively apply ancestor assemblies
        streams_config = assembly_streams_config(releases_config, target_assembly.basis.assembly, streams_config)

    target_assembly_streams = target_assembly.group
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
    key_struct = target_assembly.get(key, default)
    if target_assembly.basis.assembly:  # Does this assembly inherit from another?
        # Recursive apply ancestor assemblies
        parent_config_struct = _assembly_config_struct(releases_config, target_assembly.basis.assembly, key, default)
        key_struct = merger(key_struct, parent_config_struct.primitive())
    if isinstance(default, dict):
        return Model(dict_to_model=key_struct)
    elif isinstance(default, list):
        return ListModel(list_to_model=key_struct)
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
    Returns the a computed permits config model for a given assembly. If no
    permits are defined ListModel([]) is returned.
    """

    defined_permits = _assembly_config_struct(releases_config, assembly, 'permits', [])

    if not defined_permits and (assembly == 'stream' or not assembly):  # If assembly is None, this a group without assemblies enabled
        # TODO: Address this formally with https://issues.redhat.com/browse/ART-3162 .
        # In the short term, we need to allow certain inconsistencies for stream.
        # We don't want common pre-GA issues to stop all nightlies.
        default_stream_permits = [
            {
                'code': 'OUTDATED_RPMS_IN_STREAM_BUILD',
                'component': '*'
            },
            {
                'code': 'CONFLICTING_GROUP_RPM_INSTALLED',
                'component': 'rhcos'
            }
        ]
        return ListModel(list_to_model=default_stream_permits)

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
