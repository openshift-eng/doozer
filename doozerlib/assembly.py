import copy

from doozerlib.model import Model


def merger(a, b):
    """
    Merges two, potentially deep, objects into a new one and returns the result.
    Conceptually, 'a' is layered over 'b' and is dominant when
    necessary. The output is 'c'.
    1. if 'a' specifies a primitive value, regardless of depth, 'c' will contain that value.
    2. if a key in 'a' specifies a list and 'b' has the same key/list, a's list will be appended to b's for c's list.
       Duplicates entries will be removed and primitive (str, int, ..) lists will be returned in sorted order).
    3. if a key ending with '!' in 'a' specifies a value, c's key-! will be to set that value exactly.
    4. if a key ending with a '?' in 'a' specifies a value, c's key-? will be set to that value is 'c' does not contain the key.
    """

    if type(a) in [bool, int, float, str, bytes, type(None)]:
        return a

    c = copy.deepcopy(b)

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
            else:
                if k in c:
                    c[k] = merger(a[k], c[k])
                else:
                    c[k] = a[k]
        return c

    raise IOError(f'Unexpected value type: {type(a)}: {a}')


def group_for_assembly(releases_config: Model, assembly: str, group_config: Model):
    """
    Returns a group config based on the assembly information
    and the input group config.
    :param releases_config: A Model for releases.yaml.
    :param assembly: The name of the assembly
    :param group_config: The group config to merge into a new group config (original Model will not be altered)
    """
    if not assembly or not isinstance(releases_config, Model):
        return group_config

    target_assembly = releases_config.releases[assembly].assembly

    if target_assembly.basis.assembly:  # Does this assembly inherit from another?
        # Recursive apply ancestor assemblies
        group_config = group_for_assembly(releases_config, target_assembly.basis.assembly, group_config)

    target_assembly_group = target_assembly.group
    if not target_assembly_group:
        return group_config

    return Model(dict_to_model=merger(target_assembly_group.primitive(), group_config.primitive()))


def metadata_config_for_assembly(releases_config: Model, assembly: str, meta_type: str, distgit_key: str, meta_config: Model):
    """
    Returns a group config based on the assembly information
    and the input group config.
    :param releases_config: A Model for releases.yaml.
    :param assembly: The name of the assembly
    :param meta_type: 'rpm' or 'image'
    :param distgit_key: The name of the underlying component
    :param meta_config: The meta's config object
    :return: Returns a computed config for the metadata (e.g. value for meta.config).
    """
    if not assembly or not isinstance(releases_config, Model):
        return meta_config

    target_assembly = releases_config.releases[assembly].assembly

    if target_assembly.basis.assembly:  # Does this assembly inherit from another?
        # Recursive apply ancestor assemblies
        meta_config = metadata_config_for_assembly(releases_config, target_assembly.basis.assembly, meta_type, distgit_key, meta_config)

    config_dict = meta_config.primitive()

    component_list = target_assembly.members[f'{meta_type}s']
    for component_entry in component_list:
        if component_entry.distgit_key == '*' or component_entry.distgit_key == distgit_key:
            config_dict = merger(component_entry.metadata.primitive(), config_dict)

    return Model(dict_to_model=config_dict)
