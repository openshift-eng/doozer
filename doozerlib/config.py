from __future__ import absolute_import, print_function, unicode_literals
from future.utils import bytes_to_native_str
from . import metadata
import yaml
import os
import shutil
from .pushd import Dir
from . import exectools
import sys
import csv
import io


VALID_UPDATES = {
    'mode': metadata.CONFIG_MODES,
}


class MetaDataConfig(object):
    """
    Holds common functions for managing the MetaData configs
    Mostly is a class to hold runtime
    """
    def __init__(self, runtime):
        self.runtime = runtime
        if self.runtime.remove_tmp_working_dir:
            print('config:* options require a non-temporary working space. Must run with --working-dir')
            sys.exit(1)

    def update_meta(self, meta, k, v):
        """
        Convenience function for setting meta keys
        """
        self.runtime.logger.info('{}: [{}] -> {}'.format(meta.config_filename, k, v))
        meta.config[k] = v
        meta.save()

    def delete_key(self, meta, k):
        """
        Convenience function for deleting meta keys
        """

        self.runtime.logger.info('{}: Delete [{}]'.format(meta.config_filename, k, ))
        meta.config.pop(k, None)
        meta.save()

    def update(self, key, val):
        """
        Update [key] to [val] in all given image/rpm metas
        VALID_UPDATES is used to lock out what can be updated
        Right now only [mode] is valid, but that may change
        """
        if key not in VALID_UPDATES:
            raise ValueError('{} is not a valid update key. See --help'.format(key))

        if VALID_UPDATES[key]:
            if val not in VALID_UPDATES[key]:
                msg = '{} is not a valid value for {}. Use one of: {}'.format(val, key, ','.join(VALID_UPDATES[key]))
                raise ValueError(msg)

        for img in self.runtime.image_metas():
            self.update_meta(img, key, val)

        for rpm in self.runtime.rpm_metas():
            self.update_meta(rpm, key, val)

    def config_print(self, key=None, name_only=False, as_yaml=False):
        """
        Print name, sub-key, or entire config
        """

        data = dict(images={}, rpms={})

        def _collect_data(metas, output):
            for meta in metas:
                if name_only:
                    entry = meta.config_filename
                elif key:
                    entry = meta.config.get(key, None)
                else:
                    entry = meta.config.primitive()
                output[meta.config_filename.replace('.yml', '')] = entry

        _collect_data(self.runtime.image_metas(), data["images"])
        _collect_data(self.runtime.rpm_metas(), data["rpms"])

        if as_yaml:
            print(yaml.safe_dump(data, default_flow_style=False))
            return

        def _do_print(kind, output):
            if not output:
                return
            print('')
            print('********* {} *********'.format(kind))
            for name, entry in output.items():
                if name_only:
                    print(entry)
                else:
                    print("*****{}.yml*****".format(name))
                    print(yaml.safe_dump(entry, default_flow_style=False))
                    print('')

        _do_print("Images", data['images'])
        _do_print(" RPMS ", data['rpms'])

    def config_gen_csv(self, keys, as_type=None, output=None):
        """
        Generate csv file or print it out to STDOUT
        """

        if keys is None:
            print('No --keys specified, please make sure you have at least one key to generate')
            return

        # split --keys=a,b,c,d to list [a,b,c,d]
        keys = [c.strip() for c in keys.split(',')]

        image_list = None
        if as_type == "image" and not self.runtime.rpms:
            image_list = self.runtime.image_metas()
        if as_type == "rpm" and not self.runtime.images:
            image_list = self.runtime.rpm_metas()

        if image_list is None:
            print('Not correct --type specified (--type image or --type rpm). '
                  'Or not consistent with global options: --images/-i and --rpms/-r')
            return

        def _write_rows(w):
            # write header
            w.writerow(keys)
            # write values
            for value in image_list:
                value_list = []
                for k in keys:
                    if k == "key":
                        value_list.append(value.config_filename)
                    else:
                        value_list.append(value.config.get(k, None))
                w.writerow(value_list)

        if output is None:
            writer = csv.writer(sys.stdout, delimiter=bytes_to_native_str(b','), quotechar=bytes_to_native_str(b'"'), quoting=csv.QUOTE_MINIMAL)
            _write_rows(writer)
            return

        with io.open(output, mode='w', encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file, delimiter=bytes_to_native_str(b','), quotechar=bytes_to_native_str(b'"'), quoting=csv.QUOTE_MINIMAL)
            _write_rows(writer)

    def commit(self, msg):
        """
        Commit outstanding metadata config changes
        """
        self.runtime.gitdata.commit(msg)

    def push(self):
        """
        Push changes back to config repo.
        Will of course fail if user does not have write access.
        """
        self.runtime.gitdata.push()
