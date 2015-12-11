# Copyright 2015 Mirantis Inc.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import os
import re

from oslo_log import log
import six

from manila.common import constants as const
from manila import exception
from manila.i18n import _
from manila.i18n import _LW
from manila import utils

LOG = log.getLogger(__name__)


class NASHelperBase(object):
    """Interface to work with share."""

    def __init__(self, execute, ssh_execute, config_object):
        self.configuration = config_object
        self._execute = execute
        self._ssh_exec = ssh_execute

    def init_helper(self, server):
        pass

    def create_export(self, server, share_name, recreate=False):
        """Create new export, delete old one if exists."""
        raise NotImplementedError()

    def remove_export(self, server, share_name):
        """Remove export."""
        raise NotImplementedError()

    def configure_access(self, server, share_name):
        """Configure server before allowing access."""
        pass

    def update_access(self, server, share_name, access_rules, add_rules=None,
                      delete_rules=None):
        """Update access rules list."""
        raise NotImplementedError()

    @staticmethod
    def _verify_server_has_public_address(server):
        if 'public_address' not in server:
            raise exception.ManilaException(
                _("Can not get 'public_address' for generation of export."))

    def get_exports_for_share(self, server, old_export_location):
        """Returns list of exports based on server info."""
        raise NotImplementedError()

    def get_share_path_by_export_location(self, server, export_location):
        """Returns share path by its export location."""
        raise NotImplementedError()

    def disable_access_for_maintenance(self, server, share_name):
        """Disables access to share to perform maintenance operations."""

    def restore_access_after_maintenance(self, server, share_name):
        """Enables access to share after maintenance operations were done."""

    def _get_maintenance_file_path(self, share_name):
        return os.path.join(self.configuration.share_mount_path,
                            "%s.maintenance" % share_name)


def nfs_synchronized(f):

    def wrapped_func(self, *args, **kwargs):
        key = "nfs-%s" % args[0]["instance_id"]

        @utils.synchronized(key)
        def source_func(self, *args, **kwargs):
            return f(self, *args, **kwargs)

        return source_func(self, *args, **kwargs)

    return wrapped_func


class NFSHelper(NASHelperBase):
    """Interface to work with share."""

    def create_export(self, server, share_name, recreate=False):
        """Create new export, delete old one if exists."""
        return ':'.join([server['public_address'],
                         os.path.join(
                             self.configuration.share_mount_path, share_name)])

    def init_helper(self, server):
        try:
            self._ssh_exec(server, ['sudo', 'exportfs'])
        except exception.ProcessExecutionError as e:
            if 'command not found' in e.stderr:
                raise exception.ManilaException(
                    _('NFS server is not installed on %s')
                    % server['instance_id'])
            LOG.error(e.stderr)

    def remove_export(self, server, share_name):
        """Remove export."""

    def _check_valid_access(self, access_rules):
        for access in access_rules:
            access_type = access['access_type']
            if access_type != 'ip':
                reason = _("Only IP access type allowed.")
                raise exception.InvalidShareAccess(reason)

    @nfs_synchronized
    def update_access(self, server, share_name, access_rules, add_rules=None,
                      delete_rules=None):
        """Update access rules list."""
        add_rules, delete_rules = convert_none_to_list(add_rules, delete_rules)
        local_path = os.path.join(self.configuration.share_mount_path,
                                  share_name)
        out, err = self._ssh_exec(server, ['sudo', 'exportfs'])
        if not (add_rules or delete_rules):
            self._check_valid_access(access_rules)
            hosts = self._get_host_list(out, local_path)
            for host in hosts:
                self._ssh_exec(server, ['sudo', 'exportfs', '-u',
                                        ':'.join([host, local_path])])
            self._sync_nfs_temp_and_perm_files(server)
            for access in access_rules:
                self._ssh_exec(
                    server,
                    ['sudo', 'exportfs', '-o',
                     '%s,no_subtree_check' % access['access_level'],
                     ':'.join([access['access_to'], local_path])])
        else:
            self._check_valid_access(add_rules)
            for access in add_rules:
                access_to, access_type = (access['access_to'],
                                          access['access_type'])
                found_item = re.search(
                    re.escape(local_path) + '[\s\n]*' + re.escape(access_to),
                    out)
                if found_item is not None:
                    raise exception.ShareAccessExists(access_type=access_type,
                                                      access=access_to)
            for access in add_rules:
                self._ssh_exec(
                    server,
                    ['sudo', 'exportfs', '-o',
                     '%s,no_subtree_check' % access['access_level'],
                     ':'.join([access['access_to'], local_path])])
            for access in delete_rules:
                self._ssh_exec(server, ['sudo', 'exportfs', '-u',
                               ':'.join([access['access_to'], local_path])])
        self._sync_nfs_temp_and_perm_files(server)

    def _get_host_list(self, output, local_path):
        entries = []
        output = output.replace('\n\t\t', ' ')
        lines = output.split('\n')
        for line in lines:
            items = line.split(' ')
            if local_path == items[0]:
                entries.append(items[1])
        return entries

    def _sync_nfs_temp_and_perm_files(self, server):
        """Sync changes of exports with permanent NFS config file.

        This is required to ensure, that after share server reboot, exports
        still exist.
        """
        sync_cmd = [
            'sudo', 'cp', const.NFS_EXPORTS_FILE_TEMP, const.NFS_EXPORTS_FILE
        ]
        self._ssh_exec(server, sync_cmd)
        self._ssh_exec(server, ['sudo', 'exportfs', '-a'])

    def get_exports_for_share(self, server, old_export_location):
        self._verify_server_has_public_address(server)
        path = old_export_location.split(':')[-1]
        return [':'.join([server['public_address'], path])]

    def get_share_path_by_export_location(self, server, export_location):
        return export_location.split(':')[-1]

    @nfs_synchronized
    def disable_access_for_maintenance(self, server, share_name):
        maintenance_file = self._get_maintenance_file_path(share_name)
        backup_exports = [
            'cat', const.NFS_EXPORTS_FILE,
            '| grep', share_name,
            '| sudo tee', maintenance_file
        ]
        self._ssh_exec(server, backup_exports)

        local_path = os.path.join(self.configuration.share_mount_path,
                                  share_name)
        self._ssh_exec(server, ['sudo', 'exportfs', '-u', local_path])
        self._sync_nfs_temp_and_perm_files(server)

    @nfs_synchronized
    def restore_access_after_maintenance(self, server, share_name):
        maintenance_file = self._get_maintenance_file_path(share_name)
        restore_exports = [
            'cat', maintenance_file,
            '| sudo tee -a', const.NFS_EXPORTS_FILE,
            '&& sudo exportfs -r',
            '&& sudo rm -f', maintenance_file
        ]
        self._ssh_exec(server, restore_exports)


class CIFSHelperIPAccess(NASHelperBase):
    """Manage shares in samba server by net conf tool.

    Class provides functionality to operate with CIFS shares.
    Samba server should be configured to use registry as configuration
    backend to allow dynamically share managements. This class allows
    to define access to shares by IPs with RW access level.
    """
    def __init__(self, *args):
        super(CIFSHelperIPAccess, self).__init__(*args)
        self.export_format = '\\\\%s\\%s'
        self.parameters = {
            'browseable': 'yes',
            '\"create mask\"': '0755',
            '\"hosts deny\"': '0.0.0.0/0',  # deny all by default
            '\"hosts allow\"': '127.0.0.1',
            '\"read only\"': 'no',
        }

    def init_helper(self, server):
        # This is smoke check that we have required dependency
        self._ssh_exec(server, ['sudo', 'net', 'conf', 'list'])

    def create_export(self, server, share_name, recreate=False):
        """Create share at samba server."""
        share_path = os.path.join(self.configuration.share_mount_path,
                                  share_name)
        create_cmd = [
            'sudo', 'net', 'conf', 'addshare', share_name, share_path,
            'writeable=y', 'guest_ok=y',
        ]
        try:
            self._ssh_exec(
                server, ['sudo', 'net', 'conf', 'showshare', share_name, ])
        except exception.ProcessExecutionError as parent_e:
            # Share does not exist, create it
            try:
                self._ssh_exec(server, create_cmd)
            except Exception as child_e:
                # If we get here, then it will be useful
                # to log parent exception too.
                error = six.text_type(child_e)
                LOG.exception(error)
                LOG.error(parent_e)
                raise exception.ManilaException(reason=error)
        else:
            # Share exists
            if recreate:
                self._ssh_exec(
                    server, ['sudo', 'net', 'conf', 'delshare', share_name, ])
                self._ssh_exec(server, create_cmd)
            else:
                msg = _('Share section %s already defined.') % share_name
                raise exception.ShareBackendException(msg=msg)

        for param, value in self.parameters.items():
            self._ssh_exec(server, ['sudo', 'net', 'conf', 'setparm',
                           share_name, param, value])

        return self.export_format % (server['public_address'], share_name)

    def remove_export(self, server, share_name):
        """Remove share definition from samba server."""
        try:
            self._ssh_exec(
                server, ['sudo', 'net', 'conf', 'delshare', share_name])
        except exception.ProcessExecutionError as e:
            LOG.warning(_LW("Caught error trying delete share: %(error)s, try"
                            "ing delete it forcibly."), {'error': e.stderr})
            self._ssh_exec(server, ['sudo', 'smbcontrol', 'all', 'close-share',
                                    share_name])

    def _check_valid_access(self, access_rules):
        for access in access_rules:
            access_type = access['access_type']
            access_level = access['access_level']

            if access_type != 'ip':
                reason = _('Only IP access type allowed.')
                raise exception.InvalidShareAccess(reason)
            if access_level != const.ACCESS_LEVEL_RW:
                raise exception.InvalidShareAccessLevel(level=access_level)

    def update_access(self, server, share_name, access_rules, add_rules=None,
                      delete_rules=None):
        """Update IP access rules list."""
        add_rules, delete_rules = convert_none_to_list(add_rules, delete_rules)
        hosts = []
        if not (add_rules or delete_rules):
            self._check_valid_access(access_rules)
            for access in access_rules:
                hosts.append(access['access_to'])
        else:
            self._check_valid_access(add_rules)
            hosts = self._get_allow_hosts(server, share_name)
            for access in add_rules:
                if access['access_to'] in hosts:
                    raise exception.ShareAccessExists(
                        access_type=access['access_type'],
                        access=access['access_to'])
                hosts.append(access['access_to'])
            for access in delete_rules:
                if access['access_to'] in hosts:
                    # Access rule can be in error state, if so
                    # it can be absent in rules, hence - skip removal.
                    hosts.remove(access['access_to'])
        self._set_allow_hosts(server, hosts, share_name)

    def _get_allow_hosts(self, server, share_name):
        (out, _) = self._ssh_exec(server, ['sudo', 'net', 'conf', 'getparm',
                                           share_name, '\"hosts allow\"'])
        return out.split()

    def _set_allow_hosts(self, server, hosts, share_name):
        value = "\"" + ' '.join(hosts) + "\""
        self._ssh_exec(server, ['sudo', 'net', 'conf', 'setparm', share_name,
                                '\"hosts allow\"', value])

    @staticmethod
    def _get_share_group_name_from_export_location(export_location):
        if '/' in export_location and '\\' in export_location:
            pass
        elif export_location.startswith('\\\\'):
            return export_location.split('\\')[-1]
        elif export_location.startswith('//'):
            return export_location.split('/')[-1]

        msg = _("Got incorrect CIFS export location '%s'.") % export_location
        raise exception.InvalidShare(reason=msg)

    def get_exports_for_share(self, server, old_export_location):
        self._verify_server_has_public_address(server)
        group_name = self._get_share_group_name_from_export_location(
            old_export_location)
        data = dict(ip=server['public_address'], share=group_name)
        return ['\\\\%(ip)s\\%(share)s' % data]

    def get_share_path_by_export_location(self, server, export_location):
        # Get name of group that contains share data on CIFS server
        group_name = self._get_share_group_name_from_export_location(
            export_location)

        # Get parameter 'path' from group that belongs to current share
        (out, __) = self._ssh_exec(
            server, ['sudo', 'net', 'conf', 'getparm', group_name, 'path'])

        # Remove special symbols from response and return path
        return out.strip()

    def disable_access_for_maintenance(self, server, share_name):
        maintenance_file = self._get_maintenance_file_path(share_name)
        allowed_hosts = " ".join(self._get_allow_hosts(server, share_name))

        backup_exports = [
            'echo', "'%s'" % allowed_hosts, '| sudo tee', maintenance_file
        ]
        self._ssh_exec(server, backup_exports)
        self._set_allow_hosts(server, [], share_name)

    def restore_access_after_maintenance(self, server, share_name):
        maintenance_file = self._get_maintenance_file_path(share_name)
        (exports, __) = self._ssh_exec(server, ['cat', maintenance_file])
        self._set_allow_hosts(server, exports.split(), share_name)
        self._ssh_exec(server, ['sudo rm -f', maintenance_file])


class CIFSHelperUserAccess(CIFSHelperIPAccess):
    """Manage shares in samba server by net conf tool.

    Class provides functionality to operate with CIFS shares.
    Samba server should be configured to use registry as configuration
    backend to allow dynamically share managements. This class allows
    to define access to shares by usernames with either RW or RO access levels.
    """
    def __init__(self, *args):
        super(CIFSHelperUserAccess, self).__init__(*args)
        self.export_format = '//%s/%s'
        self.parameters = {
            'browseable': 'yes',
            'create mask': '0755',
            'hosts allow': '0.0.0.0/0',
            'read only': 'no',
        }

    def _check_valid_access(self, access_rules):
        for access in access_rules:
            access_type = access['access_type']
            if access_type != 'user':
                reason = _('Only user access type allowed.')
                raise exception.InvalidShareAccess(reason=reason)

    def update_access(self, server, share_name, access_rules, add_rules=None,
                      delete_rules=None):
        """Update user access rules list."""
        add_rules, delete_rules = convert_none_to_list(add_rules, delete_rules)
        all_users_rw = []
        all_users_ro = []
        if not (add_rules or delete_rules):
            self._check_valid_access(access_rules)
            for access in access_rules:
                if access['access_level'] == const.ACCESS_LEVEL_RW:
                    all_users_rw.append(access['access_to'])
                else:
                    all_users_ro.append(access['access_to'])
        else:
            self._check_valid_access(add_rules)
            all_users_rw = self._get_valid_users(server, share_name,
                                                 const.ACCESS_LEVEL_RW)
            all_users_ro = self._get_valid_users(server, share_name,
                                                 const.ACCESS_LEVEL_RO)
            all_users = all_users_ro + all_users_rw
            for access in add_rules:
                if access['access_to'] in all_users:
                    raise exception.ShareAccessExists(
                        access_type=access['access_type'],
                        access=access['access_to'])
                if access['access_level'] == const.ACCESS_LEVEL_RW:
                    all_users_rw.append(access['access_to'])
                else:
                    all_users_ro.append(access['access_to'])
            for access in delete_rules:
                # Access rule can be in error state, if so
                # it can be absent in rules, hence - skip removal.
                if access['access_to'] in all_users_rw:
                    all_users_rw.remove(access['access_to'])
                if access['access_to'] in all_users_ro:
                    all_users_ro.remove(access['access_to'])
        self._set_allow_hosts(server, all_users_rw, share_name)
        self._set_allow_hosts(server, all_users_ro, share_name)

    def _get_valid_users(self, server, share_name, access_level, force=True):
        param = self._get_conf_param(access_level)
        try:
            (out, _) = self._ssh_exec(server, ['sudo', 'net', 'conf',
                                               'getparm', share_name, param])
            out = out.replace("\"", "")
            return out.split()
        except exception.ProcessExecutionError:
            if not force:
                raise
            return []

    def _get_conf_param(self, access_level):
        if access_level == const.ACCESS_LEVEL_RW:
            return 'valid users'
        else:
            return 'read list'

    def _set_valid_users(self, server, users, share_name, access_level):
        value = "\"" + ' '.join(users) + "\""
        param = self._get_conf_param(access_level)
        self._ssh_exec(server, ['sudo', 'net', 'conf', 'setparm', share_name,
                                param, value])


def convert_none_to_list(list1, list2):
    if list1 is None:
        list1 = []
    if list2 is None:
        list2 = []
    return list1, list2
