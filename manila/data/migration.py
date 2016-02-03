# Copyright (c) 2015 Hitachi Data Systems.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
"""Helper class for Share Migration."""

from oslo_config import cfg
from oslo_log import log
import six

from manila.common import constants
from manila.i18n import _LE
from manila.share import rpcapi as share_rpc
from manila import utils

LOG = log.getLogger(__name__)

migration_opts = [
    cfg.IntOpt(
        'migration_wait_access_rules_timeout',
        default=90,
        help="Time to wait for access rules to be allowed/denied on backends "
             "when migrating shares using generic approach (seconds)."),
]

CONF = cfg.CONF
CONF.register_opts(migration_opts)


class ShareMigrationHelper(object):

    def __init__(self, context, db, share, share_instance_id,
                 new_share_instance_id):

        self.db = db
        self.share = share
        self.context = context
        self.share_rpc = share_rpc.ShareAPI()
        self.share_instance = self.db.share_instance_get(
            self.context, share_instance_id, with_share_data=True)
        self.new_share_instance = self.db.share_instance_get(
            self.context, new_share_instance_id, with_share_data=True)
        self.migration_wait_access_rules_timeout = (
            CONF.migration_wait_access_rules_timeout)

    def allow_migration_access(self, access):

        values = {
            'share_id': self.share['id'],
            'access_type': access['access_type'],
            'access_level': access['access_level'],
            'access_to': access['access_to']
        }

        share_access_list = self.db.share_access_get_all_by_type_and_access(
            self.context, self.share['id'], access['access_type'],
            access['access_to'])

        if len(share_access_list) > 0:
            for access in share_access_list:
                self._change_migration_access_to_instance(
                    self.share_instance, access, allow=False)

        access_ref = self.db.share_access_create(self.context, values)

        self._change_migration_access_to_instance(
            self.share_instance, access_ref, allow=True)
        self._change_migration_access_to_instance(
            self.new_share_instance, access_ref, allow=True)

        return access_ref

    def deny_migration_access(self, access_ref):

        self._change_migration_access_to_instance(
            self.share_instance, access_ref, allow=False)
        self._change_migration_access_to_instance(
            self.new_share_instance, access_ref, allow=False)

    # NOTE(ganso): Cleanup methods do not throw exceptions, since the
    # exceptions that should be thrown are the ones that call the cleanup

    def cleanup_migration_access(self, access_ref):

        try:
            self.deny_migration_access(access_ref)
        except Exception as mae:
            LOG.exception(six.text_type(mae))
            LOG.error(_LE("Could not cleanup access rule of share "
                          "%s") % self.share['id'])

    def cleanup_temp_folder(self, instance_id, mount_path):

        try:
            utils.execute('rmdir', mount_path + instance_id,
                          check_exit_code=False)

        except Exception as tfe:
            LOG.exception(six.text_type(tfe))
            LOG.error(_LE("Could not cleanup instance %(instance_id)s "
                          "temporary folders for migration of "
                          "share %(share_id)s") % {
                              'instance_id': instance_id,
                              'share_id': self.share['id']})

    def cleanup_unmount_temp_folder(self, instance_id, migration_info, path):

        try:
            migration_info['umount'].append(path)
            utils.execute(*migration_info['umount'], run_as_root=True)
        except Exception as utfe:
            LOG.exception(six.text_type(utfe))
            LOG.error(_LE("Could not unmount folder of instance"
                          " %(instance_id)s for migration of "
                          "share %(share_id)s") % {
                              'instance_id': instance_id,
                              'share_id': self.share['id']})

    def _change_migration_access_to_instance(
            self, instance, access_ref, allow=False):

        self.db.share_instance_update_access_status(
            self.context, instance['id'], constants.STATUS_OUT_OF_SYNC)

        if allow:
            self.share_rpc.allow_access(self.context, instance, access_ref)
        else:
            self.share_rpc.deny_access(self.context, instance, access_ref)

        utils.wait_for_access_update(
            self.context, self.db, instance,
            self.migration_wait_access_rules_timeout)
