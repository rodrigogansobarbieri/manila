# Copyright 2015, Hitachi Data Systems.
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

"""
Data Service
"""

import time

from oslo_config import cfg
from oslo_log import log
import six

from manila.i18n import _, _LE, _LI
from manila.common import constants
from manila import context
from manila.data import migration
from manila.data import utils as data_utils
from manila import exception
from manila import manager
from manila.share import rpcapi as share_rpc
from manila import utils

LOG = log.getLogger(__name__)

data_opts = [
    cfg.StrOpt(
        'migration_tmp_location',
        default='/tmp/',
        help="Temporary path to create and mount shares during migration."),
    cfg.StrOpt(
        'migration_data_node_ip',
        default=None,
        help="The IP of the node interface connected to the admin network. "
             "Used for allowing access to the mounting shares."),
    cfg.StrOpt(
        'migration_data_node_cert',
        default=None,
        help="The certificate installed in the data node in order to "
             "allow access to certificate authentication-based shares."),

]

CONF = cfg.CONF
CONF.register_opts(data_opts)


class DataManager(manager.Manager):
    """Receives requests to handle data and sends responses."""

    RPC_API_VERSION = '1.2'

    def __init__(self, service_name=None, *args, **kwargs):
        super(DataManager, self).__init__(*args, **kwargs)
        self.busy_tasks_shares = {}

    def init_host(self):
        ctxt = context.get_admin_context()
        shares = self.db.share_get_all(ctxt)
        for share in shares:
            if share['task_state'] in constants.BUSY_COPYING_STATES:
                self.db.share_update(ctxt, share['id'], {
                    'task_state': constants.TASK_STATE_MIGRATION_ERROR
                })

    def migration_cancel(self, context, share_id):
        LOG.info(_LI("Received request to cancel share migration "
                     "of share %s.") % share_id)
        copy = self.busy_tasks_shares.get(share_id)
        if copy:
            copy.cancel()
        else:
            msg = _("Data copy for migration of share %s cannot be cancelled"
                    " at this moment.") % share_id
            LOG.error(msg)
            raise exception.InvalidShare(reason=msg)

    def migration_get_progress(self, context, share_id):
        LOG.info(_LI("Received request to get share migration information "
                     "of share %s.") % share_id)
        copy = self.busy_tasks_shares.get(share_id)
        if copy:
            result = copy.get_progress()
            LOG.info(_LI("Obtained following share migration information "
                         "of share %(share)s: %(info)s.") % {
                'share': share_id,
                'info': six.text_type(result)})
            return result
        else:
            msg = _("Migration of share %s data copy progress cannot be "
                    "obtained at this moment.") % share_id
            LOG.error(msg)
            raise exception.InvalidShare(reason=msg)

    def migrate_share(self, context, ignore_list, share_id, share_instance_id,
                      new_share_instance_id, migration_info_src,
                      migration_info_dest, notify):

        LOG.info(_LI(
            "Received request to migrate share content from share instance "
            "%(instance_id)s to instance %(new_instance_id)s.")
            % {'instance_id': share_instance_id,
               'new_instance_id': new_share_instance_id})

        self.db.share_update(context, share_id, {
            'task_state':
                constants.TASK_STATE_MIGRATION_COPYING_STARTING
        })

        share_ref = self.db.share_get(context, share_id)

        helper = migration.ShareMigrationHelper(
            context, self.db, share_ref, share_instance_id,
            new_share_instance_id)

        share_rpcapi = share_rpc.ShareAPI()

        try:
            self._copy_share_data(
                context, helper, ignore_list, share_ref, share_instance_id,
                new_share_instance_id, migration_info_src, migration_info_dest)
        except exception.ShareMigrationCancelled:
            share_rpcapi.migration_complete(
                context, share_ref, share_instance_id, new_share_instance_id,
                constants.TASK_STATE_MIGRATION_CANCELLED)
            return
        except Exception as e:
            error = six.text_type(e)
            LOG.exception(error)
            share_rpcapi.migration_complete(
                context, share_ref, share_instance_id, new_share_instance_id,
                constants.TASK_STATE_MIGRATION_ERROR)
            raise exception.ShareMigrationFailed(reason=error)
        finally:
            if self.busy_tasks_shares.get(share_id):
                self.busy_tasks_shares.pop(share_id)

        LOG.info(_LI(
            "Completed copy operation of migrating share content from share "
            "instance %(instance_id)s to instance %(new_instance_id)s.")
            % {'instance_id': share_instance_id,
                'new_instance_id': new_share_instance_id})

        self.db.share_update(context, share_id, {
            'task_state':
                constants.TASK_STATE_MIGRATION_COPYING_COMPLETED
        })

        if notify:
            LOG.info(_LI(
                "Notifying source backend that migrating share content from"
                " share instance %(instance_id)s to instance "
                "%(new_instance_id)s completed.") % {
                    'instance_id': share_instance_id,
                    'new_instance_id': new_share_instance_id})

            share_rpcapi.migration_complete(
                context, share_ref, share_instance_id, new_share_instance_id,
                None)

    def _copy_share_data(self, context, helper, ignore_list, share,
                         share_instance_id, new_share_instance_id,
                         migration_info_src, migration_info_dest):

        migrated = False
        mount_path = CONF.migration_tmp_location

        if share['share_proto'].upper() == 'GLUSTERFS':

            access_to = CONF.migration_data_node_cert
            access_type = 'cert'

            if not access_to:
                msg = _("Data Node Certificate not specified. Cannot mount "
                        "instances for migration of share %(share_id)s. "
                        "Aborting.") % {
                    'share_id': share['id']}
                raise exception.ShareMigrationFailed(reason=msg)

        else:

            access_to = CONF.migration_data_node_ip
            access_type = 'ip'

            if not access_to:
                msg = _("Data Node Admin Network IP not specified. Cannot "
                        "mount instances for migration of share %(share_id)s. "
                        "Aborting.") % {
                    'share_id': share['id']}
                raise exception.ShareMigrationFailed(reason=msg)

        access = {'access_type': access_type,
                  'access_level': 'rw',
                  'access_to': access_to}

        src_path = ''.join((mount_path, share_instance_id))
        dest_path = ''.join((mount_path, new_share_instance_id))

        try:
            access_ref = helper.allow_migration_access(access)
        except Exception as e:
            LOG.error(_LE("Share migration failed attempting to allow "
                          "access %(access)s to share %(share_id)s.") % {
                'access': access,
                'share_id': share['id']})
            msg = six.text_type(e)
            LOG.exception(msg)
            raise exception.ShareMigrationFailed(reason=msg)

        def _mount_for_migration(migration_info, path):

            migration_info['mount'].append(path)

            try:
                utils.execute(*migration_info['mount'], run_as_root=True)
            except Exception:
                LOG.error(_LE("Failed to mount temporary folder for "
                              "migration of share instance "
                              "%(share_instance_id)s "
                              "to %(new_share_instance_id)s") % {
                    'share_instance_id': share_instance_id,
                    'new_share_instance_id': new_share_instance_id})
                helper.cleanup_migration_access(access_ref)
                raise

        def _run_unmount_command(migration_info, path):

            migration_info['umount'].append(path)
            utils.execute(*migration_info['umount'], run_as_root=True)

        utils.execute('mkdir', '-p', src_path)

        utils.execute('mkdir', '-p', dest_path)

        # NOTE(ganso): mkdir command sometimes returns faster than it
        # actually runs, so we better sleep for 1 second.

        time.sleep(1)

        try:
            _mount_for_migration(migration_info_src, src_path)
        except Exception as e:
            LOG.error(_LE("Share migration failed attempting to mount "
                          "share instance %s.") % share_instance_id)
            msg = six.text_type(e)
            LOG.exception(msg)
            helper.cleanup_temp_folder(share_instance_id, mount_path)
            helper.cleanup_temp_folder(new_share_instance_id, mount_path)
            raise exception.ShareMigrationFailed(reason=msg)

        try:
            _mount_for_migration(migration_info_dest, dest_path)
        except Exception as e:
            LOG.error(_LE("Share migration failed attempting to mount "
                          "share instance %s.") % new_share_instance_id)
            msg = six.text_type(e)
            LOG.exception(msg)
            helper.cleanup_unmount_temp_folder(share_instance_id,
                                               migration_info_src, src_path)
            helper.cleanup_temp_folder(share_instance_id, mount_path)
            helper.cleanup_temp_folder(new_share_instance_id, mount_path)
            raise exception.ShareMigrationFailed(reason=msg)

        copy = None

        try:
            copy = data_utils.Copy(mount_path + share_instance_id,
                                   mount_path + new_share_instance_id,
                                   ignore_list)

            self.busy_tasks_shares[share['id']] = copy

            self.db.share_update(context, share['id'], {
                'task_state':
                    constants.TASK_STATE_MIGRATION_COPYING_IN_PROGRESS
            })

            copy.run()

            self.db.share_update(
                context, share['id'],
                {'task_state':
                    constants.TASK_STATE_MIGRATION_COPYING_COMPLETING
                 })

            if copy.get_progress()['total_progress'] == 100:
                migrated = True

        except Exception as e:
            LOG.exception(six.text_type(e))
            LOG.error(_LE("Failed to copy files for "
                          "migration of share instance %(share_instance_id)s "
                          "to %(new_share_instance_id)s") % {
                'share_instance_id': share_instance_id,
                'new_share_instance_id': new_share_instance_id})

        # TODO(ganso): Implement queues in Data Service to prevent AMQP
        # errors when migration takes a very long time.

        _run_unmount_command(migration_info_src, src_path)
        _run_unmount_command(migration_info_dest, dest_path)

        utils.execute('rmdir', ''.join((mount_path, share_instance_id)),
                      check_exit_code=False)
        utils.execute('rmdir', ''.join((mount_path, new_share_instance_id)),
                      check_exit_code=False)

        helper.deny_migration_access(access_ref)

        if copy and copy.cancelled:
            msg = _("Share migration of share %s was cancelled.") % share['id']
            LOG.warn(msg)
            raise exception.ShareMigrationCancelled(share_id=share['id'])

        elif not migrated:
            msg = ("Copying from share instance %(instance_id)s "
                   "to %(new_instance_id)s did not succeed." % {
                       'instance_id': share_instance_id,
                       'new_instance_id': new_share_instance_id})
            raise exception.ShareMigrationFailed(reason=msg)

        LOG.debug("Copying completed in migration for share %s." % share['id'])
