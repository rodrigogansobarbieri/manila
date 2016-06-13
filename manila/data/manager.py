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

import os

from oslo_config import cfg
from oslo_log import log
import six

from manila.i18n import _, _LE, _LI, _LW
from manila.common import constants
from manila import context
from manila.data import helper as data_helper
from manila.data import utils as data_utils
from manila import exception
from manila import manager
from manila.share import rpcapi as share_rpc
from manila import utils

LOG = log.getLogger(__name__)

data_opts = [
    cfg.StrOpt(
        'mount_tmp_location',
        default='/tmp/',
        deprecated_name='migration_tmp_location',
        help="Temporary path to create and mount shares during migration."),

]

CONF = cfg.CONF
CONF.register_opts(data_opts)


class DataManager(manager.Manager):
    """Receives requests to handle data and sends responses."""

    RPC_API_VERSION = '1.1'

    def __init__(self, service_name=None, *args, **kwargs):
        super(DataManager, self).__init__(*args, **kwargs)
        self.busy_tasks_shares = {}

    def init_host(self):
        ctxt = context.get_admin_context()
        shares = self.db.share_get_all(ctxt)
        for share in shares:
            if share['task_state'] in constants.BUSY_COPYING_STATES:
                self.db.share_update(
                    ctxt, share['id'],
                    {'task_state': constants.TASK_STATE_DATA_COPYING_ERROR})

    def migration_start(self, context, ignore_list, share_id,
                        share_instance_id, dest_share_instance_id,
                        migration_info_src, migration_info_dest, complete):

        LOG.info(_LI(
            "Received request to migrate share content from share instance "
            "%(instance_id)s to instance %(dest_instance_id)s."),
            {'instance_id': share_instance_id,
             'dest_instance_id': dest_share_instance_id})

        share_ref = self.db.share_get(context, share_id)

        share_rpcapi = share_rpc.ShareAPI()

        mount_path = CONF.mount_tmp_location

        try:
            copy = data_utils.Copy(
                os.path.join(mount_path, share_instance_id),
                os.path.join(mount_path, dest_share_instance_id),
                ignore_list)

            self._copy_share_data(
                context, copy, share_ref, None, share_instance_id,
                dest_share_instance_id, migration_info_src,
                migration_info_dest)
        except exception.ShareDataCopyCancelled:
            share_rpcapi.migration_complete(
                context, share_ref, share_instance_id, dest_share_instance_id)
            return
        except Exception:
            self.db.share_update(
                context, share_id,
                {'task_state': constants.TASK_STATE_DATA_COPYING_ERROR})
            msg = _("Failed to copy contents from instance %(src)s to "
                    "instance %(dest)s.") % {'src': share_instance_id,
                                             'dest': dest_share_instance_id}
            LOG.exception(msg)
            share_rpcapi.migration_complete(
                context, share_ref, share_instance_id, dest_share_instance_id)
            raise exception.ShareDataCopyFailed(reason=msg)
        finally:
            self.busy_tasks_shares.pop(share_id, None)

        LOG.info(_LI(
            "Completed copy operation of migrating share content from share "
            "instance %(instance_id)s to instance %(dest_instance_id)s."),
            {'instance_id': share_instance_id,
             'dest_instance_id': dest_share_instance_id})

        if complete:
            LOG.info(_LI(
                "Notifying source backend that migrating share content from"
                " share instance %(instance_id)s to instance "
                "%(dest_instance_id)s completed."),
                {'instance_id': share_instance_id,
                 'dest_instance_id': dest_share_instance_id})

            share_rpcapi.migration_complete(
                context, share_ref, share_instance_id, dest_share_instance_id)

    def data_copy_cancel(self, context, share_id):
        LOG.info(_LI("Received request to cancel data copy "
                     "of share %s."), share_id)
        copy = self.busy_tasks_shares.get(share_id)
        if copy:
            copy.cancel()
        else:
            msg = _("Data copy for migration of share %s cannot be cancelled"
                    " at this moment.") % share_id
            LOG.error(msg)
            raise exception.InvalidShare(reason=msg)

    def data_copy_get_progress(self, context, share_id):
        LOG.info(_LI("Received request to get data copy information "
                     "of share %s."), share_id)
        copy = self.busy_tasks_shares.get(share_id)
        if copy:
            result = copy.get_progress()
            LOG.info(_LI("Obtained following data copy information "
                         "of share %(share)s: %(info)s."),
                     {'share': share_id,
                      'info': six.text_type(result)})
            return result
        else:
            msg = _("Migration of share %s data copy progress cannot be "
                    "obtained at this moment.") % share_id
            LOG.error(msg)
            raise exception.InvalidShare(reason=msg)

    def copy_share_data(self, context, src_share_id, dest_share_id,
                        src_path, dest_path, src_share_instance_id,
                        dest_share_instance_id, migration_info_src,
                        migration_info_dest, force_copy):
        LOG.info(_LI(
            "Received request to copy share data from share instance "
            "%(src_instance_id)s to instance %(dest_instance_id)s."),
            {'src_instance_id': src_share_instance_id,
             'dest_instance_id': dest_share_instance_id})

        src_share_ref = self.db.share_get(context, src_share_id)
        dest_share_ref = self.db.share_get(context, dest_share_id)

        mount_path = CONF.mount_tmp_location

        copy_path_src = os.path.join(mount_path, src_share_instance_id,
                                     src_path)
        copy_path_dest = os.path.join(mount_path, dest_share_instance_id,
                                      dest_path)

        try:
            copy = data_utils.Copy(copy_path_src, copy_path_dest, [])

            self._copy_share_data(
                context, copy, src_share_ref, dest_share_ref,
                src_share_instance_id, dest_share_instance_id,
                migration_info_src, migration_info_dest, force_copy,
                copy_path_src, copy_path_dest)
        except exception.ShareDataCopyCancelled:
            return
        except Exception:
            for share in (src_share_ref, dest_share_ref):
                self.db.share_update(
                    context, share['id'],
                    {'task_state': constants.TASK_STATE_DATA_COPYING_ERROR})
            msg = _("Failed to copy contents from instance %(src)s to "
                    "instance %(dest)s.") % {'src': src_share_instance_id,
                                             'dest': dest_share_instance_id}
            LOG.exception(msg)
            raise exception.ShareDataCopyFailed(reason=msg)
        finally:
            self.busy_tasks_shares.pop(src_share_id, None)
            self.busy_tasks_shares.pop(dest_share_id, None)

        LOG.info(_LI(
            "Completed operation of copying share data from share "
            "instance %(src_instance_id)s to instance %(dest_instance_id)s."),
            {'src_instance_id': src_share_instance_id,
             'dest_instance_id': dest_share_instance_id})

    def delete_share_data(self, context, share_id, path, share_instance_id,
                          migration_info):

        LOG.info(_LI(
            "Received request to delete share data in share instance "
            "%(instance_id)s on path %(path)s."),
            {'instance_id': share_instance_id, 'path': path})

        share_ref = self.db.share_get(context, share_id)

        try:
            self._delete_share_data(self, share_ref, share_instance_id,
                                    migration_info, path)
        except Exception:
            self.db.share_update(
                context, share_id,
                {'task_state': constants.TASK_STATE_DATA_COPYING_ERROR})
            msg = _("Failed to delete contents from instance %(instance)s on "
                    "path %(path)s.") % {'instance': share_instance_id,
                                         'path': path}
            LOG.exception(msg)
            raise exception.ShareDataCopyFailed(reason=msg)

        LOG.info(_LI(
            "Completed delete share data operation on share instance %s."),
            share_instance_id)

    def _delete_share_data(self, context, share, share_instance_id,
                           migration_info, path):

        deleted = False
        helper = data_helper.DataServiceHelper(context, self.db, share)

        access_ref = helper.allow_access_to_data_service(
            share, share_instance_id, None)

        mount_path = CONF.mount_tmp_location

        try:
            helper.mount_share_instance(
                migration_info['mount'], mount_path, share_instance_id)
        except Exception:
            msg = _("Data deletion failed attempting to mount "
                    "share instance %s.") % share_instance_id
            LOG.exception(msg)
            raise exception.ShareDataCopyFailed(reason=msg)

        try:
            delete_path = os.path.join(mount_path, share_instance_id, path)
            utils.execute('rm', '-rf', delete_path, run_as_root=True)
            deleted = True
        except Exception:
            LOG.exception(_LE("Failed to delete data from share instance "
                              "%(share_instance_id)s on path %(path)s."),
                          {'share_instance_id': share_instance_id,
                           'path': path})

        try:
            helper.unmount_share_instance(migration_info['unmount'],
                                          mount_path, share_instance_id)
        except Exception:
            LOG.exception(_LE("Could not unmount folder of instance"
                          " %s after its data deletion."), share_instance_id)

        try:
            helper.deny_access_to_data_service(access_ref, share_instance_id)
        except Exception:
            LOG.exception(_LE("Could not deny access to instance"
                          " %s after its data deletion."), share_instance_id)

        if not deleted:
            msg = _("Deleting data from share instance %(instance_id)s "
                    "on path %(path)s did not succeed.") % (
                {'instance_id': share_instance_id, 'path': path})
            raise exception.ShareDataCopyFailed(reason=msg)
        else:
            self.db.share_update(
                context, share['id'],
                {'task_state': constants.TASK_STATE_DATA_COPYING_COMPLETED})

        LOG.debug("Data deletion from share instance %(src_instance)s on "
                  "path %(path)s was successful.",
                  {'src_instance': share_instance_id, 'path': path})

    def _copy_share_data(
            self, context, copy, src_share, dest_share, share_instance_id,
            dest_share_instance_id, migration_info_src, migration_info_dest,
            force_copy=True, copy_path_src=None, copy_path_dest=None):

        shares = [x for x in (src_share, dest_share) if x is not None]
        copied = False
        mount_path = CONF.mount_tmp_location

        share_instance = self.db.share_instance_get(
            context, share_instance_id, with_share_data=True)
        dest_share_instance = self.db.share_instance_get(
            context, dest_share_instance_id, with_share_data=True)

        for share in shares:
            self.db.share_update(
                context, share['id'],
                {'task_state': constants.TASK_STATE_DATA_COPYING_STARTING})

        helper_src = data_helper.DataServiceHelper(context, self.db, src_share)
        if dest_share:
            helper_dest = data_helper.DataServiceHelper(
                context, self.db, dest_share)
        else:
            helper_dest = helper_src

        if dest_share:
            access_ref_list_src = helper_src.allow_access_to_data_service(
                share_instance, migration_info_src)
            try:
                access_ref_list_dest = (
                    helper_dest.allow_access_to_data_service(
                        dest_share_instance, migration_info_dest))
            except Exception:
                msg = _("Data copy failed attempting to allow "
                        "access to share instance %(instance_id)s.") % {
                    'instance_id': dest_share_instance_id}
                LOG.exception(msg)
                helper_src.cleanup_data_access(access_ref_list_src,
                                               share_instance_id)
                raise exception.ShareDataCopyFailed(reason=msg)
        else:
            access_ref_list_src = helper_src.allow_access_to_data_service(
                share_instance, migration_info_src, dest_share_instance,
                migration_info_dest)
            access_ref_list_dest = access_ref_list_src

        def _call_cleanups(items):
            for item in items:
                if 'unmount_src' == item:
                    helper_src.cleanup_unmount_temp_folder(
                        migration_info_src['unmount'], mount_path,
                        share_instance_id)
                elif 'temp_folder_src' == item:
                    helper_src.cleanup_temp_folder(share_instance_id,
                                                   mount_path)
                elif 'temp_folder_dest' == item:
                    helper_dest.cleanup_temp_folder(dest_share_instance_id,
                                                    mount_path)
                elif 'access_src' == item:
                    helper_src.cleanup_data_access(access_ref_list_src,
                                                   share_instance_id)
                elif 'access_dest' == item:
                    helper_dest.cleanup_data_access(access_ref_list_dest,
                                                    dest_share_instance_id)
        try:
            helper_src.mount_share_instance(
                migration_info_src['mount'], mount_path, share_instance)
        except Exception:
            msg = _("Data copy failed attempting to mount "
                    "share instance %s.") % share_instance_id
            LOG.exception(msg)
            _call_cleanups(['temp_folder_src', 'access_dest', 'access_src'])
            raise exception.ShareDataCopyFailed(reason=msg)

        try:
            helper_dest.mount_share_instance(
                migration_info_dest['mount'], mount_path,
                dest_share_instance)
        except Exception:
            msg = _("Data copy failed attempting to mount "
                    "share instance %s.") % dest_share_instance_id
            LOG.exception(msg)
            _call_cleanups(['temp_folder_dest', 'unmount_src',
                            'temp_folder_src', 'access_dest', 'access_src'])
            raise exception.ShareDataCopyFailed(reason=msg)

        for share in shares:
            self.busy_tasks_shares[share['id']] = copy
            self.db.share_update(
                context, share['id'],
                {'task_state': constants.TASK_STATE_DATA_COPYING_IN_PROGRESS})

        if not force_copy:
            out, __ = utils.execute('df', '-B1', copy_path_dest,
                                    run_as_root=True)
            free = int(out.split('\n')[1].split()[3])
            out, __ = utils.execute('du', '-bslLB1', copy_path_src,
                                    run_as_root=True)
            required = int(out.split()[0])
            if required > free:
                msg = _("There is not enough space to perform copying from "
                        "%(src)s to %(dest)s.") % {'src': copy_path_src,
                                                   'dest': copy_path_dest}
                raise exception.ShareDataCopyFailed(reason=msg)

        try:
            copy.run()

            for share in shares:
                self.db.share_update(
                    context, share['id'],
                    {'task_state': (
                        constants.TASK_STATE_DATA_COPYING_COMPLETING)})

            if copy.get_progress()['total_progress'] == 100:
                copied = True

        except Exception:
            LOG.exception(_LE("Failed to copy data from share instance "
                              "%(share_instance_id)s to "
                              "%(dest_share_instance_id)s."),
                          {'share_instance_id': share_instance_id,
                           'dest_share_instance_id': dest_share_instance_id})

        try:
            helper_src.unmount_share_instance(migration_info_src['unmount'],
                                              mount_path, share_instance_id)
        except Exception:
            LOG.exception(_LE("Could not unmount folder of instance"
                          " %s after its data copy."), share_instance_id)

        try:
            helper_dest.unmount_share_instance(
                migration_info_dest['unmount'], mount_path,
                dest_share_instance_id)
        except Exception:
            LOG.exception(_LE("Could not unmount folder of instance"
                          " %s after its data copy."), dest_share_instance_id)

        try:
            helper_src.deny_access_to_data_service(
                access_ref_list_src, share_instance)
        except Exception:
            LOG.exception(_LE("Could not deny access to instance"
                          " %s after its data copy."), share_instance_id)

        try:
            helper_dest.deny_access_to_data_service(
                access_ref_list_dest, dest_share_instance)
        except Exception:
            LOG.exception(_LE("Could not deny access to instance"
                          " %s after its data copy."), dest_share_instance_id)

        if copy and copy.cancelled:
            for share in shares:
                self.db.share_update(
                    context, share['id'],
                    {'task_state': (
                        constants.TASK_STATE_DATA_COPYING_CANCELLED)})
            LOG.warning(_LW("Copy of data from share instance "
                            "%(src_instance)s to share instance "
                            "%(dest_instance)s was cancelled."),
                        {'src_instance': share_instance_id,
                         'dest_instance': dest_share_instance_id})
            raise exception.ShareDataCopyCancelled(
                src_instance=share_instance_id,
                dest_instance=dest_share_instance_id)

        elif not copied:
            msg = _("Copying data from share instance %(instance_id)s "
                    "to %(dest_instance_id)s did not succeed.") % (
                {'instance_id': share_instance_id,
                 'dest_instance_id': dest_share_instance_id})
            raise exception.ShareDataCopyFailed(reason=msg)

        for share in shares:
            self.db.share_update(
                context, share['id'],
                {'task_state': constants.TASK_STATE_DATA_COPYING_COMPLETED})

        LOG.debug("Copy of data from share instance %(src_instance)s to "
                  "share instance %(dest_instance)s was successful.",
                  {'src_instance': share_instance_id,
                   'dest_instance': dest_share_instance_id})
