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
Tests For Data Manager
"""

import ddt
import mock

import time

from oslo_config import cfg

from manila.common import constants
from manila import context
from manila.data import manager
from manila.data import migration as migration_helper
from manila.data import utils as data_utils
from manila import db
from manila import exception
from manila.share import rpcapi as share_rpc
from manila import test
from manila.tests import db_utils
from manila import utils

CONF = cfg.CONF


@ddt.ddt
class DataManagerTestCase(test.TestCase):
    """Test case for data manager."""

    manager_cls = manager.DataManager

    def setUp(self):
        super(DataManagerTestCase, self).setUp()
        self.manager = self.manager_cls()
        self.context = context.RequestContext('fake_user', 'fake_project')
        self.topic = 'fake_topic'

    def test_init(self):
        manager = self.manager
        self.assertIsNotNone(manager)

    @ddt.data(constants.TASK_STATE_MIGRATION_COPYING_COMPLETING,
              constants.TASK_STATE_MIGRATION_COPYING_STARTING,
              constants.TASK_STATE_MIGRATION_COPYING_IN_PROGRESS)
    def test_init_host(self, status):
        fake_share = db_utils.create_share(
            task_state=status)
        self.mock_object(db, 'share_get_all', mock.Mock(
            return_value=[fake_share]))
        self.mock_object(db, 'share_update')
        data_manager = manager.DataManager()
        data_manager.init_host()

        db.share_update.assert_called_with(
            utils.IsAMatcher(context.RequestContext), fake_share['id'],
            {'task_state': constants.TASK_STATE_MIGRATION_ERROR})

    def _setup_mocks_migrate_share(self):
        fake_share = db_utils.create_share(
            id='fakeid', status=constants.STATUS_AVAILABLE, host='fake_host')
        fake_instance = db_utils.create_share_instance(
            share_id=fake_share['id'],
            status=constants.STATUS_AVAILABLE)
        migration_info = {'mount': ['fake_mount'],
                          'umount': ['fake_umount']}
        self.mock_object(db, 'share_update')
        self.mock_object(db, 'share_instance_get', mock.Mock(
            return_value=fake_instance))
        self.mock_object(db, 'share_get', mock.Mock(return_value=fake_share))
        self.mock_object(share_rpc.ShareAPI, 'migration_complete')
        self.mock_object(time, 'sleep')
        return fake_share, migration_info

    def test_migrate_share(self):

        share, migration_info = self._setup_mocks_migrate_share()

        access = {'access_type': 'ip',
                  'access_level': 'rw',
                  'access_to': 'fake_ip'}

        fake_access_ref = db_utils.create_access(share_id=share['id'])

        CONF.set_default('migration_data_node_ip', 'fake_ip')

        self.mock_object(utils, 'execute')
        self.mock_object(migration_helper.ShareMigrationHelper,
                         'deny_migration_access')
        self.mock_object(migration_helper.ShareMigrationHelper,
                         'allow_migration_access',
                         mock.Mock(return_value=fake_access_ref))
        self.mock_object(utils, 'execute')
        self.mock_object(data_utils.Copy, 'run')

        data_manager = manager.DataManager()
        data_manager.migrate_share(
            self.context, None, 'fakeid', 'ins_id_1', 'ins_id_2',
            migration_info, migration_info, True)

        # asserts

        share_rpc.ShareAPI.migration_complete.assert_called_once_with(
            self.context, share, 'ins_id_1', 'ins_id_2', None)

        migration_helper.ShareMigrationHelper.allow_migration_access.\
            assert_called_once_with(access)

        migration_helper.ShareMigrationHelper.deny_migration_access.\
            assert_called_once_with(fake_access_ref)

    def test_migrate_share_no_access_ip(self):

        share, migration_info = self._setup_mocks_migrate_share()

        CONF.set_default('migration_data_node_ip', None)

        data_manager = manager.DataManager()
        self.assertRaises(
            exception.ShareMigrationFailed, data_manager.migrate_share,
            self.context, None, 'fakeid', 'ins_id_1', 'ins_id_2',
            migration_info, migration_info, True)

        # asserts

        share_rpc.ShareAPI.migration_complete.assert_called_once_with(
            self.context, share, 'ins_id_1', 'ins_id_2',
            constants.TASK_STATE_MIGRATION_ERROR)

    def test_migrate_share_exception_allow_migration_access(self):

        share, migration_info = self._setup_mocks_migrate_share()

        access = {'access_type': 'ip',
                  'access_level': 'rw',
                  'access_to': 'fake_ip'}

        CONF.set_default('migration_data_node_ip', 'fake_ip')

        self.mock_object(migration_helper.ShareMigrationHelper,
                         'allow_migration_access',
                         mock.Mock(side_effect=Exception('')))

        data_manager = manager.DataManager()
        self.assertRaises(
            exception.ShareMigrationFailed, data_manager.migrate_share,
            self.context, None, 'fakeid', 'ins_id_1', 'ins_id_2',
            migration_info, migration_info, True)

        # asserts

        share_rpc.ShareAPI.migration_complete.assert_called_once_with(
            self.context, share, 'ins_id_1', 'ins_id_2',
            constants.TASK_STATE_MIGRATION_ERROR)

        migration_helper.ShareMigrationHelper.allow_migration_access.\
            assert_called_once_with(access)

    def test_migrate_share_exception_mount_1(self):

        share, migration_info = self._setup_mocks_migrate_share()

        access = {'access_type': 'ip',
                  'access_level': 'rw',
                  'access_to': 'fake_ip'}

        fake_access_ref = db_utils.create_access(share_id=share['id'])

        CONF.set_default('migration_data_node_ip', 'fake_ip')

        self.mock_object(migration_helper.ShareMigrationHelper,
                         'deny_migration_access',
                         mock.Mock(side_effect=Exception('')))

        self.mock_object(migration_helper.ShareMigrationHelper,
                         'allow_migration_access',
                         mock.Mock(return_value=fake_access_ref))

        self.mock_object(utils, 'execute', mock.Mock(
            side_effect=[None, None, Exception(''), Exception('')]))

        data_manager = manager.DataManager()
        self.assertRaises(
            exception.ShareMigrationFailed, data_manager.migrate_share,
            self.context, None, 'fakeid', 'ins_id_1', 'ins_id_2',
            migration_info, migration_info, True)

        # asserts

        share_rpc.ShareAPI.migration_complete.assert_called_once_with(
            self.context, share, 'ins_id_1', 'ins_id_2',
            constants.TASK_STATE_MIGRATION_ERROR)

        migration_helper.ShareMigrationHelper.allow_migration_access.\
            assert_called_once_with(access)

        migration_helper.ShareMigrationHelper.deny_migration_access.\
            assert_called_once_with(fake_access_ref)

    def test_migrate_share_exception_mount_2(self):

        share, migration_info = self._setup_mocks_migrate_share()

        access = {'access_type': 'ip',
                  'access_level': 'rw',
                  'access_to': 'fake_ip'}

        fake_access_ref = db_utils.create_access(share_id=share['id'])

        CONF.set_default('migration_data_node_ip', 'fake_ip')

        self.mock_object(migration_helper.ShareMigrationHelper,
                         'deny_migration_access')

        self.mock_object(migration_helper.ShareMigrationHelper,
                         'allow_migration_access',
                         mock.Mock(return_value=fake_access_ref))

        self.mock_object(utils, 'execute', mock.Mock(
            side_effect=[None, None, None, Exception('')]))

        data_manager = manager.DataManager()
        self.assertRaises(
            exception.ShareMigrationFailed, data_manager.migrate_share,
            self.context, None, 'fakeid', 'ins_id_1', 'ins_id_2',
            migration_info, migration_info, True)

        # asserts

        share_rpc.ShareAPI.migration_complete.assert_called_once_with(
            self.context, share, 'ins_id_1', 'ins_id_2',
            constants.TASK_STATE_MIGRATION_ERROR)

        migration_helper.ShareMigrationHelper.allow_migration_access.\
            assert_called_once_with(access)

        migration_helper.ShareMigrationHelper.deny_migration_access.\
            assert_called_once_with(fake_access_ref)

    def test_migrate_share_exception_copy(self):

        share, migration_info = self._setup_mocks_migrate_share()

        access = {'access_type': 'ip',
                  'access_level': 'rw',
                  'access_to': 'fake_ip'}

        fake_access_ref = db_utils.create_access(share_id=share['id'])

        CONF.set_default('migration_data_node_ip', 'fake_ip')

        self.mock_object(migration_helper.ShareMigrationHelper,
                         'deny_migration_access')

        self.mock_object(migration_helper.ShareMigrationHelper,
                         'allow_migration_access',
                         mock.Mock(return_value=fake_access_ref))

        self.mock_object(utils, 'execute')

        self.mock_object(data_utils.Copy, 'run', mock.Mock(
            side_effect=Exception('')))

        data_manager = manager.DataManager()
        self.assertRaises(
            exception.ShareMigrationFailed, data_manager.migrate_share,
            self.context, None, 'fakeid', 'ins_id_1', 'ins_id_2',
            migration_info, migration_info, True)

        # asserts

        share_rpc.ShareAPI.migration_complete.assert_called_once_with(
            self.context, share, 'ins_id_1', 'ins_id_2',
            constants.TASK_STATE_MIGRATION_ERROR)

        migration_helper.ShareMigrationHelper.allow_migration_access.\
            assert_called_once_with(access)

        migration_helper.ShareMigrationHelper.deny_migration_access.\
            assert_called_once_with(fake_access_ref)

    def test_migrate_share_cancelled(self):

        share, migration_info = self._setup_mocks_migrate_share()

        access = {'access_type': 'ip',
                  'access_level': 'rw',
                  'access_to': 'fake_ip'}

        def fake_get_progress():
            return {'total_progress': 99}

        fake_access_ref = db_utils.create_access(share_id=share['id'])

        CONF.set_default('migration_data_node_ip', 'fake_ip')

        self.mock_object(migration_helper.ShareMigrationHelper,
                         'deny_migration_access')

        self.mock_object(migration_helper.ShareMigrationHelper,
                         'allow_migration_access',
                         mock.Mock(return_value=fake_access_ref))

        self.mock_object(data_utils, 'Copy', mock.MagicMock(
            cancelled=True, get_progress=fake_get_progress))

        self.mock_object(utils, 'execute')

        self.mock_object(data_utils.Copy, 'run')

        data_manager = manager.DataManager()

        data_manager.migrate_share(
            self.context, None, 'fakeid', 'ins_id_1', 'ins_id_2',
            migration_info, migration_info, True)

        # asserts

        share_rpc.ShareAPI.migration_complete.assert_called_once_with(
            self.context, share, 'ins_id_1', 'ins_id_2',
            constants.TASK_STATE_MIGRATION_CANCELLED)

        migration_helper.ShareMigrationHelper.allow_migration_access.\
            assert_called_once_with(access)

        migration_helper.ShareMigrationHelper.deny_migration_access.\
            assert_called_once_with(fake_access_ref)

    def test_migration_cancel(self):

        share = db_utils.create_share()

        data_manager = manager.DataManager()
        data_manager.busy_tasks_shares[share['id']] = data_utils.Copy

        self.mock_object(data_utils.Copy, 'cancel')

        data_manager.migration_cancel(self.context, share['id'])

    def test_migration_cancel_not_copying(self):

        data_manager = manager.DataManager()
        self.assertRaises(exception.InvalidShare,
                          data_manager.migration_cancel, self.context,
                          'fake_id')

    def test_migration_get_progress(self):

        share = db_utils.create_share()

        data_manager = manager.DataManager()
        data_manager.busy_tasks_shares[share['id']] = data_utils.Copy

        expected = 'fake_progress'

        self.mock_object(data_utils.Copy, 'get_progress',
                         mock.Mock(return_value=expected))

        result = data_manager.migration_get_progress(self.context, share['id'])

        self.assertEqual(expected, result)

    def test_migration_get_progress_not_copying(self):

        data_manager = manager.DataManager()
        self.assertRaises(exception.InvalidShare,
                          data_manager.migration_get_progress, self.context,
                          'fake_id')
