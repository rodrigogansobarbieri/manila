# Copyright 2015 Hitachi Data Systems inc.
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

import ddt
import mock

import time

from manila.common import constants
from manila import context
from manila import db
from manila import exception
from manila.share import api as share_api
from manila.share import migration
from manila import test
from manila.tests import db_utils
from manila import utils as migration_utils


@ddt.ddt
class ShareMigrationHelperTestCase(test.TestCase):
    """Tests ShareMigrationHelper."""

    def setUp(self):
        super(ShareMigrationHelperTestCase, self).setUp()
        self.share = db_utils.create_share()
        self.context = context.get_admin_context()
        self.helper = migration.ShareMigrationHelper(
            self.context, db, self.share)

    def test_delete_instance_and_wait(self):

        self.mock_object(share_api.API, 'delete_instance')
        self.mock_object(db, 'share_instance_get',
                         mock.Mock(side_effect=[self.share.instance, None]))
        self.mock_object(time, 'sleep')

        self.helper.delete_instance_and_wait(self.context, self.share.instance)

        db.share_instance_get.assert_any_call(
            self.context, self.share.instance['id'])

    def test_delete_instance_and_wait_timeout(self):

        self.mock_object(share_api.API, 'delete_instance')
        self.mock_object(db, 'share_instance_get',
                         mock.Mock(side_effect=[self.share.instance, None]))
        self.mock_object(time, 'sleep')

        now = time.time()
        timeout = now + 310

        self.mock_object(time, 'time',
                         mock.Mock(side_effect=[now, timeout]))

        self.assertRaises(exception.ShareMigrationFailed,
                          self.helper.delete_instance_and_wait,
                          self.context, self.share.instance)

        db.share_instance_get.assert_called_once_with(
            self.context, self.share.instance['id'])

    def test_delete_instance_and_wait_not_found(self):

        self.mock_object(share_api.API, 'delete_instance')
        self.mock_object(db, 'share_instance_get',
                         mock.Mock(side_effect=exception.NotFound))
        self.mock_object(time, 'sleep')

        self.helper.delete_instance_and_wait(self.context,
                                             self.share.instance)

        db.share_instance_get.assert_called_once_with(
            self.context, self.share.instance['id'])

    def test_create_instance_and_wait(self):

        host = {'host': 'fake-host'}

        share_instance_creating = db_utils.create_share_instance(
            share_id=self.share['id'], status=constants.STATUS_CREATING,
            share_network_id='fake_network_id')
        share_instance_available = db_utils.create_share_instance(
            share_id=self.share['id'], status=constants.STATUS_AVAILABLE,
            share_network_id='fake_network_id')

        self.mock_object(share_api.API, 'create_instance',
                         mock.Mock(return_value=share_instance_creating))
        self.mock_object(db, 'share_instance_get',
                         mock.Mock(side_effect=[share_instance_creating,
                                                share_instance_available]))
        self.mock_object(time, 'sleep')

        self.helper.create_instance_and_wait(
            self.context, self.share, share_instance_creating, host)

        db.share_instance_get.assert_any_call(
            self.context, share_instance_creating['id'], with_share_data=True)

    def test_create_instance_and_wait_status_error(self):

        host = {'host': 'fake-host'}

        share_instance_error = db_utils.create_share_instance(
            share_id=self.share['id'], status=constants.STATUS_ERROR,
            share_network_id='fake_network_id')

        self.mock_object(share_api.API, 'create_instance',
                         mock.Mock(return_value=share_instance_error))
        self.mock_object(self.helper, 'delete_instance_and_wait')
        self.mock_object(db, 'share_instance_get',
                         mock.Mock(return_value=share_instance_error))
        self.mock_object(time, 'sleep')

        self.assertRaises(exception.ShareMigrationFailed,
                          self.helper.create_instance_and_wait,
                          self.context, self.share, share_instance_error, host)

        db.share_instance_get.assert_called_with(
            self.context, share_instance_error['id'], with_share_data=True)

    def test_create_instance_and_wait_timeout(self):

        host = {'host': 'fake-host'}

        share_instance_creating = db_utils.create_share_instance(
            share_id=self.share['id'], status=constants.STATUS_CREATING,
            share_network_id='fake_network_id')

        self.mock_object(share_api.API, 'create_instance',
                         mock.Mock(return_value=share_instance_creating))

        self.mock_object(self.helper, 'delete_instance_and_wait')

        self.mock_object(db, 'share_instance_get',
                         mock.Mock(return_value=share_instance_creating))
        self.mock_object(time, 'sleep')

        now = time.time()
        timeout = now + 310

        self.mock_object(time, 'time', mock.Mock(side_effect=[now, timeout]))

        self.assertRaises(exception.ShareMigrationFailed,
                          self.helper.create_instance_and_wait, self.context,
                          self.share, share_instance_creating, host)

        db.share_instance_get.assert_called_with(
            self.context, share_instance_creating['id'], with_share_data=True)

    def test_change_to_read_only_with_ro_support(self):

        share_instance = db_utils.create_share_instance(
            share_id=self.share['id'], status=constants.STATUS_AVAILABLE)

        access = db_utils.create_access(share_id=self.share['id'],
                                        access_to='fake_ip',
                                        access_level='rw')

        server = db_utils.create_share_server(share_id=self.share['id'])

        share_driver = mock.Mock()
        self.mock_object(share_driver, 'update_access')

        self.mock_object(db, 'share_access_get_all_for_instance',
                         mock.Mock(return_value=[access]))

        self.helper.change_to_read_only(share_instance, server, True,
                                        share_driver)

        db.share_access_get_all_for_instance.assert_called_once_with(
            self.context, share_instance['id'])
        share_driver.update_access.assert_called_once_with(
            self.context, share_instance, [access], add_rules=[],
            delete_rules=[], share_server=server)

    def test_change_to_read_only_without_ro_support(self):

        share_instance = db_utils.create_share_instance(
            share_id=self.share['id'], status=constants.STATUS_AVAILABLE)

        access = db_utils.create_access(share_id=self.share['id'],
                                        access_to='fake_ip',
                                        access_level='rw')

        server = db_utils.create_share_server(share_id=self.share['id'])

        share_driver = mock.Mock()
        self.mock_object(share_driver, 'update_access')

        self.mock_object(db, 'share_access_get_all_for_instance',
                         mock.Mock(return_value=[access]))

        self.helper.change_to_read_only(share_instance, server, False,
                                        share_driver)

        db.share_access_get_all_for_instance.assert_called_once_with(
            self.context, share_instance['id'])
        share_driver.update_access.assert_called_once_with(
            self.context, share_instance, [], add_rules=[],
            delete_rules=[access], share_server=server)

    def test_revert_access_rules(self):

        share_instance = db_utils.create_share_instance(
            share_id=self.share['id'], status=constants.STATUS_AVAILABLE)

        access = db_utils.create_access(share_id=self.share['id'],
                                        access_to='fake_ip',
                                        access_level='rw')

        server = db_utils.create_share_server(share_id=self.share['id'])

        share_driver = mock.Mock()
        self.mock_object(share_driver, 'update_access')

        self.mock_object(db, 'share_access_get_all_for_instance',
                         mock.Mock(return_value=[access]))

        self.helper.revert_access_rules(share_instance, server, share_driver)

        db.share_access_get_all_for_instance.assert_called_once_with(
            self.context, share_instance['id'])
        share_driver.update_access.assert_called_once_with(
            self.context, share_instance, [access], add_rules=[],
            delete_rules=[], share_server=server)

    def test_apply_new_access_rules(self):

        share_instance = db_utils.create_share_instance(
            share_id=self.share['id'], status=constants.STATUS_AVAILABLE)
        new_share_instance = db_utils.create_share_instance(
            share_id=self.share['id'], status=constants.STATUS_AVAILABLE,
            access_rules_status='active')

        access = db_utils.create_access(share_id=self.share['id'],
                                        access_to='fake_ip',
                                        access_level='rw')

        self.mock_object(db, 'share_access_get_all_for_instance',
                         mock.Mock(return_value=[access]))
        self.mock_object(db, 'share_instance_get', mock.Mock(
            return_value=new_share_instance))
        self.mock_object(share_api.API, 'allow_access_to_instance')
        self.mock_object(migration_utils, 'wait_for_access_update')

        self.helper.apply_new_access_rules(share_instance, new_share_instance)

        db.share_access_get_all_for_instance.assert_called_once_with(
            self.context, share_instance['id'])
        share_api.API.allow_access_to_instance.assert_called_once_with(
            self.context, new_share_instance, [access])
