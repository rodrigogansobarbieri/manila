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

from manila.common import constants
from manila import context
from manila import db
from manila.migration import helper as migration_helper
from manila.migration import utils as migration_utils
from manila.share import rpcapi as share_rpc
from manila import test
from manila.tests import db_utils


@ddt.ddt
class ShareMigrationHelperTestCase(test.TestCase):
    """Tests DataMigrationHelper."""

    def setUp(self):
        super(ShareMigrationHelperTestCase, self).setUp()
        self.share = db_utils.create_share()
        self.share_instance = db_utils.create_share_instance(
            share_id=self.share['id'],
            status=constants.STATUS_AVAILABLE)
        self.context = context.get_admin_context()

        share_instance_mock = self.mock_object(
            db, 'share_instance_get', mock.Mock(
                side_effect=[self.share_instance, self.share_instance]))

        self.helper = migration_helper.ShareMigrationHelper(
            self.context, db, self.share, self.share_instance['id'],
            self.share_instance['id'])

        share_instance_mock.reset_mock()

    def test_allow_migration_access(self):

        access = {'share_id': self.share['id'],
                  'access_to': 'fake_ip',
                  'access_type': 'fake_type',
                  'access_level': 'rw'}

        access_active = db_utils.create_access(state=constants.STATUS_ACTIVE,
                                               share_id=self.share['id'])

        self.mock_object(self.helper.db, 'share_access_create',
                         mock.Mock(return_value=access_active))

        self.mock_object(migration_utils, 'wait_for_access_update')

        self.mock_object(self.helper.db, 'share_instance_update_access_status')

        self.mock_object(share_rpc.ShareAPI, 'allow_access')

        self.helper.allow_migration_access(access)

        self.helper.db.share_access_create.assert_called_once_with(
            self.context, access)

        self.helper.db.share_instance_update_access_status.\
            assert_called_with(
                self.context, self.share_instance['id'],
                constants.STATUS_OUT_OF_SYNC)

        share_rpc.ShareAPI.allow_access.assert_called_with(
            self.context, self.share_instance, access_active)

    def test_deny_migration_access(self):

        access_active = db_utils.create_access(state=constants.STATUS_ACTIVE,
                                               share_id=self.share['id'])

        self.mock_object(self.helper.db, 'share_instance_update_access_status')

        self.mock_object(migration_utils, 'wait_for_access_update')

        self.mock_object(share_rpc.ShareAPI, 'deny_access')

        self.helper.deny_migration_access(access_active)

        self.helper.db.share_instance_update_access_status.\
            assert_called_with(
                self.context, self.share_instance['id'],
                constants.STATUS_OUT_OF_SYNC)

        share_rpc.ShareAPI.deny_access.assert_called_with(
            self.context, self.share_instance, access_active)
