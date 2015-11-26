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
from manila.migration import utils as migration_utils
from manila import test


@ddt.ddt
class ShareMigrationHelperTestCase(test.TestCase):
    """Tests DataMigrationHelper."""

    def setUp(self):
        super(ShareMigrationHelperTestCase, self).setUp()
        self.context = context.get_admin_context()

    def test_wait_for_access_update(self):
        sid = 1
        fake_share_instances = [
            {'id': sid, 'access_rules_status': constants.STATUS_OUT_OF_SYNC},
            {'id': sid, 'access_rules_status': constants.STATUS_ACTIVE},
        ]

        self.mock_object(time, 'sleep')
        self.mock_object(db, 'share_instance_get',
                         mock.Mock(side_effect=fake_share_instances))

        migration_utils.wait_for_access_update(self.context, db,
                                               fake_share_instances[0], 1)

        db.share_instance_get.assert_has_calls(
            [mock.call(mock.ANY, sid), mock.call(mock.ANY, sid)]
        )
        time.sleep.assert_called_once_with(1)

    @ddt.data(
        (
            {'id': '1', 'access_rules_status': constants.STATUS_ERROR},
            exception.ShareMigrationFailed
        ),
        (
            {'id': '1', 'access_rules_status': constants.STATUS_OUT_OF_SYNC},
            exception.ShareMigrationFailed
        ),
    )
    @ddt.unpack
    def test_wait_for_access_update_invalid(self, fake_instance, expected_exc):
        self.mock_object(time, 'sleep')
        self.mock_object(db, 'share_instance_get',
                         mock.Mock(return_value=fake_instance))

        now = time.time()
        timeout = now + 100

        self.mock_object(time, 'time',
                         mock.Mock(side_effect=[now, timeout]))

        self.assertRaises(expected_exc,
                          migration_utils.wait_for_access_update, self.context,
                          db, fake_instance, 1)
