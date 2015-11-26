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
"""Share migration-related utilities and helpers."""

import time

from manila.common import constants
from manila import exception
from manila.i18n import _


def wait_for_access_update(context, db, share_instance,
                           migration_wait_access_rules_timeout):
    starttime = time.time()
    deadline = starttime + migration_wait_access_rules_timeout
    tries = 0

    while True:
        instance = db.share_instance_get(context, share_instance['id'])

        if instance['access_rules_status'] == constants.STATUS_ACTIVE:
            break

        tries += 1
        now = time.time()
        if instance['access_rules_status'] == constants.STATUS_ERROR:
            msg = _("Failed to update access rules"
                    " on share instance %s") % share_instance['id']
            raise exception.ShareMigrationFailed(reason=msg)
        elif now > deadline:
            msg = _("Timeout trying to update access rules"
                    " on share instance %(share_id)s. Timeout "
                    "was %(timeout)s seconds.") % {
                'share_id': share_instance['id'],
                'timeout': migration_wait_access_rules_timeout}
            raise exception.ShareMigrationFailed(reason=msg)
        else:
            time.sleep(tries ** 2)
