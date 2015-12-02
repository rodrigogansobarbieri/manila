# Copyright (c) 2015 Mirantis Inc.
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

from oslo_log import log
from oslo_utils import excutils
import six

from manila.common import constants
from manila.i18n import _LI

LOG = log.getLogger(__name__)


class ShareInstanceAccess(object):

    def __init__(self, db, driver):
        self.db = db
        self.driver = driver

    def update_access_rules(self, context, share_instance, add_rules=None,
                            delete_rules=None, share_server=None):
        """Update access rules in driver and database for given share instance.

        :param context: current context
        :param share_instance: Share instance model
        :param add_rules: list with ShareAccessMapping models or None - rules
        which should be added
        :param delete_rules: list with ShareAccessMapping models, "all", None
        - rules which should be deleted. If "all" is provided - all rules will
        be deleted.
        :param share_server: Share server model
        """
        share_instance = self.db.share_instance_get(
            context, share_instance['id'], with_share_data=True)

        rules = self.db.share_access_get_all_for_share(
            context, share_instance['share_id'])

        add_rules = add_rules or []
        delete_rules = delete_rules or []

        if six.text_type(delete_rules).lower() == "all":
            delete_rules = rules
            rules = []
        elif delete_rules:
            delete_ids = [rule['id'] for rule in delete_rules]
            rules = list(filter(lambda r: r['id'] not in delete_ids, rules))

        try:
            try:
                self.driver.update_access(
                    context,
                    share_instance,
                    rules,
                    add_rules=add_rules,
                    delete_rules=delete_rules,
                    share_server=share_server
                )
            except NotImplementedError:
                # NOTE(u_glide): Fallback to legacy allow_access/deny_access
                # for drivers without update_access() method support

                for rule in add_rules:
                    self.driver.allow_access(
                        context,
                        share_instance,
                        rule,
                        share_server=share_server
                    )

                for rule in delete_rules:
                    self.driver.deny_access(
                        context,
                        share_instance,
                        rule,
                        share_server=share_server
                    )
        except Exception:
            with excutils.save_and_reraise_exception():
                self.db.share_instance_update_access_status(
                    context,
                    share_instance['id'],
                    constants.STATUS_ERROR
                )

        self._remove_access_rules(context, delete_rules, share_instance['id'])

        self.db.share_instance_update_access_status(
            context,
            share_instance['id'],
            constants.STATUS_ACTIVE
        )

        LOG.info(_LI("Access rules ware successfully applied for "
                     "share instance: %s"),
                 share_instance['id'])

    def _remove_access_rules(self, context, access_rules, share_instance_id):
        if not access_rules:
            return

        for rule in access_rules:
            access_mapping = self.db.share_instance_access_get(
                context, rule['id'], share_instance_id)

            self.db.share_instance_access_delete(context, access_mapping['id'])
