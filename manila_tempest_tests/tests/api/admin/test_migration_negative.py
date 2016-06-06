# Copyright 2015 Hitachi Data Systems.
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

from tempest import config
from tempest.lib import exceptions as lib_exc
from tempest import test

from manila_tempest_tests.tests.api import base

CONF = config.CONF


class MigrationTest(base.BaseSharesAdminTest):
    """Tests Share Migration.

    Tests migration in multi-backend environment.
    """

    protocol = "nfs"

    @classmethod
    def resource_setup(cls):
        super(MigrationTest, cls).resource_setup()

        if len(CONF.share.backend_names) < 2:
            raise cls.skipException("For running migration tests it is "
                                    "required two names in config. Skipping.")

        if not (CONF.share.run_fallback_migration_tests or
                CONF.share_run_optimized_migration_tests):
            raise cls.skipException("Share migration tests disabled. "
                                    "Skipping.")

        pools = cls.shares_client.list_pools()['pools']

        if len(pools) < 2:
            raise cls.skipException("At least two different pool entries "
                                    "are needed to run migration tests. "
                                    "Skipping.")

        cls.share = cls.create_share(cls.protocol)
        cls.share = cls.shares_client.get_share(cls.share['id'])
        dest_pool = next(
            (x for x in pools if (x['name'] != cls.share['host'] and any(
                y in x['name'] for y in CONF.share.backend_names))), None)

        if not dest_pool or dest_pool.get('name') is None:
            raise cls.skipException("No valid pool entries to run migration "
                                    "tests. Skipping.")

        cls.dest_pool = dest_pool['name']

    @test.attr(type=[base.TAG_NEGATIVE, base.TAG_API_WITH_BACKEND])
    @base.skip_if_microversion_lt("2.15")
    def test_migration_cancel_invalid(self):
        self.assertRaises(
            lib_exc.BadRequest, self.shares_v2_client.migration_cancel,
            self.share['id'])

    @test.attr(type=[base.TAG_NEGATIVE, base.TAG_API_WITH_BACKEND])
    @base.skip_if_microversion_lt("2.15")
    def test_migration_get_progress_invalid(self):
        self.assertRaises(
            lib_exc.BadRequest, self.shares_v2_client.migration_get_progress,
            self.share['id'])

    @test.attr(type=[base.TAG_NEGATIVE, base.TAG_API_WITH_BACKEND])
    @base.skip_if_microversion_lt("2.15")
    def test_migration_complete_invalid(self):
        self.assertRaises(
            lib_exc.BadRequest, self.shares_v2_client.migration_complete,
            self.share['id'])

    @test.attr(type=[base.TAG_NEGATIVE, base.TAG_API_WITH_BACKEND])
    @base.skip_if_microversion_lt("2.5")
    def test_migrate_share_with_snapshot(self):
        snap = self.create_snapshot_wait_for_active(self.share['id'])
        self.assertRaises(
            lib_exc.BadRequest, self.shares_v2_client.migrate_share,
            self.share['id'], self.dest_pool, True)
        self.shares_client.delete_snapshot(snap['id'])
        self.shares_client.wait_for_resource_deletion(snapshot_id=snap["id"])

    @test.attr(type=[base.TAG_NEGATIVE, base.TAG_API_WITH_BACKEND])
    @base.skip_if_microversion_lt("2.5")
    def test_migrate_share_same_host(self):
        self.assertRaises(
            lib_exc.BadRequest, self.shares_v2_client.migrate_share,
            self.share['id'], self.share['host'], True)

    @test.attr(type=[base.TAG_NEGATIVE, base.TAG_API_WITH_BACKEND])
    @base.skip_if_microversion_lt("2.19")
    def test_migrate_share_fallback_not_allowed(self):
        self.shares_v2_client.migrate_share(
            self.share['id'], self.dest_pool, True,
            skip_optimized_migration=True, writable=True,
            preserve_metadata=True)
        self.shares_v2_client.wait_for_migration_status(
            self.share['id'], self.dest_pool, 'migration_error')

    @test.attr(type=[base.TAG_NEGATIVE, base.TAG_API_WITH_BACKEND])
    @base.skip_if_microversion_lt("2.5")
    def test_migrate_share_invalid_share(self):
        self.assertRaises(
            lib_exc.NotFound, self.shares_v2_client.migrate_share, True,
            'invalid_share_id', self.dest_pool)

    @test.attr(type=[base.TAG_NEGATIVE, base.TAG_API_WITH_BACKEND])
    @base.skip_if_microversion_lt("2.5")
    def test_migrate_share_not_available(self):
        self.shares_client.reset_state(self.share['id'], 'error')
        self.shares_client.wait_for_share_status(self.share['id'], 'error')
        self.assertRaises(
            lib_exc.BadRequest, self.shares_v2_client.migrate_share,
            self.share['id'], self.dest_pool, True)
        self.shares_client.reset_state(self.share['id'], 'available')
        self.shares_client.wait_for_share_status(self.share['id'], 'available')

    @test.attr(type=[base.TAG_NEGATIVE, base.TAG_API_WITH_BACKEND])
    @base.skip_if_microversion_lt("2.19")
    def test_migrate_share_invalid_share_network(self):
        self.assertRaises(
            lib_exc.NotFound, self.shares_v2_client.migrate_share,
            self.share['id'], self.dest_pool, True,
            new_share_network_id='invalid_net_id')
