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

import ddt
from tempest import config
from tempest import test

from manila_tempest_tests.tests.api import base
from manila_tempest_tests import utils

CONF = config.CONF


@ddt.ddt
class MigrationNFSTest(base.BaseSharesAdminTest):
    """Tests Share Migration for NFS shares.

    Tests migration of NFS shares in multi-backend environment.

    This class covers:
    1) Optimized migration: skip_optimized_migration, writable and
    preserve-metadata are False.
    2) Fallback migration: skip_optimized_migration is True, writable and
    preserve-metadata are False.
    3) 2-phase migration of both Fallback and Optimized: complete is False.

    No need to test with writable and preserve-metadata is True, values are
    supplied to the driver which decides what to do. Test should be positive,
    so not being writable and not preserving metadata is less restrictive for
    drivers, which would abort if they cannot handle them.

    Drivers that implement optimized migration should enable the configuration
    flag to be tested.
    """

    protocol = "nfs"

    @classmethod
    def resource_setup(cls):
        super(MigrationNFSTest, cls).resource_setup()
        if cls.protocol not in CONF.share.enable_protocols:
            message = "%s tests are disabled" % cls.protocol
            raise cls.skipException(message)
        if len(CONF.share.backend_names) < 2:
            raise cls.skipException("For running migration tests it is "
                                    "required two names in config. Skipping.")
        if not (CONF.share.run_fallback_migration_tests or
                        CONF.share_run_optimized_migration_tests):
            raise cls.skipException("Share migration tests disabled. "
                                    "Skipping.")

    @test.attr(type=[base.TAG_POSITIVE, base.TAG_BACKEND])
    @base.skip_if_microversion_lt("2.5")
    @ddt.data(True, False)
    def test_migration_empty(self, skip_optimized):

        self._check_migration_enabled(skip_optimized)

        share, dest_pool = self._setup_migration()

        new_share_network_id = self._create_secondary_share_network(
            share['share_network_id'])

        share = self.migrate_share(
            share['id'], dest_pool, skip_optimized_migration=skip_optimized,
            new_share_network_id=new_share_network_id)

        self._validate_migration_successful(
            dest_pool, share, share_network_id=new_share_network_id)

    @test.attr(type=[base.TAG_POSITIVE, base.TAG_BACKEND])
    @base.skip_if_microversion_lt("2.15")
    @ddt.data(True, False)
    def test_migration_2phase_empty(self, skip_optimized):

        self._check_migration_enabled(skip_optimized)

        share, dest_pool = self._setup_migration()

        old_exports = self.shares_v2_client.list_share_export_locations(
            share['id'])
        self.assertNotEmpty(old_exports)
        old_exports = [x['path'] for x in old_exports
                       if x['is_admin_only'] is False]
        self.assertNotEmpty(old_exports)

        status = ('data_copying_completed' if skip_optimized else
                  'migration_driver_phase1_done')

        old_share_network_id = share['share_network_id']
        new_share_network_id = self._create_secondary_share_network(
            old_share_network_id)

        share = self.migrate_share(
            share['id'], dest_pool, complete=False,
            skip_optimized_migration=skip_optimized,
            wait_for_status=status, new_share_network_id=new_share_network_id)

        self._validate_migration_successful(
            dest_pool, share, complete=False,
            share_network_id=old_share_network_id)

        share = self.migration_complete(share['id'], dest_pool)

        self._validate_migration_successful(
            dest_pool, share, complete=True,
            share_network_id=new_share_network_id)

    def _setup_migration(self):

        pools = self.shares_client.list_pools()['pools']

        if len(pools) < 2:
            raise self.skipException("At least two different pool entries "
                                     "are needed to run migration tests. "
                                     "Skipping.")

        share = self.create_share(self.protocol)
        share = self.shares_client.get_share(share['id'])

        self.shares_v2_client.create_access_rule(
            share['id'], access_to="50.50.50.50", access_level="rw")

        self.shares_v2_client.wait_for_share_status(
            share['id'], 'active', status_attr='access_rules_status')

        self.shares_v2_client.create_access_rule(
            share['id'], access_to="51.51.51.51", access_level="ro")

        self.shares_v2_client.wait_for_share_status(
            share['id'], 'active', status_attr='access_rules_status')

        dest_pool = next(
            (x for x in pools if (x['name'] != share['host'] and any(
                y in x['name'] for y in CONF.share.backend_names))), None)

        self.assertIsNotNone(dest_pool)
        self.assertIsNotNone(dest_pool.get('name'))

        dest_pool = dest_pool['name']

        return share, dest_pool

    def _validate_migration_successful(self, dest_pool, share,
                                       version=CONF.share.max_api_microversion,
                                       complete=True, share_network_id=None):
        if utils.is_microversion_lt(version, '2.9'):
            new_exports = share['export_locations']
            self.assertNotEmpty(new_exports)
        else:
            new_exports = self.shares_v2_client.list_share_export_locations(
                share['id'], version=version)
            self.assertNotEmpty(new_exports)
            new_exports = [x['path'] for x in new_exports if
                           x['is_admin_only'] is False]
            self.assertNotEmpty(new_exports)

        # Share migrated
        if complete:
            self.assertEqual(dest_pool, share['host'])
            self.assertEqual('migration_success', share['task_state'])
            self.shares_v2_client.delete_share(share['id'])
            self.shares_v2_client.wait_for_resource_deletion(
                share_id=share['id'])
        # Share not migrated yet
        else:
            self.assertNotEqual(dest_pool, share['host'])
            self.assertIn(share['task_state'],
                          ('data_copying_completed',
                           'migration_driver_phase1_done'))
        if share_network_id:
            self.assertEqual(share_network_id, share['share_network_id'])

    def _check_migration_enabled(self, skip_optimized):

        if skip_optimized:
            if not CONF.share.run_fallback_migration_tests:
                raise self.skipException(
                    "Fallback migration tests disabled. Skipping.")
        else:
            if not CONF.share.run_optimized_migration_tests:
                raise self.skipException(
                    "Optimized migration tests disabled. Skipping.")

    def _create_secondary_share_network(self, old_share_network_id):
        if (utils.is_microversion_ge(
                CONF.share.max_api_microversion, "2.19") and
                CONF.share.multitenancy_enabled):

            old_share_network = self.shares_v2_client.get_share_network(
                old_share_network_id)

            new_share_network = self.create_share_network(
                cleanup_in_class=True,
                neutron_net_id=old_share_network['neutron_net_id'],
                neutron_subnet_id=old_share_network['neutron_subnet_id'])

            return new_share_network['id']
        else:
            return None
