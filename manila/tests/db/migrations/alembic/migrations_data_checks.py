# Copyright 2015 Mirantis inc.
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

"""
Tests data for database migrations.

All database migrations with data manipulation
(like moving data from column to the table) should have data check class:

@map_to_migration('1f0bd302c1a6') # Revision of checked db migration
class FooMigrationChecks(BaseMigrationChecks):
   def setup_upgrade_data(self, engine):
       ...

    def check_upgrade(self, engine, data):
       ...

    def check_downgrade(self, engine):
       ...

See BaseMigrationChecks class for more information.
"""

import abc

from oslo_utils import uuidutils
import six

from manila.db.migrations import utils


class DbMigrationsData(object):

    migration_mappings = {}

    methods_mapping = {
        'pre': 'setup_upgrade_data',
        'check': 'check_upgrade',
        'post': 'check_downgrade',
    }

    def __getattr__(self, item):
        parts = item.split('_')

        is_mapping_method = (
            len(parts) > 2 and parts[0] == ''
            and parts[1] in self.methods_mapping
        )

        if not is_mapping_method:
            return super(DbMigrationsData, self).__getattribute__(item)

        check_obj = self.migration_mappings.get(parts[-1], None)

        if check_obj is None:
            raise AttributeError

        check_obj.set_test_case(self)

        return getattr(check_obj, self.methods_mapping.get(parts[1]))


def map_to_migration(revision):
    def decorator(cls):
        DbMigrationsData.migration_mappings[revision] = cls()
        return cls
    return decorator


class BaseMigrationChecks(object):

    six.add_metaclass(abc.ABCMeta)

    def __init__(self):
        self.test_case = None

    def set_test_case(self, test_case):
        self.test_case = test_case

    @abc.abstractmethod
    def setup_upgrade_data(self, engine):
        """This method should be used to insert test data for migration.

        :param engine: SQLAlchemy engine
        :return: any data which will be passed to 'check_upgrade' as 'data' arg
        """

    @abc.abstractmethod
    def check_upgrade(self, engine, data):
        """This method should be used to do assertions after upgrade method.

        To perform assertions use 'self.test_case' instance property:
        self.test_case.assertTrue(True)

        :param engine: SQLAlchemy engine
        :param data: data returned by 'setup_upgrade_data'
        """

    @abc.abstractmethod
    def check_downgrade(self, engine):
        """This method should be used to do assertions after downgrade method.

        To perform assertions use 'self.test_case' instance property:
        self.test_case.assertTrue(True)

        :param engine: SQLAlchemy engine
        """


@map_to_migration('1f0bd302c1a6')
class AvailabilityZoneMigrationChecks(BaseMigrationChecks):

    valid_az_names = ('az1', 'az2')

    def _get_service_data(self, options):
        base_dict = {
            'binary': 'manila-share',
            'topic': 'share',
            'disabled': '0',
            'report_count': '100',
        }
        base_dict.update(options)
        return base_dict

    def setup_upgrade_data(self, engine):
        service_fixture = [
            self._get_service_data(
                {'deleted': 0, 'host': 'fake1', 'availability_zone': 'az1'}
            ),
            self._get_service_data(
                {'deleted': 0, 'host': 'fake2', 'availability_zone': 'az1'}
            ),
            self._get_service_data(
                {'deleted': 1, 'host': 'fake3', 'availability_zone': 'az2'}
            ),
        ]

        services_table = utils.load_table('services', engine)

        for fixture in service_fixture:
            engine.execute(services_table.insert(fixture))

    def check_upgrade(self, engine, _):
        az_table = utils.load_table('availability_zones', engine)

        for az in engine.execute(az_table.select()):
            self.test_case.assertTrue(uuidutils.is_uuid_like(az.id))
            self.test_case.assertTrue(az.name in self.valid_az_names)
            self.test_case.assertEqual('False', az.deleted)

        services_table = utils.load_table('services', engine)
        for service in engine.execute(services_table.select()):
            self.test_case.assertTrue(
                uuidutils.is_uuid_like(service.availability_zone_id)
            )

    def check_downgrade(self, engine):
        services_table = utils.load_table('services', engine)
        for service in engine.execute(services_table.select()):
            self.test_case.assertIn(
                service.availability_zone, self.valid_az_names
            )


@map_to_migration('344c1ac4747f')
class AccessRulesStatusMigrationChecks(BaseMigrationChecks):

    def _get_instance_data(self, data):
        base_dict = {}
        base_dict.update(data)
        return base_dict

    def setup_upgrade_data(self, engine):

        share_table = utils.load_table('shares', engine)

        share = {
            'id': 1,
            'share_proto': "NFS",
            'size': 0,
            'snapshot_id': None,
            'user_id': 'fake',
            'project_id': 'fake',
        }

        engine.execute(share_table.insert(share))

        rules1 = [
            {'id': 'r1', 'share_instance_id': 1, 'state': 'active',
             'deleted': 'False'},
            {'id': 'r2', 'share_instance_id': 1, 'state': 'active',
             'deleted': 'False'},
            {'id': 'r3', 'share_instance_id': 1, 'state': 'deleting',
             'deleted': 'False'},
        ]
        rules2 = [
            {'id': 'r4', 'share_instance_id': 2, 'state': 'active',
             'deleted': 'False'},
            {'id': 'r5', 'share_instance_id': 2, 'state': 'error',
             'deleted': 'False'},
        ]

        rules3 = [
            {'id': 'r6', 'share_instance_id': 3, 'state': 'new',
             'deleted': 'False'},
        ]

        instance_fixtures = [
            {'id': 1, 'deleted': 'False', 'host': 'fake1', 'share_id': 1,
             'status': 'available', 'rules': rules1},
            {'id': 2, 'deleted': 'False', 'host': 'fake2', 'share_id': 1,
             'status': 'available', 'rules': rules2},
            {'id': 3, 'deleted': 'False', 'host': 'fake3', 'share_id': 1,
             'status': 'available', 'rules': rules3},
            {'id': 4, 'deleted': 'False', 'host': 'fake4', 'share_id': 1,
             'status': 'deleting', 'rules': []},
        ]

        share_instances_table = utils.load_table('share_instances', engine)
        share_instances_rules_table = utils.load_table(
            'share_instance_access_map', engine)

        for fixture in instance_fixtures:
            rules = fixture.pop('rules')
            engine.execute(share_instances_table.insert(fixture))

            for rule in rules:
                engine.execute(share_instances_rules_table.insert(rule))

    def check_upgrade(self, engine, _):
        instances_table = utils.load_table('share_instances', engine)

        valid_statuses = {
            '1': 'active',
            '2': 'error',
            '3': 'out_of_sync',
            '4': None,
        }

        for instance in engine.execute(instances_table.select()):
            self.test_case.assertEqual(valid_statuses[instance['id']],
                                       instance['access_rules_status'])

    def check_downgrade(self, engine):
        share_instances_rules_table = utils.load_table(
            'share_instance_access_map', engine)

        valid_statuses = {
            '1': 'active',
            '2': 'error',
            '3': 'error',
            '4': None,
        }

        for rule in engine.execute(share_instances_rules_table.select()):
            valid_state = valid_statuses[rule['share_instance_id']]
            self.test_case.assertEqual(valid_state, rule['state'])
