# Copyright 2016 Hitachi Data Systems.
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
Module provides possibility for share drivers to send
requests to Data Service.
"""

from manila.data import rpcapi as data_rpc
from manila.db import api as db_api
from manila.share import rpcapi as share_rpc


class DataServiceHelper(object):

    def __init__(self):
        self.db = db_api
        self.data_rpc = data_rpc.DataAPI()
        self.share_rpc = share_rpc.ShareAPI()
        return

    def copy_share_data(self, context, src_share_id, src_path, dest_share_id,
                        dest_path, callback=None):

        src_share_instance_ref = self.db.share_instance_get(
            context, src_share_id, with_share_data=True)
        dest_share_instance_ref = self.db.share_instance_get(
            context, dest_share_id, with_share_data=True)

        src_share_server = None
        if src_share_instance_ref['share_server_id']:
            src_share_server = self.db.share_server_get(
                context, src_share_instance_ref['share_server_id'])
            src_share_server = {
                'id': src_share_server['id'],
                'share_network_id': src_share_server['share_network_id'],
                'host': src_share_server['host'],
                'status': src_share_server['status'],
                'backend_details': src_share_server['backend_details'],
            } if src_share_server else src_share_server

        dest_share_server = None
        if dest_share_instance_ref['share_server_id']:
            dest_share_server = self.db.share_server_get(
                context, dest_share_instance_ref['share_server_id'])
            dest_share_server = {
                'id': dest_share_server['id'],
                'share_network_id': dest_share_server['share_network_id'],
                'host': dest_share_server['host'],
                'status': dest_share_server['status'],
                'backend_details': dest_share_server['backend_details'],
            } if dest_share_server else dest_share_server

        migration_info_src = self.share_rpc.get_migration_info(
            context, src_share_instance_ref, src_share_server)
        migration_info_dest = self.share_rpc.get_migration_info(
            context, dest_share_instance_ref, dest_share_server)

        self.data_rpc.copy_share_data(
            context, src_share_instance_ref['id'],
            dest_share_instance_ref['id'], src_path, dest_path,
            src_share_instance_ref['id'], dest_share_instance_ref['id'],
            migration_info_src, migration_info_dest, callback)

    def data_copy_cancel(self, context, src_share_id):

        src_share_instance_ref = self.db.share_instance_get(
            context, src_share_id, with_share_data=True)

        self.data_rpc.data_copy_cancel(
            context, src_share_instance_ref['share_id'])

    def data_copy_get_progress(self, context, src_share_id):

        src_share_instance_ref = self.db.share_instance_get(
            context, src_share_id, with_share_data=True)

        return self.data_rpc.data_copy_get_progress(
            context, src_share_instance_ref['share_id'])
