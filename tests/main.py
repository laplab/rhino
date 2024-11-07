import unittest
import json
import websocket
from contextlib import closing
from ulid import ulid as ULID
import time
import fdb
from copy import deepcopy

fdb.api_version(710)

CONTROL_ADDR = 'ws://localhost:3003'
EU_ADDR = 'ws://localhost:3002'
US_ADDR = 'ws://localhost:3004'
API_ADDR = 'ws://localhost:3001'

class TestRhino(unittest.TestCase):
    def send_recv(self, ws, payload):
        cid = str(ULID())
        ws.send(json.dumps({'correlation_id': cid, 'payload': payload}))

        response = json.loads(ws.recv())
        self.assertEqual(cid, response['correlation_id'])
        return response['payload']

    def ws(self, addr):
        return closing(websocket.create_connection(addr))

    def setUp(self):
        # Remove all data from clusters.
        for cluster in ['fdb-central', 'fdb-eu', 'fdb-us']:
            db = fdb.open(f'configs/fdb/{cluster}.cluster')
            db.clear_range(b'', b'\xFF')

    def test_login(self):
        for region in [EU_ADDR, US_ADDR]:
            with self.ws(region) as ws:
                self.assertEqual(
                    self.send_recv(ws, {'GetTenantIdByToken': {'token': 'test_token'}}),
                    'InvalidToken'
                )

        with self.ws(API_ADDR) as ws:
            self.assertEqual(
                self.send_recv(ws, {'Login': {'token': 'test_token'}}),
                'InvalidToken'
            )

        with self.ws(CONTROL_ADDR) as ws:
            self.assertEqual(
                self.send_recv(ws, {'AddGlobalTenant': {'tenant_id': 'test_tenant', 'token': 'test_token'}}),
                'GlobalTenantAdded'
            )

        # Wait for the replication to happen
        time.sleep(2)

        for region in [EU_ADDR, US_ADDR]:
            with self.ws(region) as ws:
                self.assertEqual(
                    self.send_recv(ws, {'GetTenantIdByToken': {'token': 'test_token'}}),
                    {'TenantIdByToken': {'tenant_id': 'test_tenant'}}
                )

        with self.ws(API_ADDR) as ws:
            self.assertEqual(
                self.send_recv(ws, {'Login': {'token': 'test_token'}}),
                {'LoggedIn': {'region': 'eu-west-2'}}
            )

    def test_routing(self):
        for region in [EU_ADDR, US_ADDR, CONTROL_ADDR]:
            with self.ws(region) as ws:
                self.assertEqual(
                    self.send_recv(ws, {'GetShardRegion': {'tenant_id': 'test_tenant', 'shard_name': 'test_shard'}}),
                    'ShardNotFound',
                )

        # Add a test tenant
        with self.ws(CONTROL_ADDR) as ws:
            self.assertEqual(
                self.send_recv(ws, {'AddGlobalTenant': {'tenant_id': 'test_tenant', 'token': 'test_token'}}),
                'GlobalTenantAdded'
            )

        # Wait for the replication of tokens to happen
        time.sleep(2)

        with self.ws(API_ADDR) as ws:
            self.assertEqual(
                self.send_recv(ws, {'Login': {'token': 'test_token'}}),
                {'LoggedIn': {'region': 'eu-west-2'}}
            )

            self.assertEqual(
                self.send_recv(ws, {'CreateShard': {'name': 'test_shard', 'region': 'us-east-2'}}),
                'ShardCreated'
            )

            for region in ['eu-west-2', 'us-east-2']:
                self.assertEqual(
                    self.send_recv(ws, {'CreateShard': {'name': 'test_shard', 'region': region}}),
                    'ShardAlreadyExists'
                )

        # Wait for the replication of routing information to happen
        time.sleep(2)

        for region in [EU_ADDR, US_ADDR, CONTROL_ADDR]:
            with self.ws(region) as ws:
                self.assertEqual(
                    self.send_recv(ws, {'GetShardRegion': {'tenant_id': 'test_tenant', 'shard_name': 'test_shard'}}),
                    {'ShardLocated': {'region': 'us-east-2'}},
                )

    def test_schema(self):
        for region in [EU_ADDR, US_ADDR, CONTROL_ADDR]:
            with self.ws(region) as ws:
                self.assertEqual(
                    self.send_recv(ws, {'GetTableSchema': {'tenant_id': 'test_tenant', 'table_name': 'test_table'}}),
                    'TableNotFound',
                )

        # Add a test tenant
        with self.ws(CONTROL_ADDR) as ws:
            self.assertEqual(
                self.send_recv(ws, {'AddGlobalTenant': {'tenant_id': 'test_tenant', 'token': 'test_token'}}),
                'GlobalTenantAdded'
            )

        # Wait for the replication of tokens to happen
        time.sleep(2)

        with self.ws(API_ADDR) as ws:
            self.assertEqual(
                self.send_recv(ws, {'CreateTable': {'name': 'test_table', 'pk': ['first', 'second']}}),
                'NotLoggedIn'
            )

            self.assertEqual(
                self.send_recv(ws, {'Login': {'token': 'test_token'}}),
                {'LoggedIn': {'region': 'eu-west-2'}}
            )

            self.assertEqual(
                self.send_recv(ws, {'CreateTable': {'name': 'test_table', 'pk': ['first', 'second']}}),
                'TableCreated'
            )

            self.assertEqual(
                self.send_recv(ws, {'CreateTable': {'name': 'test_table', 'pk': ['second', 'first']}}),
                'TableAlreadyExists'
            )

        # Wait for the replication of schema information to happen
        time.sleep(2)

        for region in [EU_ADDR, US_ADDR, CONTROL_ADDR]:
            with self.ws(region) as ws:
                self.assertEqual(
                    self.send_recv(ws, {'GetTableSchema': {'tenant_id': 'test_tenant', 'table_name': 'test_table'}}),
                    {'TableSchema': {'pk': ['first', 'second']}},
                )

    def test_get_set(self):
        # Add a test tenant
        with self.ws(CONTROL_ADDR) as ws:
            self.assertEqual(
                self.send_recv(ws, {'AddGlobalTenant': {'tenant_id': 'test_tenant', 'token': 'test_token'}}),
                'GlobalTenantAdded'
            )

        # Wait for the replication of tokens to happen
        time.sleep(2)
        def _create_table():
            with self.ws(API_ADDR) as ws:
                self.assertEqual(
                    self.send_recv(ws, {'Login': {'token': 'test_token'}}),
                    {'LoggedIn': {'region': 'eu-west-2'}}
                )

                self.assertEqual(
                    self.send_recv(ws, {'CreateTable': {'name': 'test_table', 'pk': ['first', 'second']}}),
                    'TableCreated'
                )

                # Wait for the replication of schema information to happen
                time.sleep(2)

        def _test_api_server():
            row = {
                'first': {
                    'String': 'third_value',
                },
                'second': {
                    'String': 'forth_value',
                },
                'something_else': {
                    'U64': 5678,
                },
            }

            with self.ws(API_ADDR) as ws:
                self.assertEqual(
                    self.send_recv(ws, {'Login': {'token': 'test_token'}}),
                    {'LoggedIn': {'region': 'eu-west-2'}}
                )
                # Set
                self.assertEqual(
                    self.send_recv(ws, {'SetRow': {'shard_name': 'test_shard', 'table_name': 'test_table', 'row': row}}),
                    'ShardNotFound',
                )

                # self.assertEqual(
                #     self.send_recv(ws, {'SetRow': {'shard_name': 'test_shard', 'table_name': 'invalid_table', 'row': row}}),
                #     'TableNotFound',
                # )

                self.assertEqual(
                    self.send_recv(ws, {'CreateShard': {'name': 'test_shard', 'region': 'us-east-2'}}),
                    'ShardCreated'
                )

                self.assertEqual(
                    self.send_recv(ws, {'SetRow': {'shard_name': 'test_shard', 'table_name': 'test_table', 'row': {}}}),
                    {'MissingPkValue': {'component': 'first'}},
                )

                self.assertEqual(
                    self.send_recv(ws, {'SetRow': {'shard_name': 'test_shard', 'table_name': 'test_table', 'row': row}}),
                    'SetRowCompleted',
                )

                # Get
                self.assertEqual(
                    self.send_recv(ws, {'GetRow': {'shard_name': 'test_shard', 'table_name': 'invalid_table', 'pk': row}}),
                    'TableNotFound',
                )

                self.assertEqual(
                    self.send_recv(ws, {'GetRow': {'shard_name': 'test_shard', 'table_name': 'test_table', 'pk': {}}}),
                    {'MissingPkValue': {'component': 'first'}},
                )

                self.assertEqual(
                    self.send_recv(ws, {'GetRow': {'shard_name': 'test_shard', 'table_name': 'test_table', 'pk': row}}),
                    {'RowFound': {'row': row}}
                )

                non_existing = deepcopy(row)
                non_existing['first'] = {'String': 'non_existing'}
                self.assertEqual(
                    self.send_recv(ws, {'GetRow': {'shard_name': 'test_shard', 'table_name': 'test_table', 'pk': non_existing}}),
                    'RowNotFound'
                )


        def _test_region_server():
            row = {
                'first': {
                    'String': 'first_value',
                },
                'second': {
                    'String': 'second_value',
                },
                'something_else': {
                    'U64': 1234,
                },
            }
            for region in [EU_ADDR, US_ADDR]:
                with self.ws(region) as ws:
                    # Set
                    self.assertEqual(
                        self.send_recv(ws, {'SetRow': {'tenant_id': 'invalid_tenant', 'shard_name': 'test_shard', 'table_name': 'test_table', 'row': row}}),
                        'TableNotFound',
                    )

                    self.assertEqual(
                        self.send_recv(ws, {'SetRow': {'tenant_id': 'test_tenant', 'shard_name': 'test_shard', 'table_name': 'invalid_table', 'row': row}}),
                        'TableNotFound',
                    )

                    self.assertEqual(
                        self.send_recv(ws, {'SetRow': {'tenant_id': 'test_tenant', 'shard_name': 'test_shard', 'table_name': 'test_table', 'row': {}}}),
                        {'MissingPkValue': {'component': 'first'}},
                    )

                    self.assertEqual(
                        self.send_recv(ws, {'SetRow': {'tenant_id': 'test_tenant', 'shard_name': 'test_shard', 'table_name': 'test_table', 'row': row}}),
                        'SetRowCompleted',
                    )

                    # Get
                    self.assertEqual(
                        self.send_recv(ws, {'GetRow': {'tenant_id': 'invalid_tenant', 'shard_name': 'test_shard', 'table_name': 'test_table', 'pk': row}}),
                        'TableNotFound',
                    )

                    self.assertEqual(
                        self.send_recv(ws, {'GetRow': {'tenant_id': 'test_tenant', 'shard_name': 'test_shard', 'table_name': 'invalid_table', 'pk': row}}),
                        'TableNotFound',
                    )

                    self.assertEqual(
                        self.send_recv(ws, {'GetRow': {'tenant_id': 'test_tenant', 'shard_name': 'test_shard', 'table_name': 'test_table', 'pk': {}}}),
                        {'MissingPkValue': {'component': 'first'}},
                    )

                    self.assertEqual(
                        self.send_recv(ws, {'GetRow': {'tenant_id': 'test_tenant', 'shard_name': 'test_shard', 'table_name': 'test_table', 'pk': row}}),
                        {'RowFound': {'row': row}}
                    )

                    non_existing = deepcopy(row)
                    non_existing['first'] = {'String': 'non_existing'}
                    self.assertEqual(
                        self.send_recv(ws, {'GetRow': {'tenant_id': 'test_tenant', 'shard_name': 'test_shard', 'table_name': 'test_table', 'pk': non_existing}}),
                        'RowNotFound'
                    )
        _create_table()
        _test_api_server()
        _test_region_server()


if __name__ == '__main__':
    unittest.main()
