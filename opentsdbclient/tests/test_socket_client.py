# Copyright 2014: Mirantis Inc.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import socket
import time

import mock

from opentsdbclient.socket import client
from opentsdbclient import tests


class SocketClientTest(tests.BaseTestCase):
    def setUp(self):
        super(SocketClientTest, self).setUp()
        self.client = client.SocketOpenTSDBClient(opentsdb_hosts=[(self.host,
                                                                   self.port)],
                                                  send_queue_max_size=1)
        self.client.tsd = mock.MagicMock()
        self.client.tsd.sendall = mock.MagicMock()
        self.meters = [{'metric': 'bla1', 'timestamp': 12345,
                        'value': 123, 'tags': {'some_tag': 'foo'}},
                       {'metric': 'bla2', 'timestamp': 23456,
                        'value': 123, 'tags': {'some_tag': 'foo'}}]

    def test_put_meter(self):
        self.client.maintain_connection = mock.MagicMock()

        self.client.put_meter(self.meters)
        self.client.tsd.sendall.assert_called_once_with(
            'put bla1 12345 123 some_tag=foo\n'
            'put bla2 23456 123 some_tag=foo\n')

    def test_send_data(self):
        self.client.send_queue = self.meters
        self.client.send_data()
        self.client.tsd.sendall.assert_called_once_with(
            'put bla1 12345 123 some_tag=foo\n'
            'put bla2 23456 123 some_tag=foo\n')

    def test_compose_line_from_meter(self):
        res = self.client.compose_line_from_meter(self.meters[0])
        self.assertEqual('bla1 12345 123 some_tag=foo', res)

    @mock.patch.object(socket, 'getaddrinfo')
    @mock.patch.object(socket, 'socket')
    def test_maintain_connection(self, sock_mock, sock_addr_mock):
        self.client.verify_connection = mock.MagicMock()
        pop_list = [True, False]
        self.client.verify_connection.side_effect = lambda: pop_list.pop()
        sock_addr_mock.side_effect = lambda a, b, c, d, e: [(1, 2, 3, 4, 5)]
        tsd = mock.MagicMock()
        tsd.connect = mock.MagicMock()
        sock_mock.side_effect = tsd
        self.client.maintain_connection()
        sock_addr_mock.assert_called_once_with(self.host, self.port,
                                               socket.AF_UNSPEC,
                                               socket.SOCK_STREAM, 0)
        sock_mock.assert_called_once_with(1, 2, 3)
        self.client.tsd.connect.assert_called_once_with(5)

    def test_verify_connection_non_tsd(self):
        self.client.tsd = None
        self.assertFalse(self.client.verify_connection())

    def test_verify_connection_tsd(self):
        self.assertTrue(self.client.verify_connection())

    def test_verify_connection_tsd_reconnect(self):
        self.client.last_verify = time.time() - 3600
        self.client.reconnect_interval = 1
        self.assertFalse(self.client.verify_connection())
