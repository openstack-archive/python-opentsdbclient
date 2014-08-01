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

import json

import mock
import requests

from opentsdbclient import client
from opentsdbclient import tests


class ClientTest(tests.BaseTestCase):
    def setUp(self):
        super(ClientTest, self).setUp()
        self.client = client.OpenTSDBClient(opentsdb_host=self.host,
                                            opentsdb_port=self.port)

    @mock.patch.object(requests, 'get')
    def test_get_statistics(self, get_mock):
        self.client.get_statistics()
        get_mock.assert_called_once_with('http://127.0.0.1:4242/api/stats')

    @mock.patch.object(requests, 'post')
    def test_put_meter(self, post_mock):
        put_dict = {'metric': 'bla', 'timestamp': '0',
                    'value': 123, 'tags': {'some_tag': 'foo'}}
        self.client.put_meter(sorted(put_dict))
        post_mock.assert_called_once_with(
            'http://127.0.0.1:4242/api/put?details', data=json.dumps(put_dict))

    @mock.patch.object(requests, 'post')
    def test_define_retention(self, post_mock):
        self.client.define_retention('foo', 12)
        post_mock.assert_called_once_with(
            'http://127.0.0.1:4242/api/uid/tsmeta?tsuid=foo',
            data='{"tsuid": "foo", "retention": 12}')

    @mock.patch.object(requests, 'get')
    def test_get_aggregators(self, get_mock):
        self.client.get_aggregators()
        get_mock.assert_called_once_with(
            'http://127.0.0.1:4242/api/aggregators')

    @mock.patch.object(requests, 'get')
    def test_get_version(self, get_mock):
        self.client.get_version()
        get_mock.assert_called_once_with('http://127.0.0.1:4242/api/version')

    @mock.patch.object(requests, 'get')
    def test_get_query(self, get_mock):
        self.client.get_query('start=0&end=12&m=max:2-min:bla')
        get_mock.assert_called_once_with(
            'http://127.0.0.1:4242/api/query?start=0&end=12&m=max:2-min:bla')
