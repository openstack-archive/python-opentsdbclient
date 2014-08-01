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

import requests

import opentsdbclient
from opentsdbclient import utils


class OpenTSDBClient(object):
    def __init__(self, opentsdb_host, opentsdb_port):
        self.host = opentsdb_host
        self.port = opentsdb_port

    def get_statistics(self):
        """Get info about what metrics are registered and with what stats."""
        req = requests.get(utils.STATS_TEMPL % {'host': self.host,
                                                'port': self.port})
        return req

    def put_meter(self, meters):
        """Post new meter(s) to the database.

        Meter dictionary *should* contain the following four required fields:
          - metric: the name of the metric you are storing
          - timestamp: a Unix epoch style timestamp in seconds or milliseconds.
                       The timestamp must not contain non-numeric characters.
          - value: the value to record for this data point. It may be quoted or
                   not quoted and must conform to the OpenTSDB value rules.
          - tags: a map of tag name/tag value pairs. At least one pair must be
                  supplied.
        """
        res = []
        if type(meters) == dict:
            meters = [meters]
        for meter_dict in meters:
            if (set(meter_dict.keys())
                    != set(['metric', 'timestamp', 'value', 'tags'])):
                raise opentsdbclient.InvalidOpenTSDBFormat(
                    actual=meter_dict,
                    expected="{'metric': <meter_name>, 'timestamp': <ts>, "
                             "'value': <value>, 'tags': <at least one pair>}")

            req = requests.post(utils.PUT_TEMPL %
                                {'host': self.host, 'port': self.port},
                                data=json.dumps(meter_dict))
            res.append(req)
        return res

    def define_retention(self, tsuid, retention_days):
        """Set retention days for the defined by ID timeseries.

        ##########################################################
        NOTE: currently not working directly through the REST API.
              that should be done directly on the HBase level.
        ##########################################################

        :param tsuid: hexadecimal representation of the timeseries UID
        :param retention_days: number of days of data points to retain for the
                               given timeseries. When set to 0, the default,
                               data is retained indefinitely.
        """
        meta_data = {'tsuid': tsuid, 'retention': retention_days}
        req = requests.post(utils.META_TEMPL % {'host': self.host,
                                                'port': self.port,
                                                'tsuid': tsuid},
                            data=json.dumps(meta_data))
        return req

    def get_aggregators(self):
        """Used to get the list of default aggregation functions."""
        req = requests.get(utils.AGGR_TEMPL % {'host': self.host,
                                               'port': self.port})
        return req

    def get_version(self):
        """Used to check OpenTSDB version.

        That might be needed in case of unknown bugs - this code is written
        only for the 2.x REST API version, so some of the failures might refer
        to the wrong OpenTSDB version installed.
        """
        req = requests.get(utils.VERSION_TEMPL % {'host': self.host,
                                                  'port': self.port})
        return req

    def _make_query(self, query, verb):
        meth = getattr(requests, verb.lower(), None)
        if meth is None:
            pass
        req = meth(utils.QUERY_TEMPL % {'host': self.host, 'port': self.port,
                                        'query': query})
        return req

    def get_query(self, query):
        return self._make_query(query, 'get')

    def process_response(self, resp):
        try:
            res = json.loads(resp.text)
        except Exception:
            raise opentsdbclient.OpenTSDBError(resp.text)

        if 'errors' in res:
            raise opentsdbclient.OpenTSDBError(res['error'])

        return res
