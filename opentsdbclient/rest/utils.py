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


STATS_TEMPL = 'http://%(host)s:%(port)s/api/stats'
PUT_TEMPL = 'http://%(host)s:%(port)s/api/put?details'
META_TEMPL = 'http://%(host)s:%(port)s/api/uid/tsmeta?tsuid=%(tsuid)s'
CONF_TEMPL = 'http://%(host)s:%(oprt)s/api/config'
AGGR_TEMPL = 'http://%(host)s:%(port)s/api/aggregators'
VERSION_TEMPL = 'http://%(host)s:%(port)s/api/version'
QUERY_TEMPL = 'http://%(host)s:%(port)s/api/query?%(query)s'
