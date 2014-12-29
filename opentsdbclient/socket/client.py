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

import copy
import itertools
import logging
import random
import socket
import time

import six

from opentsdbclient import base

LOG = logging.getLogger('opentsdb_client')


class SocketOpenTSDBClient(base.BaseOpenTSDBClient):

    def __init__(self, opentsdb_hosts, conn_verify_trusted_time=60,
                 reconnect_interval=0, send_queue_max_size=1000,
                 max_uncaught_exceptions=100, wait_retry=False):
        super(SocketOpenTSDBClient, self).__init__(opentsdb_hosts)
        self.send_queue = []
        self.tsd = None
        self.host = None
        self.port = None
        self.blacklisted_hosts = set()
        self.current_tsd_host = -1
        self.last_verify = 0
        self.time_reconnect = 0
        self.conn_verify_trusted_time = conn_verify_trusted_time
        self.reconnect_interval = reconnect_interval
        self.send_queue_max_size = send_queue_max_size
        self.max_uncaught_exceptions = max_uncaught_exceptions
        self.wait_retry = wait_retry

    def blacklist_tsd_host(self):
        """Marks the current TSD host we're trying to use as blacklisted."""
        LOG.info('Blacklisting %s:%s for a while', self.host, self.port)
        self.blacklisted_hosts.add((self.host, self.port))

    def verify_connection(self):
        """Is used to check is socket connection is actually working."""
        if self.tsd is None:
            return False

        # if last verifying was not so long ago, let's trust this connection
        if self.last_verify > time.time() - self.conn_verify_trusted_time:
            return True

        # if it's time to reconnect, let's close current one
        if (self.reconnect_interval > 0 and
                self.time_reconnect < time.time() - self.reconnect_interval):
            try:
                self.tsd.close()
            except socket.error:
                pass
            self.time_reconnect = time.time()
            return False

        LOG.debug('Verifying our TSD connection is alive')
        try:
            # this request is *really* light-weighted, good thing to check the
            # connectivity
            self.tsd.sendall('version\n')
        except socket.error:
            self.tsd = None
            self.blacklist_tsd_host()
            return False

        bufsize = 4096
        # read some ^^ data from socket connection to make sure it's *really*
        # alive
        try:
            buf = self.tsd.recv(bufsize)
        except socket.error:
            self.tsd = None
            self.blacklist_tsd_host()
            return False
        if not buf:
            self.tsd = None
            self.blacklist_tsd_host()
            return False

        self.last_verify = time.time()
        return True

    def pick_connection(self):
        """Picks up a random host/port connection."""
        for self.current_tsd_host in range(self.current_tsd_host + 1,
                                           len(self.hosts)):
            host_port = self.hosts[self.current_tsd_host]
            if host_port not in self.blacklisted_hosts:
                break
        else:
            LOG.info('No more healthy hosts, '
                     'retry with previously blacklisted')
            random.shuffle(self.hosts)
            self.blacklisted_hosts.clear()
            self.current_tsd_host = 0
            host_port = self.hosts[self.current_tsd_host]

        self.host, self.port = host_port
        LOG.info('Selected connection: %s:%d', self.host, self.port)

    def maintain_connection(self):
        while True:
            if self.verify_connection():
                return

            # that's just a hack to sleep some time if OpenTSDB is somehow
            # maintained at the moment
            if self.wait_retry:
                try_delay = random.randint(60, 360)
                LOG.debug('SenderThread blocking %0.2f seconds', try_delay)
                time.sleep(try_delay)

            # now actually try the connection
            self.pick_connection()

            try:
                addresses = socket.getaddrinfo(self.host, self.port,
                                               socket.AF_UNSPEC,
                                               socket.SOCK_STREAM, 0)
            except socket.gaierror as e:
                # Don't croak on transient DNS resolution issues.
                if e[0] in (socket.EAI_AGAIN, socket.EAI_NONAME,
                            socket.EAI_NODATA):
                    LOG.debug('DNS resolution failure: %s: %s', self.host, e)
                    continue
                raise
            for family, socket_type, proto, canon_name, sock_addr in addresses:
                try:
                    self.tsd = socket.socket(family, socket_type, proto)
                    self.tsd.settimeout(15)
                    self.tsd.connect(sock_addr)
                    # if we get here it connected
                    LOG.debug('Connection to %s was successful'
                              % (str(sock_addr)))
                    break
                except socket.error as e:
                    LOG.warning('Connection attempt failed to %s:%d: %s',
                                self.host, self.port, e)
                self.tsd.close()
                self.tsd = None
            if not self.tsd:
                LOG.error('Failed to connect to %s:%d', self.host, self.port)
                self.blacklist_tsd_host()

    def put_meter(self, meters, commit=False):
        """Post new meter(s) to the database.

        :param meters: dictionary containing only four required fields:
                       - metric: the name of the metric you are storing
                       - timestamp: a Unix epoch style timestamp in seconds or
                         milliseconds. The timestamp must not contain
                         non-numeric characters.
                       - value: the value to record for this data point.
                         It may be quoted or not quoted and must conform to the
                         OpenTSDB value rules.
                       - tags: a map of tag name/tag value pairs. At least one
                         pair must be supplied.
        :param commit: bool variable defining if data sending *should* be
                       processed immediately (no matter if queue is full or
                       not)
        """

        # put meter to the send_queue and check if it's time to send it to the
        # OpenTSDB
        meters = self._check_meters(meters)
        self.send_queue = list(itertools.chain(self.send_queue, meters))

        if len(self.send_queue) <= self.send_queue_max_size and not commit:
            return

        self.maintain_connection()

        errors = 0
        try:
            self.send_data()
        except (ArithmeticError, EOFError, EnvironmentError, LookupError,
                ValueError):
            errors += 1
            if errors > self.max_uncaught_exceptions:
                raise
            LOG.exception('Uncaught exception while trying to send meters, '
                          'ignoring')
        except Exception:
            LOG.exception('Uncaught exception in while trying to send meters, '
                          'going to raise. Max number %s of uncaught errors '
                          'has been collected' % self.max_uncaught_exceptions)
            raise

    @staticmethod
    def compose_line_from_meter(m_dict):
        meter_dict = copy.deepcopy(m_dict)
        tags = meter_dict.pop('tags')
        tags_str = ''.join(' %s=%s' % (k, v) for k, v in six.iteritems(tags))
        line = '%(metric)s %(timestamp)d %(value)s' % meter_dict
        meter_dict['tags'] = tags
        return '%(metric)s%(tags)s' % {'metric': line, 'tags': tags_str}

    def send_data(self):
        req = ''.join("put %s\n" % self.compose_line_from_meter(meter_dict)
                      for meter_dict in self.send_queue)

        try:
            self.tsd.sendall(req)
            self.send_queue = []
        except socket.error as e:
            LOG.error('failed to send data: %s', e)
            try:
                self.tsd.close()
            except socket.error:
                pass
            self.tsd = None
            self.blacklist_tsd_host()
