#! /usr/bin/env

#  Copyright 2014 TangoMe Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import boto.sns
from notifier import Notifier


class AmazonSNS(Notifier):

    def __init__(self, **kwargs):
        '''amazon sns'''
        super(AmazonSNS, self).__init__()
        self._level = kwargs.get('level', 'caution')
        self._conn = boto.sns.connect_to_region(kwargs.get('aws_region', 'us-west-2'),
                aws_access_key_id = kwargs.get('aws_access_key_id', None),
                aws_secret_access_key = kwargs.get('aws_secret_access_key', None))
        self._sns_topic = kwargs.get('sns_topic_{0}'.format(self._level), None)


    def __str__(self):
        return 'Amazon SNS'

    def send(self, alerts):
        if not self._sns_topic:
            self._debug('noop: topic not available for {0}'.format(self))
            return

        if not self._conn:
            raise RuntimeError('{0} connection is not established'.format(self))

        self._info("publishing to amazon sns topic: {0} level: {1}".format(self._sns_topic, self._level))

        subject = '{0}: some New Relic metrics on failing'.format(self._level.upper())
        msgs = [ alert.summary() for alert in alerts ]

        return self._conn.publish(topic=self._sns_topic, subject=subject, message='\n\n'.join(msgs))

    def send_summary(self, summary, details):
        if not self._sns_topic:
            self._debug('noop: topic not available for {0}'.format(self))
            return

        if not self._conn:
            raise RuntimeError('{0} connection is not established'.format(self))

        self._info("publishing to amazon sns topic: {0} level: {1}".format(self._sns_topic, self._level))


        return self._conn.publish(topic=self._sns_topic, subject=summary, message=details)

    def publish_message(self, subject, message):
        if not self._conn:
            raise RuntimeError('{0} connection is not established'.format(self))

        return self._conn.publish(topic=self._sns_topic, subject=subject, message=message)


