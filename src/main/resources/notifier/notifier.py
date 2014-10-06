#! /usr/bin/env python

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

from collections import namedtuple
import pyclbr

class Alert(namedtuple('Alert', (
        'app', 'begin_time', 'end_time', 'metric', 'threshold', 'value', 'instance', 'level'
        ))):
    '''a dictionary wrapper''' 

    def summary(self):
        fmt = '{0:>20}:  {1}'
        msgs = []
        if self.app:
            msgs.append(fmt.format('application', self.app))
        msgs.append(fmt.format('metric', self.metric))
        msgs.append(fmt.format('level', self.level))
        msgs.append(fmt.format('timespan', self.begin_time + ' - ' + self.end_time))
        msgs.append(fmt.format('instance' if self.app else 'server', '{0} {1} ({2})'.format(self.instance, self.value, self.threshold)))
        return '\n'.join(msgs)

    @staticmethod
    def get_topn(alerts, topn):
        topn_by_level = {}

        levels = {}
        for alert in alerts:
            levels.setdefault(alert.level, []).append(alert)

        for level, level_alerts in levels.iteritems():
            histo = {}
            for level_alert in level_alerts:
                key = level_alert.app if level_alert.app else level_alert.instance
                histo.setdefault(key, 0)
                histo[key] += 1

            topn_by_level[level] = sorted(histo.items(), key=lambda x: x[1], reverse=True)[:topn]

        return topn_by_level


class Notifier(object):

    def __init__(self):
        self._logger = None

    def use_logger(self, logger):
        '''inject a logger'''
        self._logger = logger
    
    def is_alert_nullable(self):
        '''invocable even there's no oustanding alert'''
        return False

    def send(self, alerts):
        '''send alerts'''
        return {}

    def send_summary(self, summary, details):
        '''send summary'''
        pass

    def publish_message(self, topic, subject, message):
        '''send summary'''
        pass

    def summarize(self, summaries):
        '''aggregate all summaries'''
        return {}

    def _formatted(self, msg):
        return '[{0}] {1}'.format(self, msg)

    def _info(self, msg):
        if self._logger:
            self._logger.info(self._formatted(msg))

    def _debug(self, msg):
        if self._logger:
            self._logger.debug(self._formatted(msg))

    def _warn(self, msg):
        if self._logger:
            self._logger.warn(self._formatted(msg))

def create(config, logger = None,  options = None):
    ''' instantiate notifiers '''
    if logger:
        logger.info("loading notifiers ...")

    if not config.has_option('notification', 'plugins'):
        return []

    plugins = config.get('notification', 'plugins')

    notifiers = []
    for modname in [ p.strip() for p in plugins.split(',') if p.strip() ]:
        mod = __import__(modname, globals(), locals(), [modname], -1)
        for clzname in pyclbr.readmodule(mod.__name__).keys():
            if clzname == 'Notifier':
                continue
            clz = getattr(mod, clzname)
            if issubclass(clz, Notifier):
                if logger: 
                    logger.info("instantiating notifier: {0}".format(clzname))
                inits = dict(config.items(clzname)) if config.has_section(clzname) else {}
                if options:
                    inits.update( options )

                notifier = clz(**inits)
                notifier.use_logger(logger)
                notifiers.append(notifier)

    return notifiers
