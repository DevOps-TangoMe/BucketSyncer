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

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import datetime
import pprint
import hipchat

SUMMARY_DTS = '%Y%m%d'

def get_config_val(config, section, option, default):
    return config.get(section, option) if config.has_option(section, option) else default

def daily_summary(alerts, stats, summary_file_prefix):
    now = datetime.datetime.utcnow()
    yesterday = now - datetime.timedelta(days=1)

    sum_file = '{0}.{1}'.format(summary_file_prefix, yesterday.strftime(SUMMARY_DTS))
    yesterday_alerts, yesterday_summaries = None, {}
    if os.path.exists(sum_file):
        _ctxt = {}
        execfile(sum_file, globals(), _ctxt)
        yesterday_alerts = _ctxt.get('alerts', [])
        yesterday_summaries = {}
        for _s in _ctxt.get('summaries', []):
            for _nn, _ss in _s.iteritems():
                for _lvl, _sum in _ss.iteritems():
                    if _sum:
                        yesterday_summaries.setdefault(_nn, {}).setdefault(_lvl, []).append(_sum)
            
        os.remove(sum_file)

    ## today's 
    sum_file = '{0}.{1}'.format(summary_file_prefix, now.strftime(SUMMARY_DTS))
    summaries = [ stats ]
    if os.path.exists(sum_file):
        _ctxt = {}
        execfile(sum_file, globals(), _ctxt)
        alerts += _ctxt.get('alerts', [])
        summaries += _ctxt.get('summaries', [])

    with open(sum_file, "w") as output:
        output.write("alerts = " + pprint.PrettyPrinter(indent=4, width=1).pformat(alerts) + "\n")
        output.write("summaries = " + pprint.PrettyPrinter(indent=4, width=1).pformat(summaries) + "\n")

    return yesterday_alerts, yesterday_summaries
   
class HipChatClient(object):
    def __init__(self, api_key, room_name):
        self._api_key = api_key
        self._room_name = room_name
        self._agent = hipchat.HipChat(token=self._api_key)

    def message(self, source, msg):
        self._agent.message_room(self._room_name, source, msg)
