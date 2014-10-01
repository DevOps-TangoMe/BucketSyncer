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

import requests
import json
import ConfigParser
import ast


url = None
headers = None



def setUpAppmon(cfgFile):
    global url, headers
    cfg = ConfigParser.ConfigParser()
    cfg.read(cfgFile)
    url = cfg.get('Appmon','url', None)
    headers = ast.literal_eval(cfg.get('Appmon','headers', None))
    if not url or not headers:
        raise LookupError('url or headers not found in config.cfg')


def notify_appmon(payload):
    global url, headers

    try:
        json_alerts = json.dumps(payload, separators=(',', ': '))

        response = requests.post(url, data = json_alerts, headers=headers)
    except Exception as e:
        raise RuntimeError('Failed to send data to appmon: {0}'.format(e))
