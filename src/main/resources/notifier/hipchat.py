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
import json
import urllib2
from notifier import Notifier


class HipChat(Notifier):


    def __init__(self, **kwargs):
        self._api_key = kwargs.get('api_key')
        self._room_name = kwargs.get('room_name')
        self._header = {'content-type':'application/json'}


    def __str__(self):
        return 'HipChat'


    def publish_message(self, subject, message):
        url = "https://api.hipchat.com/v2/room/" + str(self._room_name) + "/notification"
        data = json.dumps({"message":subject})
        req = urllib2.Request(url+"?auth_token="+self._api_key, data, self._header)
        res = urllib2.urlopen(req)
        return res.read()


    def send_message(self, room_id_or_name, message):
        url = "https://api.hipchat.com/v2/room/" + str(room_id_or_name) + "/notification"
        data = json.dumps({"message":message})
        req = urllib2.Request(url+"?auth_token="+self._api_key, data, self._header)
        res = urllib2.urlopen(req)
        return res.read()





