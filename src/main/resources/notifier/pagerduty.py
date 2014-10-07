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

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import copy
import datetime
import pprint
import urllib
import json

from helper.http import http_get, http_post, do_request
from notifier import Notifier

class PagerDuty(Notifier):

    SUMMARY_STATS = { 
        'incidents_detected':                           0,
        'resolved_local':                               0,
        'resolved_api':                                 0, 
        'incidents_not_triggered_ins_processed':        0,
        'incidents_not_triggered_app_processed':        0,
        'incidents_triggered':                          0,
        'grace_period_applied':                         0,
    }

    def __init__(self, **kwargs):
        '''page duty'''
        super(PagerDuty, self).__init__()
        self._level = kwargs.get('level', 'caution')
        self._service_api_key = kwargs.get('service_api_key_{0}'.format(self._level), None)
        self._grace_period = int(kwargs.get('grace_period_{0}'.format(self._level), 0)) * 60 
        self._access_key = kwargs.get('access_key', None)
        self._outstanding_incidents_prefix = kwargs.get('outstanding_incidents_prefix', None) 
        self._response_timeout = int(kwargs.get('http_response_timeout', 0))
        self._base_headers = [ ('Content-Type', 'application/json'), ('Authorization', 'Token token={0}'.format(self._access_key)) ]
        self._trace = kwargs.get('debug', False)
        self._app_service_name = ','.join([ a.strip() for a in kwargs.get('app_service_name', '').split(',')])
        self._app_service_grace_period = int(kwargs.get('app_service_grace_period', 0))
        self._app_processed = {}
        self._retry_total = int(kwargs.get('retry_total', 1))
        self._retry_interval = int(kwargs.get('retry_interval', 3))
        self._get_request_url = int(kwargs.get('get_request_url', None))

        if self._outstanding_incidents_prefix and not self._outstanding_incidents_prefix.startswith(os.path.sep):
            self._outstanding_incidents_prefix = os.path.join(os.path.dirname(__file__), self._outstanding_incidents_prefix)

    def __str__(self):
        return 'PagerDuty'

    def is_alert_nullable(self):
        return True

    def _reportable(self, incident):
        if not self._grace_period:
            return True

        delta = incident['time_occurred'] - incident['time_created']
        reportable = delta.seconds >= self._grace_period
        self._info('grace period {4} for {5} incident: {0} first:{1} last:{2} grace_period:{3}'.format(
                incident['incident_key'], incident['time_created'], incident['time_occurred'], self._grace_period,
                'EXPIRED' if reportable else 'APPLIED', self._level))

        return reportable

    def send(self, alerts):
        if not self._service_api_key:
            self._debug('noop: service_api_key not available for {0}'.format(self))
            return

        self._debug("processing alerts: {0} api_key: {1}".format(self._level, self._service_api_key))
        self._summary = copy.deepcopy(PagerDuty.SUMMARY_STATS)

        new_incidents = self._to_incidents(alerts or [])
        self._summary['incidents_detected'] = len(new_incidents)

        resolveable_incidents = {} 
        outstanding_incidents = {} 

        for key, oi in self._load_outstanding_incidents().iteritems():
            if new_incidents.has_key(key):
                outstanding_incidents[key] = new_incidents.pop(key)
                outstanding_incidents[key]['time_created'] = oi['time_created']
                outstanding_incidents[key]['reported'] = oi['reported']
            else:
                resolveable_incidents[key] = oi

        save = []
        for incident in resolveable_incidents.values():
            if not self._resolve_incident(incident):
                save.append(incident)

        for incident in outstanding_incidents.values():
            self._report_incident(incident, True)

        for incident in new_incidents.values():
            incident['time_created'] = incident['time_occurred']
            self._report_incident(incident, False)

        save.extend(outstanding_incidents.values())
        save.extend(new_incidents.values())

        self._persist_outstanding_incidents(save)

        return self._summary 

    def summarize(self, summaries):
        summary = copy.deepcopy(PagerDuty.SUMMARY_STATS)
        for _summary in summaries:
            for k in summary:
                if k == 'incidents_not_triggered_ins_processed':
                    ## this is a renamed stat, let's double it up
                    summary[k] += _summary.get(k, 0)
                    summary[k] += _summary.get('incidents_not_trigggered', 0)
                else:
                    summary[k] += _summary.get(k, 0)

        return summary

    def _to_incidents(self, alerts):

        incidents = {}
        for alert in alerts:
            key = '{0}@{1}'.format(alert.app if alert.app else self._level, alert.instance)
            incident = incidents.setdefault(key, { 'incident_key': key, 'event_type': 'trigger', 
                'description': '{0} {2}! {1} Alert: {3}({4})'.format('ATTENTION' if alert.level == 'caution' else 'CRITICAL', alert.instance, alert.metric, alert.value, alert.threshold),
                'details' : [],
                'time_occurred': datetime.datetime.utcnow(),
                'reported': False,
                'app': alert.app
                } )
            incident['details'].append(alert)
 
        return incidents 

    def _resolve_incident(self, incident):

        if not incident['reported']:
            self._info("no need to resolve an un-reported incident: {0}".format(incident['incident_key']))
            self._summary['resolved_local'] += 1
            return True

        def _resolver(count): 
            msgs = []
            for alert in incident['details']:
                msgs.append(alert.summary())
        
            data = json.dumps({ 'service_key': self._service_api_key, 'incident_key': incident['incident_key'], 
                        'event_type':'resolve', 'description':'RESOLVED! ' + incident['description'], 'details': msgs })
            resp = http_post('https://events.pagerduty.com/generic/2010-04-15/create_event.json', body=data, **self._http_base_params())
            self._info("resolved incident: {0} on try #{2} resp: {1}".format(incident['incident_key'], resp, count+1))
            self._summary['resolved_api'] += 1

        ok, exceptions = do_request(_resolver, self._retry_total, self._retry_interval)
        for i, e in enumerate(exceptions):
            self._warn("try #{1}: failed to resolve incident: {0}".format(e, i+1)) 

        return ok

    @staticmethod
    def _timestamp(utc):
        return utc.strftime('%Y-%m-%dT%H:%M:%S%Z')

    def _incident_app_reported(self, incident):

        app_reported = None 

        if not incident['app']:
            return app_reported
        elif not self._app_service_name:
            return app_reported

        since_time = self._timestamp(incident['time_created'] - datetime.timedelta(minutes=self._app_service_grace_period))
        until_time = self._timestamp(incident['time_occurred'])
        key = '{0}-{1}'.format(incident['time_created'], incident['time_occurred'])

        if not self._app_processed.has_key(key):
            qs = { 
                'fields':       'incident_number,created_on,status,service,trigger_summary_data', 
                'service':      self._app_service_name,
                'since':        since_time,
                'utill':        until_time,
                'limit':        100,
                'offset':       0,
            }
            app_incidents = []
            try:
                while True:
                    resp = http_get(self._get_request_url + urllib.urlencode(qs), **self._http_base_params())
                    if resp:
                        _incidents = resp.get('incidents', [])
                        app_incidents += _incidents
                        if len(_incidents) + resp['offset'] >= resp['total']:
                            break
                        else:
                            qs['offset'] = len(_incidents) + resp['offset']
                    else:
                        break

            except Exception as e:
                self._warn("failed to poll app status: {0}".format(e)) 

            self._app_processed[key] = app_incidents
            self._debug("total # of alerts found between {0} and {1}: {2}".format(since_time, until_time, len(app_incidents)))

        for _incident in self._app_processed.get(key, []):
            if incident['app'] in _incident['trigger_summary_data']['description']:
                self._debug("found a potential parent app hit: {0}".format(_incident))
                app_reported = _incident
                break

        return app_reported

    def _report_incident(self, incident, only_if_not_acknowledged):
        if not self._reportable(incident):
            self._summary['grace_period_applied'] += 1
            return

        self._debug("reporting incident: {0}".format(incident))

        if only_if_not_acknowledged:
            ## grab incident status and not spam incidents already acknowleged
            try:
                ## whether this incident has been reported or not
                qs = { 'fields': 'incident_number,status', 'incident_key': incident['incident_key'], 'status': 'acknowledged' }
                resp = http_get(self._get_request_url + urllib.urlencode(qs), **self._http_base_params())
                if resp and resp['incidents']:
                    self._info("incident: {1} ALREADY ACKnowledged: {0}!!!".format(resp['incidents'][0], incident['incident_key']))

                    incident.update(resp['incidents'][0])
                    self._summary['incidents_not_triggered_ins_processed'] += 1
                    return

            except Exception as e:
                self._warn("failed to grab incident status: {0}".format(e)) 

        app_incident =  self._incident_app_reported(incident)
        if app_incident:
            incident['time_created'] = incident['time_occurred']
            self._info("NOT reporting incident since app '{0}' already done so: {1}".format(incident['app'], app_incident))
            self._summary['incidents_not_triggered_app_processed'] += 1
            return

        ## fire this event regardless
        def _reporter(count):
            msgs = []
            for alert in incident['details']:
                msgs.append(alert.summary())
        
            data = json.dumps({ 'service_key': self._service_api_key, 'incident_key': incident['incident_key'], 
                        'event_type':'trigger', 'description':incident['description'], 'details': msgs })
            resp = http_post('https://events.pagerduty.com/generic/2010-04-15/create_event.json', body=data, **self._http_base_params())
            self._info("reported incident: {0} on try #{2} resp: {1}".format(incident['incident_key'], resp, count+1))
            self._summary['incidents_triggered'] += 1

            incident['reported'] = True

        ok, exceptions = do_request(_reporter, self._retry_total, self._retry_interval)
        for i, e in enumerate(exceptions):
            self._warn("try #{1}: failed to report incident: {0}".format(e, i+1)) 

        return ok

    def _load_outstanding_incidents(self):

        incidents = {}
        if not self._outstanding_incidents_prefix:
            self._debug("no outstanding_incidents_prefix configured.")
            return incidents
        
        history = self._get_incidents_file()
        self._debug("reading incident file: {0}".format(history))

        if os.path.exists(history):
            _ctxt = {}
            execfile(history, globals(), _ctxt)
                
            if isinstance(_ctxt.get('incidents', []), list):
                incidents = dict([ (incident['incident_key'], incident) for incident in _ctxt.get('incidents', []) ])

        return incidents

    def _persist_outstanding_incidents(self, incidents):

        if not self._outstanding_incidents_prefix:
            self._debug("no outstanding_incidents_prefix configured.")
            return 

        history = self._get_incidents_file()
        with open(history, 'w') as output:
            output.write("incidents = " + pprint.PrettyPrinter(indent=4, width=1).pformat(incidents))

    def _get_incidents_file(self):
        return '{0}.{1}'.format(self._outstanding_incidents_prefix, self._level)

    def _http_base_params(self):
        return {'format':               'json', 
                'headers':              self._base_headers,
                'logger':               self._logger,
                'trace':                self._trace,
                'response_timeout':     self._response_timeout
        }

