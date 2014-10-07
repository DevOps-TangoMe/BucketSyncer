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

import httplib2
import json
import time
import xml.etree.ElementTree as ET

def http_delete(url, logger=None, **kwargs):
    ''' simple curl wrapper for http DELETE '''
    return _http_request(url, method='DELETE', logger=logger, **kwargs)

def http_post(url, logger=None, body=None, **kwargs):
    ''' simple curl wrapper for http POST '''
    return _http_request(url, method='POST', logger=logger, body=body, **kwargs)

def http_get(url, logger=None, **kwargs):
    ''' simple curl wrapper for http GET '''
    return _http_request(url, logger=logger, **kwargs)

def _http_request(url, method='GET', logger=None, body='', **kwargs):
    ''' simple curl wrapper for http GET '''
    headers = kwargs.get('headers', [])
    fmt = kwargs.get('format', 'json')
    response_to = kwargs.get('response_timeout', 0)
    trace = kwargs.get('trace', False)

    timeout = response_to if response_to else None
    try:
        msg = None
        if logger:
            bt = time.time()
            if trace:
                logger.debug(url)

        http = httplib2.Http(disable_ssl_certificate_validation=True, timeout=timeout)
        resp, body = http.request(url, method, body, dict(headers)) 
        if int(resp.get('status', 0)) == 200:
            if fmt == 'json':
                result = json.loads(body)
            elif fmt == 'xml':
                result = ET.fromstring(body)
            else:
                result = body

            return result
        else:
            msg = "server responded with {0} status code: {1}".format(resp['status'], body)

    except Exception as e:
        msg = "exception caught: {0}".format(e)

    finally:
        if logger and msg:
            logger.debug("{0} secs later ... {1}".format( ( time.time() - bt ), msg))

        if msg:
            raise RuntimeError("http_get exception: {0}".format(msg))

def do_request(func, retry_total, retry_interval):

    exceptions = []
    ok = False
    while True:
        try:
            func(len(exceptions))
            ok = True
            break
        except Exception as e:
            exceptions.append(e)
            if len(exceptions) >= retry_total:
                break
            else:
                time.sleep(retry_interval)

    return ok, exceptions 

if __name__ == '__main__':

    import unittest

    class DoRequestTests(unittest.TestCase):

        def setUp(self):
            self.count = 0

        def test_booyah(self):

            def _booyah(c):
                self.count += c

            ok, exceptions = do_request(_booyah, 5, 0.1)
            self.assertTrue(ok)
            self.assertEquals(self.count, 0)
            self.assertEquals(0, len(exceptions))
    
        def test_boohoo(self):
        
            def _boohoo(c):
                self.count += c
                raise RuntimeError("round {0}".format(c))

            ok, exceptions = do_request(_boohoo, 5, 0.1)
            self.assertFalse(ok)
            self.assertEquals(self.count, 10)
            self.assertEquals(5, len(exceptions))
            for i, exception in enumerate(exceptions):
                self.assertEquals(str(exception), 'round {0}'.format(i))

    suite = unittest.TestLoader().loadTestsFromTestCase(DoRequestTests)
    unittest.TextTestRunner(verbosity=2).run(suite)
