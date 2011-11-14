#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import urllib
import cgi
import pprint
import time
import os

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import util
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.api import app_identity
from google.appengine.api import logservice

LEVEL = {
  logservice.LOG_LEVEL_DEBUG    : 'DEBUG',
  logservice.LOG_LEVEL_INFO     : 'INFO',
  logservice.LOG_LEVEL_WARNING  : 'WARNING',
  logservice.LOG_LEVEL_ERROR    : 'ERROR',
  logservice.LOG_LEVEL_CRITICAL : 'CRITICAL',
}

PRECISION_MS = 100
MAX_LATENCY_WIDTH = 100

class MainHandler(webapp.RequestHandler):
    def get(self):

        # version
        version = self.request.get('version')
        if not version:
          version = os.environ['CURRENT_VERSION_ID'].split('.')[0]

        # level
        try:
          level = int(self.request.get('level'))
        except ValueError:
          level = None

        # max_requests
        try: 
          max_requests = int(self.request.get('max_requests'))
        except ValueError:
          max_requests = 100

        #logging.debug("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%DEBUG")
        #logging.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%INFO")
	#logging.warning("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%WARNING")
        #logging.error("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%ERROR")
        #logging.critical("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%CRITICAL")

        bucket={}
        found={}
        count = max_requests
        for log in logservice.fetch(end_time_usec=None,
                                    min_log_level=level,
                                    include_app_logs=True,
                                    #include_incomplete=True,
                                    version_ids=[version]):
          #self.response.out.write('%s<br>' % log)
          for line in log.line_list():
            message = "[%s] %s]" % ( LEVEL[line.level()], line.log_message() )
            #self.response.out.write('[%s][%s] %s<br>' % (time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(line.time()/1000000)), line.level(), cgi.escape(str( message ))) )
            found[message] = found.get(message, 0) + 1
          data = pprint.pformat(vars(log))
          data = cgi.escape(data)
          #self.response.out.write('<br><pre>%s</pre><br>' % data)
          latency = int(log.latency() / 1000 / PRECISION_MS)
          bucket[latency] = bucket.get(latency, 0) + 1

          count -= 1
	  if count == 0:
            break

        self.response.out.write("""
          <html>
            <head>
              <title>Logservice %s %s</title>
              <style>
                BODY {
                  font-family: arial;
                }
                LEGEND {
                  font-weight: bold;
                }
                pre.errmsg {
                  background-color: #d9d9d9;
                  padding: 0.4em 0.1em;
                  margin: 0em;
                }
              </style>
            </head>
            <body>
          """ % (app_identity.get_application_id(), version) )
        self.response.out.write("""
            </body>
          </html>
          """)
        # --------------- FORM ---------------
        self.response.out.write("""
          <fieldset style='background-color: #def'>
            <legend>Logs Filter</legend>
            <form action='/'>
              Application version: <input name='version' value='%s' size='20'><br>
              Process logs for the <input name='max_requests' value='%d' size='5'> most recent requests<br>

              Only include requests which contain at least one message at level
              <select name='level'>
        """ % (version, max_requests))

        for k, v in LEVEL.iteritems():
          if level == k:
            selected = 'selected'
          else:
            selected = ''
          self.response.out.write("""<option value='%d' %s>%s</option>""" % (k, selected, v) )

        self.response.out.write("""
              </select> or above<br>
 
              <input type='submit' value='Submit'>

            </form>
          </fieldset>
          <br>""" % ())



        # --------------- Latency ---------------
        #bucket[1] = 10
        #bucket[4] = 20
        #bucket[4] = 30
        scale = min(1, float(MAX_LATENCY_WIDTH) / max(bucket.values()))
        self.response.out.write("""<h1>Latency Histogram</h1>
                                <pre>""")
        for k in range(0, max(bucket) + 1 ):
          cnt = bucket.get(k, 0)
          self.response.out.write('%10d requests %5d - %5d ms: %s<br>' % (cnt, k * PRECISION_MS, (k+1) * PRECISION_MS -1, '*' * int(scale * cnt)) )
        self.response.out.write('</pre>')

        # --------------- Errors ---------------
        self.response.out.write("""<h1>Log message frequency</h1>""")
        for message in sorted(found, key=found.get, reverse=True):
          self.response.out.write("""
            Count: <b>%d</b><br>
            <pre class='errmsg'>%s</pre><br>
            """ % (found[message], cgi.escape(message)) )


APP = webapp.WSGIApplication(
    [
        ('/', MainHandler),
    ],
    debug=True)

def main():
  logging.getLogger().setLevel(logging.DEBUG)
  util.run_wsgi_app(APP)


if __name__ == '__main__':
    main()
