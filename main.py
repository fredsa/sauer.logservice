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
  ''                            : '(All logs)',
  logservice.LOG_LEVEL_DEBUG    : 'DEBUG',
  logservice.LOG_LEVEL_INFO     : 'INFO',
  logservice.LOG_LEVEL_WARNING  : 'WARNING',
  logservice.LOG_LEVEL_ERROR    : 'ERROR',
  logservice.LOG_LEVEL_CRITICAL : 'CRITICAL',
}

LATENCY_PRECISION_MS = 100
PENDING_PRECISION_MS = 10
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
          max_requests = 10

        raw_logs = self.request.get('raw_logs')

        #logging.debug("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%DEBUG")
        #logging.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%INFO")
	#logging.warning("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%WARNING")
        #logging.error("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%ERROR")
        #logging.critical("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%CRITICAL")


        # --------------- HTML Page ---------------
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
                H1 {
                  font-size: 1.4em;
                }
              </style>
            </head>
            <body>
          """ % (app_identity.get_application_id(), version) )

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
          self.response.out.write("""<option value='%s' %s>%s</option>""" % (k, selected, v) )

        if raw_logs:
          checked = 'checked'
        else:
          checked = ''
        self.response.out.write("""
              </select> or above<br>
 
              <input type='checkbox' name='raw_logs' %s> Pretty print raw logs<br>

              <input type='submit' value='Submit'>

            </form>
          </fieldset>
          <br>""" % checked)


        # --------------- Raw logs ---------------
        if raw_logs:
          self.response.out.write("""<h1>Raw logs</h1>""")

        latency_cached={}
        latency_static={}
        latency_dynamic={}
        pending={}
        found={}
        count = 0
        for log in logservice.fetch(end_time_usec=None,
                                    min_log_level=level,
                                    include_app_logs=True,
                                    #include_incomplete=True,
                                    version_ids=[version]):
          #self.response.out.write('%s<br>' % log)

          # --------------- Raw logs ---------------
          if raw_logs:
            data = pprint.pformat(vars(log))
            data = cgi.escape(data)
            self.response.out.write('<hr><pre>%s</pre><br>' % data)

          for line in log.line_list():
            message = "[%s] %s]" % ( LEVEL[line.level()], line.log_message() )
            #self.response.out.write('[%s][%s] %s<br>' % (time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(line.time()/1000000)), line.level(), cgi.escape(str( message ))) )
            found[message] = found.get(message, 0) + 1
            # --------------- Raw logs ---------------
            if raw_logs:
              data = pprint.pformat(vars(line))
              data = cgi.escape(data)
              self.response.out.write('<pre>%s</pre><br>' % data)



          index = int( (log.latency() - log.pending_time()) / 1000 / LATENCY_PRECISION_MS)
          if log.response_size() == 0:
            latency_static[index] = latency_static.get(index, 0) + 1
          elif log.status() == 204:
            latency_cached[index] = latency_cached.get(index, 0) + 1
          else:
            latency_dynamic[index] = latency_dynamic.get(index, 0) + 1
            idx = int( log.pending_time() / 1000 / PENDING_PRECISION_MS)
            pending[idx] = pending.get(idx, 0) + 1


          count += 1
	  if count == max_requests:
            break

        if raw_logs:
          self.response.out.write('<hr>')

        if count == 0:
          self.response.out.write("""
            No logs.
            """)
          return


        # --------------- Latency ---------------
        def show_latency(name, latency):
          self.response.out.write("""<h1>Latency Histogram - %s</h1>""" % name)
          if len(latency) == 0:
            self.response.out.write('No logs')
            return

          scale = min(1, float(MAX_LATENCY_WIDTH) / max(latency.values()))
          self.response.out.write("""<pre>""")
          for k in range(0, max(latency) + 1 ):
            cnt = latency.get(k, 0)
            self.response.out.write('%10d requests [%5d - %5d ms]: %s<br>' % (cnt, k * LATENCY_PRECISION_MS, (k+1) * LATENCY_PRECISION_MS -1, '*' * int(scale * cnt)) )
          self.response.out.write('</pre>')

        show_latency('Static Requests', latency_static)
        show_latency('Cached Requests', latency_cached)
	show_latency('Dynamic Requests', latency_dynamic)
        show_latency('Pending Time (Dynamic Requests Only)', pending)

        # --------------- Errors ---------------
        self.response.out.write("""<h1>Log message frequency</h1>""")
        if len(found) == 0:
          self.response.out.write("""
            No messages.
            """)
        else:
          for message in sorted(found, key=found.get, reverse=True):
            self.response.out.write("""
              Count: <b>%d</b><br>
              <pre class='errmsg'>%s</pre><br>
              """ % (found[message], cgi.escape(message)) )

        # --------------- End of page ---------------
        self.response.out.write("""
            </body>
          </html>
          """)

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
