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

precision_ms = 100
precision_ms = 10
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

        # precision_ms
        try:
          precision_ms = int(self.request.get('precision_ms'))
        except ValueError:
          precision_ms = 100

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
                  margin: 1.9em 0em 0.2em 0em;
                }
                .comment {
                  font-size: 0.7em;
                  font-style: italic;
                  color: #888;
                  margin: 0em 0em .5em 0em;
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

              Histogram precision: <input name='precision_ms' value='%s' size='5'>ms<br>
          """ % precision_ms)
 
        self.response.out.write("""
              <input type='checkbox' name='raw_logs' %s> Pretty print raw logs<br>

              <input type='submit' value='Submit'>

            </form>
          </fieldset>
          <br>""" % checked)


        # --------------- Raw logs ---------------
        if raw_logs:
          self.response.out.write("""<h1>Raw logs</h1>""")

        latency_errors={}
        latency_cached={}
        latency_static={}
        latency_dynamic={}
        latency_pending={}

        resource_errors={}
        resource_cached={}
        resource_static={}
        resource_dynamic={}
        resource_pending={}

        messages={}
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
            msg = "[%s] %s]" % ( LEVEL[line.level()], line.log_message() )
            #self.response.out.write('[%s][%s] %s<br>' % (time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(line.time()/1000000)), line.level(), cgi.escape(str( msg ))) )
            messages[msg] = messages.get(msg, 0) + 1
            # --------------- Raw logs ---------------
            if raw_logs:
              data = pprint.pformat(vars(line))
              data = cgi.escape(data)
              self.response.out.write('<pre>%s</pre><br>' % data)



          res = """[%s] %s""" % (log.status(), log.resource())
          index = int( (log.latency() - log.pending_time()) / 1000 / precision_ms) 
          if log.status() >= 400:
            latency_errors[index] = latency_errors.get(index, 0) + 1
            resource_errors[res] = resource_errors.get(res, 0) + 1
          elif log.response_size() == 0:
            latency_static[index] = latency_static.get(index, 0) + 1
            resource_static[res] = resource_static.get(res, 0) + 1
          elif log.status() == 204:
            latency_cached[index] = latency_cached.get(index, 0) + 1
            resource_cached[res] = resource_cached.get(res, 0) + 1
          else:
            latency_dynamic[index] = latency_dynamic.get(index, 0) + 1
            resource_dynamic[res] = resource_dynamic.get(res, 0) + 1

          if log.pending_time() > 0:
            idx = int( log.pending_time() / 1000 / precision_ms)
            latency_pending[idx] = latency_pending.get(idx, 0) + 1
            resource_pending[res] = resource_pending.get(res, 0) + 1


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
        def show_latency(latency, resource, name, comment):
          self.response.out.write("""<h1>Latency - %s</h1>""" % name)
          self.response.out.write("""<div class='comment'>%s</div>""" % comment)
          if len(latency) == 0:
            self.response.out.write('No logs')
            return

          scale = min(1, float(MAX_LATENCY_WIDTH) / max(latency.values()))
          self.response.out.write("""<pre>""")
          for k in range(0, max(latency) + 1 ):
            cnt = latency.get(k, 0)
            self.response.out.write('%6d requests: [%5d - %5d ms]: %s<br>' % (cnt, k * precision_ms, (k+1) * precision_ms -1, '*' * int(scale * cnt)) )
          self.response.out.write('</pre>')

          self.response.out.write("""<pre>""")
          for res in sorted(resource, key=resource.get, reverse=True):
            self.response.out.write("""%6d requests: %s<br>""" % (resource[res], res) )
          self.response.out.write("""</pre>""")


        show_latency(latency_dynamic, resource_dynamic, 'Dynamic Requests', 'log.response_size() > 0 and log.status() != 204 and log.status() <= 399')
        show_latency(latency_errors,  resource_errors,  'Errors',           'log.status() >= 400')
        show_latency(latency_static,  resource_static,  'Static Requests',  'log.response_size() == 0 and log.status() <= 399')
        show_latency(latency_cached,  resource_cached,  'Cached Requests',  'log.status() == 204')
        show_latency(latency_pending, resource_pending, 'Pending Time',     'log.pending_time() > 0')

        # --------------- Errors ---------------
        self.response.out.write("""<h1>Log message frequency</h1>""")
        if len(messages) == 0:
          self.response.out.write("""
            No messages.
            """)
        else:
          for msg in sorted(messages, key=messages.get, reverse=True):
            self.response.out.write("""
              Count: <b>%d</b><br>
              <pre class='errmsg'>%s</pre><br>
              """ % (messages[msg], cgi.escape(msg)) )

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
