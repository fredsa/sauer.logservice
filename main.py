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

        # max
        try: 
          max = int(self.request.get('max'))
        except ValueError:
          max = 100

        logging.debug("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%DEBUG")
        logging.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%INFO")
	logging.warning("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%WARNING")
        logging.error("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%ERROR")
        logging.critical("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%CRITICAL")

        found={}
        count = max
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

          count -= 1
	  if count == 0:
            break

        self.response.out.write("""
          <fieldset style='background-color: #def'>
            <legend>Logs Filter</legend>
            <form action='/'>
              Application version: <input name='version' value='%s' size='20'><br>
              Process logs for the <input name='max' value='%d' size='5'> most recent requests<br>

              Only include requests which contain at least one message at level
              <select name='level'>
        """ % (version, max))

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
        for message in sorted(found, key=found.get, reverse=True):
          self.response.out.write('<hr>Message count: <b>%d</b><br>Message: <b><pre>%s</pre></b><br>' % (found[message], message) )


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
