#!/usr/bin/env python

import logging
import urllib
import cgi
import pprint
import time
import calendar
import os

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import util
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.api import app_identity
from google.appengine.api import logservice

from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
#from mapreduce import operation as op
#from mapreduce import shuffler

LEVEL = {
  ''                            : '(All logs)',
  logservice.LOG_LEVEL_DEBUG    : 'DEBUG',
  logservice.LOG_LEVEL_INFO     : 'INFO',
  logservice.LOG_LEVEL_WARNING  : 'WARNING',
  logservice.LOG_LEVEL_ERROR    : 'ERROR',
  logservice.LOG_LEVEL_CRITICAL : 'CRITICAL',
}

MAX_LATENCY_WIDTH = 100
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

class Result(db.Model):
  start_minute = db.IntegerProperty()
  requests = db.ListProperty(int)
  blob_key = db.StringProperty()
  url = db.StringProperty()

class MyPipeline(base_handler.PipelineBase):

  def run(self, start_time_usec, end_time_usec):
    logging.info('*********************************** MyPipeline.run(self, %d, %d)' % (start_time_usec, end_time_usec) )
    output = yield mapreduce_pipeline.MapreducePipeline(
        "My MapReduce",
        "main.my_map",
        "main.my_reduce",
        "mapreduce.input_readers.LogInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
        mapper_params={
            "start_time": start_time_usec,
            "end_time": end_time_usec,
        },
        reducer_params={
            "mime_type": "text/plain",
        },
        shards=1)
    yield StoreOutput("MyPiplineOutput", start_time_usec, end_time_usec, output)

def my_map(log):
  """My map function."""

  logging.info('--------------------------------------- Map Something ----------------------------------')
  data = pprint.pformat(vars(log))
  data = cgi.escape(data)
  start_time_s = int(log.start_time() / 1e6)
  yield(start_time_s, 1)

def my_reduce(key, values):
  """Word Count reduce function."""

  logging.info('--------------------------------------- REDUCE: key=%s, values=%s ----------------------------------' % (key, values) )
  yield "%s,%d\n" % (key, len(values))

class StoreOutput(base_handler.PipelineBase):
  """A pipeline to store the result of the MapReduce job in the datastore.
  """

  def run(self, mr_kind, start_time_usec, end_time_usec, blob_keys):
    hostname = app_identity.get_default_version_hostname()
    if not hostname:
      hostname = "localhost:8080"
    for blob_key in blob_keys: 
      url = "http://%s%s" % (hostname, blob_key)
      logging.info("********************************************** StoreOutput.run(self, mr_kind=%s, start_time_usec=%d, end_time_usec=%d, blob_keys=%s) url = %s" % (mr_kind, start_time_usec, end_time_usec, blob_key, url) )
      db.put(Result(blob_key=blob_key, url=url))




class DownloadHandler(blobstore_handlers.BlobstoreDownloadHandler):
  """Handler to download blob by blobkey."""

  def get(self, key):
    self.response.headers.add_header("Access-Control-Allow-Origin", "*")
    key = str(urllib.unquote(key)).strip()
    logging.debug("key is %s" % key)
    blob_info = blobstore.BlobInfo.get(key)
    self.send_blob(blob_info)




class MainHandler(webapp.RequestHandler):

    def show_latency(self, precision_ms, latency, resource, name, comment):
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


    def do_visualize(self, blob_url, smooth_seconds):

        # --------------- MapReduce results ---------------
        self.response.out.write("""<h1>MapReduce results</h1>""")

        self.response.out.write("""
          <div id='status' class='status'>Initializing visualization...</div>
          <div id="visualization" style="width: 500px; height: 400px; border: 1px solid gray;">visualization</div>
        """)

        self.response.out.write(r"""
              <script type="text/javascript" src="https://www.google.com/jsapi"></script>
              <script type="text/javascript">
                google.load('visualization', '1', {packages: ['corechart']});
              </script>
              <script type="text/javascript">

                var smooth_seconds = %d;
                var blob_url = "%s";

                function setStatus(status) {
                  document.getElementById("status").innerHTML = status;
                }

                function showGraph(url) {
                  setStatus("Fetching content from " + url);

                  var xhr = new XMLHttpRequest();
                  xhr.onreadystatechange=function() {
                    setStatus("xhr.readyState = " + xhr.readyState);
                    if (xhr.readyState == 4) {
                      setStatus("xhr.status = " + xhr.status);
                      if (xhr.status==200) {
                        showData(xhr.responseText);
                      }
                    }
                  }

                  setStatus("xhr.open()...");
                  xhr.open("GET", url, true);

                  setStatus("xhr.send()...");
                  xhr.send();
                }

                function showData(response) {
                  var data = new google.visualization.DataTable();
                  data.addColumn('datetime', 'request start time');
                  data.addColumn('number', 'qps');
                  
                  var lines = response.split('\n');
                  setStatus("Parsing " + (lines.length) + " lines of CSV data...");
                  
                  // testing
                  s = lines[0].split(",", 2); lines[0] = s[0] - 600 + "," + s[1]

                  map = []
                  for (i in lines) {
                    line = lines[i];
                    if (!line) continue;
                    s = line.split(",", 2);
                    setStatus("Scanning row " + i + " with data " + line + "   s[1] = " + s[1]);
                    map[String(s[0])] = s[1];

                    ts = parseInt(s[0])
                    if (i == 0) {
                      min = ts;
                      max = ts;
                    } else {
                      min = Math.min(min, ts);
                      max = Math.max(max, ts);
                    }
                  }

                  for (ts = min; ts <= max; ts += smooth_seconds) {
                    totals=[0]
                    for (ts2 = ts; ts2 < ts + smooth_seconds; ts2++) {
                      values = map[String(ts2)]
                      setStatus("Parsing values timestamp " + ts2 + " for datapoint at " + ts + " : " + values);
                      if (!values) continue;
                      values = values.split(",");
                      for (j in values) {
                         totals[j] += parseInt(values[j])
                      }
                    }

                    data.addRow([
                      new Date(ts * 1e3),
                      parseInt(totals[0] / smooth_seconds),
                    ]);
                  }
                  
                  setStatus("Visualizing results...");
                  new google.visualization.AreaChart(document.getElementById('visualization')).
                      draw(data, {legend: "none",
                                  curveType: "none",
                                  interpolateNulls: false,
                                  hAxis: {title: 'Date Time',  titleTextStyle: {color: '#888'}},
                                  width: 500, height: 400,
                                  vAxis: {title: 'qps', titleTextStyle: {color: '#888'}},
                                 }
                          );
                  setStatus("Showing results smoothed over " + smooth_seconds + " seconds for <a href='" + blob_url + "'>" + blob_url + "</a>");
                }
            
                google.setOnLoadCallback(function() { showGraph(blob_url); });

                setStatus("script block executed");
              </script>
        """ % (smooth_seconds, blob_url) )


    def do_mapreduce(self, start_time_usec, end_time_usec):
        
        now_s = time.time()
        start_time_usec = (now_s - 1 * 60 * 60) * 1e6
        end_time_usec = now_s * 1e6

        pipeline = MyPipeline(start_time_usec, end_time_usec)
        logging.info('************************************************************************************************************************************************')
        logging.info('*************************************************************** pipeline.start() ***************************************************************')
        logging.info('************************************************************************************************************************************************')
        pipeline.start()


    def do_grep(self, version, max_requests, level, start_time_usec, end_time_usec, precision_ms, raw_logs):

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
        logging.info("fetch(start_time_usec=%s, end_time_usec=%s, ...)" % (start_time_usec, end_time_usec) )
        for log in logservice.fetch(start_time_usec=start_time_usec,
                                    end_time_usec=end_time_usec,
                                    min_log_level=level,
                                    include_app_logs=True,
                                    #include_incomplete=True,
                                    version_ids=[version]
                                   ):
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
        self.show_latency(precision_ms, latency_dynamic, resource_dynamic, 'Dynamic Requests', 'log.response_size() > 0 and log.status() != 204 and log.status() <= 399')
        self.show_latency(precision_ms, latency_errors,  resource_errors,  'Errors',           'log.status() >= 400')
        self.show_latency(precision_ms, latency_static,  resource_static,  'Static Requests',  'log.response_size() == 0 and log.status() <= 399')
        self.show_latency(precision_ms, latency_cached,  resource_cached,  'Cached Requests',  'log.status() == 204')
        self.show_latency(precision_ms, latency_pending, resource_pending, 'Pending Time',     'log.pending_time() > 0')

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

        # raw_logs
        raw_logs = self.request.get('raw_logs')

        # start_time_usec
        try:
          s = self.request.get('start_time_str')
          t = time.strptime(s, TIME_FORMAT)
          start_time_usec = long(calendar.timegm(t)) * 1e6
        except ValueError:
          start_time_usec = 0

        # end_time_usec
        try:
          s = self.request.get('end_time_str')
          t = time.strptime(s, TIME_FORMAT)
          end_time_usec = long(calendar.timegm(t)) * 1e6
        except ValueError:
          end_time_usec = time.time() * 1e6

        # desired_action
        desired_action=self.request.get("desired_action")

        # blob_url
        blob_url = self.request.get('blob_url')

        # smooth_seconds
        try:
          smooth_seconds = long(self.request.get('smooth_seconds'))
        except ValueError:
          smooth_seconds = 60


        #logging.debug("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%DEBUG")
        #logging.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%INFO")
        #logging.warning("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%WARNING")
        #logging.error("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%ERROR")
        #logging.critical("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%CRITICAL")


        # --------------- HTML Page ---------------
        self.response.out.write("""
          <html>
            <head>
              <title>Logservice %s</title>
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
                FIELDSET {
                  background-color: #def;
                 margin: 0.5em 0em;
                }
                .status {
                  white-space: pre;
                }
              </style>
            </head>
            <body>
          """ % app_identity.get_application_id() )

        # --------------- TITLE ---------------
        self.response.out.write("""
            <div><a href="/">&lt;&lt; logservice</a></div>
          """)
        # --------------- 1st FORM ---------------
        self.response.out.write("""
          <fieldset>
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
          """)
 
        start_time_str = time.strftime(TIME_FORMAT, time.gmtime(start_time_usec / 1e6) )
        end_time_str = time.strftime(TIME_FORMAT, time.gmtime(end_time_usec / 1e6) )
        self.response.out.write("""
             Only include requests between <input name='start_time_str' value='%s' size='20'>
             and <input name='end_time_str' value='%s' size='20'><br>
          """ % (start_time_str, end_time_str))
 
        self.response.out.write("""
              Histogram precision: <input name='precision_ms' value='%s' size='5'>ms<br>
          """ % precision_ms)

        self.response.out.write("""
              <input type='checkbox' name='raw_logs' %s> Pretty print raw logs<br>

              <input type='hidden' name='desired_action' value='grep'>
              <input type='submit' value='Summarize'>

            </form>
          </fieldset>
          """ % checked)

        # --------------- 2nd FORM ---------------
        self.response.out.write("""
          <fieldset>
            <legend>Logs MapReduce</legend>
            <form action='/'>
        """)

        self.response.out.write("""
             Only include requests between <input name='start_time_str' value='%s' size='20'>
             and <input name='end_time_str' value='%s' size='20'><br>
          """ % (start_time_str, end_time_str))
 
        self.response.out.write("""
              <input type='hidden' name='desired_action' value='mapreduce'>
              <input type='submit' value='Kick Off MapReduce'>
          """)

        self.response.out.write("""
            </form>
          </fieldset>
          """)

        results = db.Query(Result).fetch(limit=10)
        if results:
          self.response.out.write("""
            <fieldset>
              <legend>Visualize MapReduce results</legend>
              <form action='/'>

                Smooth results over <input name='smooth_seconds' value='%s' size='10' onChange='this.parentNode.submit()'> seconds<br>
            """ % smooth_seconds)

          self.response.out.write("""
                <select name='blob_url' onChange='this.parentNode.submit()'>
                <option value=''>(select blob to view)</option>
            """)

          for result in results:
            url = result.url
            t = pprint.pformat(db.to_dict(result))
            if url == blob_url:
              selected = "selected"
            else:
              selected = ""
            self.response.out.write("""<option value='%s' %s>%s</option>""" % (url, selected, t) )

          self.response.out.write("""
                </select><br>

                <input type='hidden' name='desired_action' value='visualize'>
              </form>
            </fieldset>
            """)

        # --------------- Conditional content ---------------
        if desired_action == "mapreduce":
          self.do_mapreduce(start_time_usec, end_time_usec)
        elif desired_action == "grep":
          self.do_grep(version, max_requests, level, start_time_usec, end_time_usec, precision_ms, raw_logs)
        elif desired_action == "visualize":
          self.do_visualize(blob_url, smooth_seconds)

        # --------------- End of page ---------------
        self.response.out.write("""
            </body>
          </html>
          """)



APP = webapp.WSGIApplication(
    [
        ('/', MainHandler),
        (r'/blobstore/(.*)', DownloadHandler),
    ],
    debug=True)

def main():
  logging.getLogger().setLevel(logging.DEBUG)
  util.run_wsgi_app(APP)


if __name__ == '__main__':
    main()
