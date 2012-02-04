#!/usr/bin/env python

import logging
import urllib
import cgi
import pprint
import time
import calendar
import os
import math

from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.ext import blobstore
from google.appengine.api import users
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
TIME_FORMAT = '%Y-%m-%d %H:%M:%S %Z'

def human_time(time_s):
    return time.strftime(TIME_FORMAT, time.localtime(time_s) )

def record_to_dict(rec):
  return dict((x, getattr(rec, x)) for x in dir(rec) if x[0] != '_')

def pretty_level(level):
  return """<span class='ae-logs-severity ae-logs-severity-%s'>%s</span>""" % (level, LEVEL[level])
  
class LogServiceMapReduceResult(db.Expando):
  start_minute = db.IntegerProperty()
  requests = db.ListProperty(int)
  blob_key = db.StringProperty()

class MyPipeline(base_handler.PipelineBase):

  def run(self, mr_type, shards, start_time, end_time, version):
    logging.info('*********************************** MyPipeline.run(self, %s, %d, %f, %f, %s)' % (mr_type, shards, start_time, end_time, version) )
    output = yield mapreduce_pipeline.MapreducePipeline(
        "My MapReduce",
        "main.my_%s_map" % mr_type,
        "main.my_%s_reduce" % mr_type,
        "mapreduce.input_readers.LogInputReader",
        "mapreduce.output_writers.BlobstoreOutputWriter",
        mapper_params={
            "start_time": start_time,
            "end_time": end_time,
            "version_ids": [version],
        },
        reducer_params={
            "mime_type": "text/plain",
        },
        shards=shards)
    yield StoreOutput(start_time, end_time, version, output)

def my_collect_map(log):
  """My map function."""

  logging.info('--------------------------------------- Map Something ----------------------------------')
  yield("%s\n" % log.combined, '')
  

def my_collect_reduce(key, values):
  """My reduce function."""

  yield key


def my_graph_map(log):
  """My map function."""

  logging.info('--------------------------------------- Map Something ----------------------------------')
  yield(log.start_time, '[1, 0]')
  logging.info('%f: hit' % log.start_time)
  start = int(math.floor(log.start_time))
  end = int(math.ceil(log.end_time))
  for t in range(start, end + 1):
    if t != start:
      logging.info('%d: continued at %d **********************************************************' % (start, t) )
      logging.info('log.start_time = %f (start = %d), log.end_time = %f (end = %d)' % (log.start_time, start, log.end_time, end) )
    yield(t, '[0, 1]')


def my_graph_reduce(key, values):
  """My reduce function."""

  logging.info('--------------------------------------- REDUCE: key=%s, values=%s ----------------------------------' % (key, values) )
  # convert list of list of strings -> list of list of ints
  values = [eval(a) for a in values]

  # sum elements of array
  values = [sum(a) for a in zip(*values)]

  yield "%s,%s\n" % (key, values)


class StoreOutput(base_handler.PipelineBase):
  """A pipeline to store the result of the MapReduce job in the datastore.
  """

  def run(self, start_time, end_time, version, blob_keys):
    for blob_key in blob_keys: 
      logging.info("********************************************** StoreOutput.run(self, blob_key=%s, start_time=%f, end_time=%f, version=%s)" % (blob_key, start_time, end_time, version) )
      result = LogServiceMapReduceResult(blob_key=blob_key, start_time=start_time, end_time=end_time, version=version)
      db.put(result)




class DownloadHandler(blobstore_handlers.BlobstoreDownloadHandler):
  """Handler to download blob by blobkey."""

  def get(self, key):
    self.response.headers.add_header("Access-Control-Allow-Origin", "*")
    key = str(urllib.unquote(key)).strip()
    logging.debug("Retuning blob with key %s" % key)
    blob_info = blobstore.BlobInfo.get(key)
    self.send_blob(blob_info)




class MainHandler(webapp.RequestHandler):


    def out(self, msg):

       if not self.batch:
         self.response.out.write(msg)


    def batch_out(self, msg):

       self.response.out.write(msg)


    def show_latency(self, precision_ms, latency, resource, name, comment):

        self.out("""<h1>Latency - %s</h1>""" % name)
        self.out("""<div class='comment'>%s</div>""" % comment)
        if len(latency) == 0:
          self.out('No logs')
          return

        scale = min(1, float(MAX_LATENCY_WIDTH) / max(latency.values()))
        self.out("""<pre>""")
        for k in range(0, max(latency) + 1 ):
          cnt = latency.get(k, 0)
          self.out('%6d requests: [%5d - %5d ms]: %s<br>' % (cnt, k * precision_ms, (k+1) * precision_ms -1, '*' * int(scale * cnt)) )
        self.out('</pre>')

        self.out("""<pre>""")
        for res in sorted(resource, key=resource.get, reverse=True):
          self.out("""%6d requests: %s<br>""" % (resource[res], res) )
        self.out("""</pre>""")


    def do_visualize(self, blob_key, smooth_seconds):

        # --------------- MapReduce results ---------------
        self.out("""<h1>MapReduce results</h1>""")

        self.out("""
          <div id='status' class='status'>Initializing visualization...</div>
          <div id="visualization" style="stlye: visibility: hidden; width: 500px; height: 400px; border: 1px solid gray;">visualization</div>
          <pre id="blob_data"></pre>
        """)

        self.out(r"""
              <script type="text/javascript" src="https://www.google.com/jsapi"></script>
              <script type="text/javascript">
                google.load('visualization', '1', {packages: ['corechart']});
              </script>
              <script type="text/javascript">

                var smooth_seconds = %d;
                var blob_key = "%s";
                var blob_url = window.location.protocol + "//" + window.location.host + blob_key;

                function setStatus(status) {
                  document.getElementById("status").innerHTML = status;
                  console.log(status);
                }

                function showGraph(url) {
                  if (!url.match(/blobstore/)) {
                    setStatus("Invalid blobstore URL " + url);
                    return;
                  }
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

                  setStatus("xhr.open('GET', '" + url + "', true)...");
                  xhr.open("GET", url, true);

                  setStatus("xhr.send()...");
                  xhr.send();
                }

                function showData(response) {
                  document.getElementById("blob_data").innerHTML = response;

                  var data = new google.visualization.DataTable();
                  data.addColumn('datetime', 'request start time');
                  data.addColumn('number', 'qps');
                  data.addColumn('number', 'conc reqs');
                  cols = data.getNumberOfColumns() - 1;
                  
                  response = response.trim();
                  if (!response) {
                    setStatus("No data. Empty CSV.");
                    document.getElementById('visualization').innerHTML = "NONE";
                    return;
                  }

                  var lines = response.split('\n');
                  setStatus("Parsing " + (lines.length) + " lines of CSV data...");
                  
                  // testing
                  // s = lines[0].split(",", 2); lines[0] = s[0] - 600 + "," + s[1]

                  map = []
                  for (i in lines) {
                    line = lines[i];
                    if (!line) continue;
                    s = /(.*?),(.*)/.exec(line);
                    //setStatus("Scanning row " + i + " with data " + line + "  -->  s[1] = " + s[1] + ", s[2] = " + s[2]);
                    map[String(s[1])] = s[2];

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
                    totals = new Array(cols);
                    for (t = 0; t < cols; t++) {
                      totals[t] = 0;
                    }

                    for (ts2 = ts; ts2 < ts + smooth_seconds; ts2++) {
                      values = map[String(ts2)]
                      //setStatus("Parsing values timestamp " + ts2 + " for datapoint at " + ts + ": " + values);
                      if (values) {
                        values = values.replace(/[\[\]]/g, "");
                        values = values.split(",");
                        for (j = 0; j < cols; j++) {
                           //console.log("typeof values[" + j + "] = " + values[j] + " which is a " + typeof values[j]);
                           totals[j] += parseInt(values[j])
                        }
                      }
                    }

                    data.addRow([
                      new Date(ts),
                      parseInt(totals[0]) / smooth_seconds,
                      parseInt(totals[1]) / smooth_seconds,
                    ]);
                  }
                  
                  setStatus("Visualizing results...");
                  document.getElementById('visualization').style.visibility = "";

                  if (smooth_seconds >= (max - min) ) {
                    chart =  new google.visualization.ColumnChart(document.getElementById('visualization'));
                  } else {
                    chart =  new google.visualization.AreaChart(document.getElementById('visualization'));
                  }
                  //new google.visualization.AreaChart(document.getElementById('visualization')).
                  chart.draw(data, {legend: "top",
                                    interpolateNulls: false,
                                    hAxis: {title: 'Date Time',  titleTextStyle: {color: '#888'}},
                                    width: 500, height: 400,
                                    vAxis: {title: 'qps', titleTextStyle: {color: '#888'}},
                             });
                  setStatus("Showing results smoothed over " + smooth_seconds + " seconds for <a href='" + blob_url + "'>" + blob_url + "</a>");
                }
            
                google.setOnLoadCallback(function() { showGraph(blob_url); });

                setStatus("script block executed");
              </script>
        """ % (smooth_seconds, blob_key) )


    def do_mapreduce(self, mr_type, shards, start_time, end_time, version):
        
        self.out("""<h1>MapReduce launched</h1>""")
        self.out("""
            <div class='status'>
            mr_type = %s<br>
            shards = %d<br>
            start_time = %s (%f)<br>
            end_time = %s (%f)<br>
            version = %s<br>
            </div>
          """ % (mr_type, shards, human_time(start_time), start_time, human_time(end_time), end_time, version) )
        self.out("""<a href='/?version=%s'>&lt;&lt;%s</a>""" % (version, version) )

        pipeline = MyPipeline(mr_type, shards, start_time, end_time, version)
        logging.info('************************************************************************************************************************************************')
        logging.info('*************************************************************** pipeline.start() ***************************************************************')
        logging.info('************************************************************************************************************************************************')
        pipeline.start()


    def do_grep(self, version, max_requests, level, start_time, end_time, precision_ms, raw_logs):

        latency_errors = {}
        latency_cached = {}
        latency_static = {}
        latency_dynamic = {}
        latency_pending = {}

        resource_errors = {}
        resource_cached = {}
        resource_static = {}
        resource_dynamic = {}
        resource_pending = {}

        messages = {}
        include_app_logs = raw_logs == 'pretty' or raw_logs == 'download'
        version_ids = [version]
        include_incomplete = False

        self.out("""<h1>logservice.fetch() parameters</h1>""")
        self.out("""<pre>""")
        self.out("""start_time: %f (%s)\n""" % (start_time, human_time(start_time)) )
        self.out("""end_time: %f (%s)\n""" % (end_time, human_time(end_time)) )
        level_str = LEVEL[level] if level is not None else 'None'
        self.out("""minimum_log_level: %s (%s)\n""" % (pprint.pformat(level), level_str) )
        self.out("""include_app_logs: %s\n""" % include_app_logs )
        self.out("""include_incomplete: %s\n""" % include_incomplete )
        self.out("""version_ids: %s\n""" % version_ids )
        self.out("""</pre>""")
        logging.info("fetch(start_time=%f, end_time=%f, minimum_log_level=%s, include_app_logs=%s, include_incomplete=%s, version_ids=%s)" % (start_time, end_time, level, include_app_logs, include_incomplete, version_ids) )
        logs = logservice.fetch(start_time=start_time,
                                end_time=end_time,
                                minimum_log_level=level,
                                include_app_logs=include_app_logs,
                                include_incomplete=include_incomplete,
                                version_ids=version_ids,
			)


        if raw_logs == 'pretty':
          self.out("""<h1>Raw logs</h1>""")
        if raw_logs == 'download':
          self.response.headers['Content-Type'] = 'text/plain'
 
        count = 0
        for log in logs:
          #self.out('%s<br>' % log)

          if raw_logs == 'pretty':
            data = record_to_dict(log)
            del data['app_logs']
            data = pprint.pformat(data)
            data = cgi.escape(data)
            self.out('<hr><pre>%s</pre><br>\n' % data)
          if raw_logs == 'download':
            self.batch_out('%s\n' % log.combined )

          for line in log.app_logs:
            safe_msg = pprint.pformat(line.message) # message may include binary data
            msg = "[%s] %s" % ( LEVEL[line.level], safe_msg )
            #self.out('[%s][%s] %s<br>' % (time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(line.time)), line.level, cgi.escape( safe_msg )) )
            messages[msg] = messages.get(msg, 0) + 1
            if raw_logs == 'pretty':
              data = record_to_dict(line)
              data = pprint.pformat(data)
              data = cgi.escape(data)
              self.out("""%s<pre class='errmsg'>%s</pre><br>""" % (pretty_level(line.level), data))
            if raw_logs == 'download':
              t = '%d:%f %s' % (line.level, line.time, line.message)
              t = t.replace("\n", "\n: ") + "\n"
              for l in t.splitlines(True):
                self.batch_out("\t%s" % l)

          res = """[%s] %s""" % (log.status, log.resource)
          index = int( (log.latency - log.pending_time) * 1000 / precision_ms) 
          if log.status >= 400:
            latency_errors[index] = latency_errors.get(index, 0) + 1
            resource_errors[res] = resource_errors.get(res, 0) + 1
          elif log.response_size == 0:
            latency_static[index] = latency_static.get(index, 0) + 1
            resource_static[res] = resource_static.get(res, 0) + 1
          elif log.status == 204:
            latency_cached[index] = latency_cached.get(index, 0) + 1
            resource_cached[res] = resource_cached.get(res, 0) + 1
          else:
            latency_dynamic[index] = latency_dynamic.get(index, 0) + 1
            resource_dynamic[res] = resource_dynamic.get(res, 0) + 1

          if log.pending_time > 0:
            idx = int( log.pending_time / 1000 / precision_ms)
            latency_pending[idx] = latency_pending.get(idx, 0) + 1
            resource_pending[res] = resource_pending.get(res, 0) + 1


          count += 1
          if count == max_requests:
            break

        self.out("""<h1>Summary</h1>""")
        self.out("""<pre>""")
        self.out("""Rows retrieved: %d\n""" % count)
        self.out("""</pre>""")

        if raw_logs == 'pretty':
          self.out('<hr>')

        if count == 0:
          self.out("""
            No logs.
            """)
          return

        # --------------- Latency ---------------
        self.show_latency(precision_ms, latency_dynamic, resource_dynamic, 'Dynamic Requests', 'log.response_size > 0 and log.status != 204 and log.status <= 399')
        self.show_latency(precision_ms, latency_errors,  resource_errors,  'Errors',           'log.status >= 400')
        self.show_latency(precision_ms, latency_static,  resource_static,  'Static Requests',  'log.response_size == 0 and log.status <= 399')
        self.show_latency(precision_ms, latency_cached,  resource_cached,  'Cached Requests',  'log.status == 204')
        self.show_latency(precision_ms, latency_pending, resource_pending, 'Pending Time',     'log.pending_time > 0')

        # --------------- Errors ---------------
        self.out("""<h1>Log message frequency</h1>""")
        if len(messages) == 0:
          self.out("""
            No messages.
            """)
        else:
          for msg in sorted(messages, key=messages.get, reverse=True):
            self.out("""
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
        self.batch = raw_logs == 'download'

        # start_time
        try:
          s = self.request.get('start_time_str')
          t = time.strptime(s, TIME_FORMAT)
          start_time = long(calendar.timegm(t))
        except ValueError:
          # default to '10 minutes ago'
          start_time = (time.time() - 600)

        # end_time
        try:
          s = self.request.get('end_time_str')
          t = time.strptime(s, TIME_FORMAT)
          end_time = long(calendar.timegm(t))
        except ValueError:
          # default to 'now'
          end_time = time.time()

        # desired_action
        desired_action=self.request.get("desired_action")

        # blob_key
        blob_key = self.request.get('blob_key')

        # smooth_seconds
        try:
          smooth_seconds = long(self.request.get('smooth_seconds'))
        except ValueError:
          smooth_seconds = 60

        # seconds_per_shard
        try:
          seconds_per_shard = long(self.request.get('seconds_per_shard'))
        except ValueError:
          seconds_per_shard = 10

        # mr_type
        try:
          mr_type = self.request.get('mr_type')
        except ValueError:
          mr_type = ""
        
        #logging.debug("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%DEBUG")
        #logging.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%INFO")
        #logging.warning("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%WARNING")
        #logging.error("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%ERROR")
        #logging.critical("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%CRITICAL")


        # --------------- HTML Page ---------------
        self.out("""
          <html>
            <head>
              <title>Logservice %s</title>
              <style>
                BODY {
                  font-family: arial;
                  padding: .8em;
                }
                LEGEND {
                  font-weight: bold;
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
                  font-family: monospace;
                  margin: 0.4em 0em;
                }
                .selected {
                  font-weight: bold;
                }
                PRE {
                 padding-left: 1em;
                }
                PRE.small {
                  font-size: 0.8em;
                }
                PRE.errmsg {
                  background-color: #d9d9d9;
                  padding: 0.4em 0.1em;
                  margin: 0em;
                }
                .level_INFO {
                  color: red;
                }
                .ae-logs-severity-4{background-color:#f22;color:#000}
                .ae-logs-severity-3{background-color:#f90;color:#000}
                .ae-logs-severity-2{background-color:#fd0}
                .ae-logs-severity-1{background-color:#3c0;color:#000}
                .ae-logs-severity-0{background-color:#09f;color:#000}
                .ae-logs-severity {
                  height: 1.2em;
                  line-height: 1.2;
                  font-weight: bold;
                  border-radius: 2px;
                  -moz-border-radius: 2px;
                  -webkit-border-radius: 2px;
                }
              </style>
            </head>
            <body>
          """ % app_identity.get_application_id() )

        # --------------- TITLE ---------------
        user = users.get_current_user();
        self.out("""
            <div style='float: right;'>git: <a href='http://code.google.com/p/sauer/source/browse/?repo=logservice' target='_blank'>http://code.google.com/p/sauer.logservice/</a><br>%s</div>
            <div>
              <a href="/?version=%s">&lt;&lt; %s</a>&nbsp;&nbsp;
              <a href='/mapreduce' target='_blank'>/mapreduce</a>
            </div>
          """ % (user.nickname(), version, version) )
        # --------------- 1st FORM ---------------
        self.out("""
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
          self.out("""<option value='%s' %s>%s</option>""" % (k, selected, v) )

        self.out("""
              </select> or above<br>
          """)
 
        start_time_str = human_time(start_time)
        end_time_str = human_time(end_time)
        self.out("""
             Only include requests between <input name='start_time_str' value='%s' size='20'>
             and <input name='end_time_str' value='%s' size='20'><br>
          """ % (start_time_str, end_time_str))
 
        self.out("""
              Histogram precision: <input name='precision_ms' value='%s' size='5'>ms<br>
          """ % precision_ms)

        self.out("""
              <label for='raw_logs_none'>
                <input type='radio' name='raw_logs' value='' id='raw_logs_none' %s>
                Summarize only
              </label><br>
              <label for='raw_logs_pretty'>
                <input type='radio' name='raw_logs' value='pretty' id='raw_logs_pretty' %s>
                Pretty print raw logs
              </label><br>
              <label for='raw_logs_download'>
                <input type='radio' name='raw_logs' value='download' id='raw_logs_download' %s>
                Download raw logs
              </label><br>
          """ % ("checked" if raw_logs == '' else "",
                "checked" if raw_logs == 'pretty' else "",
                "checked" if raw_logs == 'download' else "") )

        self.out("""
              <input type='hidden' name='desired_action' value='grep'>
              <input type='submit' value='Summarize'>

            </form>
          </fieldset>
          """)

        # --------------- 2nd FORM ---------------
        self.out("""
          <fieldset>
            <legend>Logs MapReduce</legend>
            <form action='/'>
              Application version: <input name='version' value='%s' size='20'><br>
        """ % version)

        self.out("""
             Only include requests between <input name='start_time_str' value='%s' size='20'>
             and <input name='end_time_str' value='%s' size='20'><br>
          """ % (start_time_str, end_time_str))
 
        self.out("""
              Each shard should fetch log records for <input name='seconds_per_shard' value='%d' size='10'> seconds elapsed time<br>
          """ % seconds_per_shard )

        self.out("""
              <label for='mr_type_graph'>
                <input type='radio' name='mr_type' value='graph' id='mr_type_graph' %s>
                Create fun graphs
              </label><br>
              <label for='mr_type_collect'>
                <input type='radio' name='mr_type' value='collect' id='mr_type_collect' %s>
                Collect in Blobstore for later download
              </label><br>
          """ % ("checked" if mr_type == 'graph' else "",
                "checked" if mr_type == 'collect' else "") )

        self.out("""
              <input type='hidden' name='desired_action' value='mapreduce'>
              <input type='submit' value='MapReduce' onclick='return document.getElementById("mr_type_collect").checked || document.getElementById("mr_type_graph").checked;'> 
          """)

        self.out("""
            </form>
          </fieldset>
          """)

        results = db.Query(LogServiceMapReduceResult).order('-end_time').fetch(limit=10)
        if results:
          self.out("""
            <fieldset>
              <legend>Visualize MapReduce results</legend>
              <form action='/' name='visualize_map_reduce_form'>
            """)

          self.out("""
                <input type='hidden' name='blob_key' value='%s'>
                <input type='hidden' name='version'>
                <script>
                  function visualize_map_reduce(version, blob_key) {
                    console.log("version= " + version);
                    console.log("blob_key= " + blob_key);
                    f = document.forms["visualize_map_reduce_form"];
                    f.blob_key.value = blob_key;
                    f.version.value = version;
                    f.submit();
                  }

                  function set_smooth_seconds(s) {
                    f = document.forms["visualize_map_reduce_form"];
                    f.smooth_seconds.value = s;
                    f.submit();
                  }
                </script>
            """ % blob_key)

          self.out("""
                Smooth results over <input name='smooth_seconds' value='%s' size='10'> (&#8592;
            """ % smooth_seconds)
          for s in [1, 10, 60, 300, 600, 3600]:
            if s == smooth_seconds:
              selected = "selected"
            else:
              selected = ""
            self.out("""<input type='button' value='%d' onClick='set_smooth_seconds(this.value);' class='%s'>""" % (s, selected) )
          self.out(""") seconds<br>""")

          for result in results:
            key = result.blob_key
            v = result.version
            if key == blob_key:
              css_class = "selected"
            else:
              css_class = ""
            self.out("""
              <div class='%s'>
                <input type=button onClick='visualize_map_reduce("%s", "%s")' value='visualize'>
                 start_time=%s, end_time=%s, version=%s
                 <a style='font-size: 0.6em; padding-left: 1em;' href='%s' _target='_blank'>%s</a>
              </div>""" % (css_class,
                           v, key,
                           human_time(result.start_time), human_time(result.end_time), v,
                           key, key) )
            if key == blob_key:
              t = pprint.pformat(db.to_dict(result))
              self.out("""<pre class='small'>%s</pre>""" % t)

          self.out("""
                <input type='hidden' name='desired_action' value='visualize'>
              </form>
            </fieldset>
            """)

        # --------------- Conditional content ---------------
        if desired_action == "mapreduce":
          shards = int(math.ceil(float(end_time - start_time) / float(seconds_per_shard)))
          self.do_mapreduce(mr_type, shards, start_time, end_time, version)
        elif desired_action == "grep":
          self.do_grep(version, max_requests, level, start_time, end_time, precision_ms, raw_logs)
        elif desired_action == "visualize":
          self.do_visualize(blob_key, smooth_seconds)

        # --------------- End of page ---------------
        self.out("""
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
