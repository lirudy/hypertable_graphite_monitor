#!/usr/bin/python
"""Copyright 2008 Orbitz WorldWide

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import sys
import time
import os
import json
import StringIO
import platform 
import subprocess
from socket import socket

CARBON_SERVER = '172.16.40.54'
CARBON_PORT = 2003

HYPERTABLE_ROOT = '/dinglicom/hypertable/'
MONITORING_PATH = "/current/run/monitoring/"
RRD_RANGESERV = "rangeservers/"
RRD_TABLE = "tables/"

JSON_TABLE = "table_summary.json"
JSON_RANGE = "rangeserver_summary.json"
JSON_MASTER = "master_summary.json"



delay = 15 
if len(sys.argv) > 1:
    delay = int( sys.argv[1] )

def get_ht_rrd(filepaths):
    command = "rrdtool lastupdate {0}"
    reslist = {}
    
    for (filename, filepath) in filepaths:
        if filepath.find(".rrd") < 0:
            continue
        run_cmd = command.format(filepath)
        process = subprocess.Popen(run_cmd, stdout=subprocess.PIPE, shell=True)
        os.waitpid(process.pid, 0)
        output = process.stdout.read()
        reslist[filename] = output
    return reslist
    
def gen_ht_rrd_msg(res_list):

    range_name = "rangeserver.{0}.{1}.1min {2} {3}"
    lines = []
    
    def deal_rrd(name, info):
        msgs = []
        msg_list = info.strip().split("\n")
        heads = msg_list[0].split(" ")
        vals = msg_list[2].split(" ")
        now = vals[0]
        for idx in range(len(heads)):
            msg = range_name.format(name, heads[idx], vals[idx+1], now[:-1])
            lines.append(msg)
            
    for (keyname, res) in res_list.items():
        deal_rrd(keyname, res)
        
    return lines 

def get_ht_json(json_path):
    
    try:
        j_obj = open(json_path).read()
        j_obj = json.loads(j_obj)
        return j_obj
    except Exception, exc:
        print(sys.stderr, "retrieving err: {0}".format(exc))
        
    
def get_table_rrd_file_list(j_obj):
    rrd_file_list = []
    rrd_table_name = []
    if j_obj != None:
        table_list = j_obj.get("TableSummary").get("tables")
       
        if None != table_list and len(table_list) > 0:
            for table_obj in table_list:
                table_id = table_obj.get("id")
                table_name = table_obj.get("name")
                rrd_path = "{0}{1}{2}{3}{4}".format(HYPERTABLE_ROOT, MONITORING_PATH, RRD_TABLE, table_id, "_table_stats_v0.rrd")
                rrd_file_list.append(rrd_path)
                rrd_table_name.append(table_name)
                
    return rrd_file_list, rrd_table_name

def run_t():
            
        
    sock = socket()
    try:
        sock.connect( (CARBON_SERVER,CARBON_PORT) )
    except:
        print "Couldn't connect to %(server)s on port %(port)d, is carbon-agent.py running?" % { 'server':CARBON_SERVER, 'port':CARBON_PORT }
        sys.exit(1)
    
    def send(messages):
        message = '\n'.join(lines) + '\n' #all lines must end in a newline
        print "sending message\n"
        print '-' * 80
        print message
        print
        sock.sendall(message)
        
    while True:
        now = int( time.time() )
        rs_filelists = os.listdir("{0}{1}{2}".format(HYPERTABLE_ROOT, MONITORING_PATH, RRD_RANGESERV))
      
        filelists = rs_filelists
        for filename in filelists:
            filepath = "{0}{1}{2}{3}".format(HYPERTABLE_ROOT, MONITORING_PATH, RRD_RANGESERV, filename)
            key_name = filename.split("_")[0]
            files = [(key_name, filepath)]
            res_list = get_ht_rrd(files)
            lines = gen_ht_rrd_msg(res_list)
            send(lines)
        
        
        rangeserver_json_path = "{0}{1}{2}".format(HYPERTABLE_ROOT, MONITORING_PATH, JSON_RANGE)
        table_json_path = "{0}{1}{2}".format(HYPERTABLE_ROOT, MONITORING_PATH, JSON_TABLE)
        master_json_path = "{0}{1}{2}".format(HYPERTABLE_ROOT, MONITORING_PATH, JSON_MASTER)
      
        print(table_json_path)
        table_sum_json = get_ht_json(table_json_path)
        tb_filelists, tb_names = get_table_rrd_file_list(table_sum_json)
        filelists = tb_filelists
        for idx in range(len(filelists)):
            files = [(tb_names[idx], filelists[idx])]
            print(files)
            res_list = get_ht_rrd(files)
            print(res_list)
            lines = gen_ht_rrd_msg(res_list)
            send(lines)
      
        time.sleep(delay)


if __name__ == "__main__":
    
    run_t()
    
    
    
    
    
