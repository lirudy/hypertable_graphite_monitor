hypertable_graphite_monitor
===========================

python script for monitoring hypertable, send plain text matric into graphite


first step:

  modify graphite ip/port:
  
  
  CARBON_SERVER = '{Your Graphite carbon IP}'
  CARBON_PORT = 2003
  
  
second step:
  run it:
     setsid python ht_mon.py &>/dev/null
