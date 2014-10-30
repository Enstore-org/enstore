#!/usr/bin/env python

import sys
import time
import configuration_client
import log_client
import enstore_constants
import Trace

msg = sys.argv[1]
max_count = int(sys.argv[2])
intf = log_client.LoggerClientInterface(user_mode=0)
name="STRESS"
Trace.init(name)
logc = log_client.LoggerClient((intf.config_host, intf.config_port), name,
                               enstore_constants.LOG_SERVER)

logc = log_client.LoggerClient((intf.config_host, intf.config_port), name,
                               enstore_constants.LOG_SERVER)

count = 0

while count < max_count:
    log_client.logit(logc, "%s %s"%(msg, count,))
    #time.sleep(0.001)
    count += 1
    
          
