#!/usr/bin/env python

from __future__ import print_function
import sys
import time
import configuration_client
import log_client
import enstore_constants
import Trace
import e_errors

msg = sys.argv[1]
max_count = int(sys.argv[2])
intf = log_client.LoggerClientInterface(user_mode=0)
name = "STRESS"
Trace.init(name)

logc = log_client.TCPLoggerClient((intf.config_host, intf.config_port), name,
                                  enstore_constants.LOG_SERVER)
Trace.set_log_func(logc.log_func)
count = 0

while count < max_count:
    tm = time.localtime(time.time())
    msg_full = '%02d:%02d:%02d %s' % (tm[3], tm[4], tm[5], msg)
    Trace.log(e_errors.INFO, "%s %s" % (msg_full, count,))
    #log_client.logit(logc, "%s %s"%(msg_full, count,))
    # time.sleep(0.001)
    time.sleep(0.001)
    # time.sleep(.01)
    count += 1
Trace.alarm(e_errors.ALARM, "And tgish is alarm")

logc.stop()
print("EXIT")
