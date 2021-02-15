#!/usr/bin/env python

from __future__ import print_function
import sys
import time
import configuration_client
import log_client
import enstore_constants
import Trace
import e_errors
import generic_server
import dispatching_worker
import multiprocessing


class Test(dispatching_worker.DispatchingWorker, generic_server.GenericServer):
    def __init__(self, csc, name, logclient):
        generic_server.GenericServer.__init__(self, csc, name,
                                              function=self.handle_er_msg,
                                              logc=logclient)
        self.logclient = logclient
        Trace.init(name, 'yes')

    def send_log_msg(self, max_count):
        count = 0
        while count < max_count:
            tm = time.localtime(time.time())
            msg_full = '%02d:%02d:%02d %s' % (tm[3], tm[4], tm[5], msg)
            Trace.log(e_errors.INFO, "%s %s" % (msg_full, count,))
            time.sleep(0.001)
            count += 1
        print("send_log_msg done")
        #Trace.alarm(e_errors.ALARM, "And this is alarm")

    def run_proc(self, max_count):
        mw_proc = multiprocessing.Process(
            target=self.send_log_msg, args=(max_count,))
        mw_proc.start()
        mw_proc.join()


class Test1():
    def __init__(self, csc, name, logclient):
        self.logclient = logclient
        Trace.init(name, 'yes')
        Trace.set_log_func(logc.log_func)

    def send_log_msg(self, max_count):
        count = 0
        while count < max_count:
            tm = time.localtime(time.time())
            msg_full = '%02d:%02d:%02d %s' % (tm[3], tm[4], tm[5], msg)
            Trace.log(e_errors.INFO, "%s %s" % (msg_full, count,))
            time.sleep(0.001)
            count += 1
        print("send_log_msg done")
        #Trace.alarm(e_errors.ALARM, "And this is alarm")

    def run_proc(self, max_count):
        mw_proc = multiprocessing.Process(
            target=self.send_log_msg, args=(max_count,))
        mw_proc.start()
        mw_proc.join()


msg = sys.argv[1]
max_count = int(sys.argv[2])
intf = log_client.LoggerClientInterface(user_mode=0)
name = "STRESS"
# Trace.init(name)

logc = log_client.TCPLoggerClient((intf.config_host, intf.config_port), name)

t = Test((intf.config_host, intf.config_port), name, logc)

# t.send_log_msg(max_count)
t.run_proc(max_count)
#t = Test1((intf.config_host, intf.config_port), name, logc)

# t.send_log_msg(max_count)
# t.run_proc(max_count)
print("DONE")
sys.exit(0)
