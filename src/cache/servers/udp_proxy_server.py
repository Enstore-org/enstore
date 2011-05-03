#!/usr/bin/env python

##############################################################################
#
# $Id$
#
##############################################################################

# system import
import sys
import os
import time
import string
import traceback
import threading

# enstore imports
import Trace
import log_client
import e_errors
import configuration_client
import generic_server
import dispatching_worker
import option
# enstore cache imports
#import cache.servers
import udp2amq

MY_FULL_NAME = "UDP to AQMQP message proxy server"

timing = False

class U2As(dispatching_worker.DispatchingWorker, generic_server.GenericServer):
    """
    Configurable UDP to AMQP proxy server

    """

    def  __init__(self, name, cs):
        # Obtain information from the configuration server cs.
        self.csc = cs
        #self.name = name
        generic_server.GenericServer.__init__(self, self.csc, name,
					      function = self.handle_er_msg)

        # get my configuration
        self.conf = self.csc.get(name) 
        Trace.trace(10, "proxy_server, conf=%s" % (self.conf,))
        
        if not e_errors.is_ok(self.conf):
            e_message = "Unable to acquire configuration info for %s: %s: %s" % \
                      (name, self.conf['status'][0], self.conf['status'][1])
            Trace.log(e_errors.ERROR, e_message)
            Trace.trace(10, "proxy_server, error in conf")
            raise e_errors.EnstoreError(e_errors.CONFIGDEAD)

        dispatching_worker.DispatchingWorker.__init__(self, (self.conf['hostip'],
                                                             self.conf['port']),
                                                      use_raw=0)



        # get amqp broker configuration - common for all
        self.amqp_broker_conf = self.csc.get("amqp_broker")
        Trace.trace(10, "proxy_server, amqp_broker_conf=%s" % (self.amqp_broker_conf,))
         
        if not e_errors.is_ok(self.amqp_broker_conf):
            e_message = "Unable to acquire configuration info for %s: %s: %s" % \
                      (MY_NAME, self.amqp_broker_conf['status'][0], self.amqp_broker_conf['status'][1])
            Trace.log(e_errors.ERROR, e_message)
            Trace.trace(10, "proxy_server, error in amqp_broker_conf")
            raise e_errors.EnstoreError(e_errors.CONFIGDEAD)

        Trace.trace(10, "proxy_server, extract conf")
        try:
            brk = self.amqp_broker_conf['host_port']
            udp_srv = (self.conf['host'], self.conf['udp_port'])
            target = self.conf['target_addr']
            print "TARGET", target
        except Exception, detail:
            Trace.trace(10, "proxy_server, got exception %s"%(detail,))

        Trace.trace(10, "proxy_server, creating u2a_srv")
        #Open connection to qpid broker
        Trace.log(e_errors.INFO, "create udp server instance, qpid client instance")
        # self.u2a_srv = cache.servers.udp2amq.UDP2amq(udp_srv, use_raw=1, amq_broker=brk, target_addr=target)
        print "SRV", udp_srv, brk, target
        
        self.u2a_srv = udp2amq.UDP2amq(udp_srv, use_raw=1, amq_broker=brk, target_addr=target)

        Trace.trace(10, "proxy_server,u2a created")
        
    def start(self):
        # if UDP server is in raw mode :
        if self.u2a_srv.use_raw:
            try:
                self.u2a_srv.set_out_file()
                # start receiver thread or process
                self.u2a_srv.raw_requests.receiver()
            except:
                exc_type, exc_value = sys.exc_info()[:2]
                e_message = str(exc_type)+' '+str(exc_value)+' cannot start raw requests receiver'
                Trace.alarm(e_errors.ERROR, e_message, {})
                raise e_errors.EnstoreError(e_errors.UNKNOWN)

        if not self.u2a_srv:
            return
        Trace.trace(10, "Starting proxy")
        try:
            self.u2a_srv.start()
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            e_message = str(exc_type)+' '+str(exc_value)+' IS QPID BROKER RUNNING?'
            Trace.alarm(e_errors.ERROR, e_message, {})

            raise e_errors.EnstoreError(e_errors.UNKNOWN)
        
        Trace.trace(10, "proxy_server,started in start()")

    def stop(self):
        self.u2a_srv.stop()

    ####################################################################

    # These extract value functions are used to get a value from the ticket
    # and perform validity checks in a consistent fashion.  These functions
    # duplicated in file_clerk.py; they should be made more generic to
    # eliminate maintaining two sets of identical code.

    def extract_value_from_zzz(self, key, ticket, fail_None = False):
        try:
            value = ticket[key]
        except KeyError, detail:
            message =  "%s: key %s is missing" % (MY_NAME, detail,)
            ticket["status"] = (e_errors.KEYERROR, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            return None

        if fail_None and value == None:
            message =  "%s: key %s is None" % (MY_NAME, key,)
            ticket["status"] = (e_errors.KEYERROR, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            return None

        return value


class U2AsInterface(generic_server.GenericServerInterface):
    def __init__(self):
        # fill in the defaults for possible options
        generic_server.GenericServerInterface.__init__(self)


    U2As_proxy_options = {}

    # define the command line options that are valid
    def valid_dictionaries(self):
        return generic_server.GenericServerInterface.valid_dictionaries(self) \
               + (self.U2As_proxy_options,)

    parameters = ["udp_proxy_server"]

    # parse the options like normal but make sure we have a library manager
    def parse_options(self):
        option.Interface.parse_options(self)
        # bomb out if we don't have a library manager
        if len(self.args) < 1 :
            self.missing_parameter(self.parameters())
            self.print_help()
            sys.exit(1)
        else:
            self.name = self.args[0]
    


def do_work():
    # get an interface
    intf = U2AsInterface()

    # get a library manager
    u2a = U2As(intf.name, (intf.config_host, intf.config_port))
    u2a.handle_generic_commands(intf)

    Trace.init( u2a.log_name, 'yes')
    u2a._do_print({'levels':range(5, 400)}) # no manage_queue
    u2a.start()
 
    while True:
        t_n = 'enstore_server'
        if thread_is_running(t_n):
            pass
        else:
            Trace.log(e_errors.INFO, "proxy_server %s (re)starting %s"%(intf.name, t_n))
            #lm.run_in_thread(t_n, lm.mover_requests.serve_forever)
            dispatching_worker.run_in_thread(t_n, u2a.serve_forever)

        '''
        try:
            t_n = 'u2a_proxy'
            if thread_is_running(t_n):
                pass
            else:
                Trace.log(e_errors.INFO, "%s (re)starting %s"%(intf.name, t_n))
                #lm.run_in_thread(t_n, lm.mover_requests.serve_forever)
                dispatching_worker.run_in_thread(t_n, u2a.start)

        except SystemExit, exit_code:
            sys.exit(exit_code)
        except:
            traceback.print_exc()
        '''
        time.sleep(10)
        

    Trace.alarm(e_errors.ALARM,"U2A proxy server %sfinished (impossible)"%(intf.name,))
    
# check if named thread is running
def thread_is_running(thread_name):
    threads = threading.enumerate()
    for thread in threads:
        if ((thread.getName() == thread_name) and thread.isAlive()):
            Trace.trace(10, "running")
            return True
    else:
        Trace.trace(10, "not running")
        return False


if __name__ == "__main__":
    do_work()
 
