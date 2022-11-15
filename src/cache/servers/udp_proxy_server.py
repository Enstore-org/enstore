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
import monitored_server
import event_relay_messages
import event_relay_client
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
        self.name = name
        generic_server.GenericServer.__init__(self, self.csc, name,
					      function = self.handle_er_msg)

        # get my configuration
        self.conf = self.csc.get(name)
        Trace.trace(10, "proxy_server, conf=%s" % (self.conf,))
        self.alive_interval = monitored_server.get_alive_interval(self.csc,
                                                                  name,
                                                                  self.conf)
        
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
                      (self.name, self.amqp_broker_conf['status'][0], self.amqp_broker_conf['status'][1])
            Trace.log(e_errors.ERROR, e_message)
            Trace.trace(10, "proxy_server, error in amqp_broker_conf")
            raise e_errors.EnstoreError(e_errors.CONFIGDEAD)

        Trace.trace(10, "proxy_server, extract conf")
        try:
            brk = self.amqp_broker_conf['host'], self.amqp_broker_conf['port']
            udp_srv = (self.conf['host'], self.conf['udp_port'])
            target = self.conf['target_addr']
        except Exception, detail:
            Trace.trace(10, "proxy_server, got exception %s"%(detail,))

        Trace.trace(10, "proxy_server, creating u2a_srv")
        #Open connection to qpid broker
        Trace.log(e_errors.INFO, "create udp server instance, qpid client instance")
        # self.u2a_srv = cache.servers.udp2amq.UDP2amq(udp_srv, use_raw=1, amq_broker=brk, target_addr=target)
        
        self.u2a_srv = udp2amq.UDP2amq(udp_srv, use_raw=1, amq_broker=brk, target_addr=target)

        Trace.trace(10, "proxy_server,u2a created")
        # setup the communications with the event relay task
        self.resubscribe_rate = 300
        self.erc = event_relay_client.EventRelayClient(self, function = self.handle_er_msg)
        Trace.erc = self.erc # without this Trace.notify takes 500 times longer
        self.erc.start([event_relay_messages.NEWCONFIGFILE],
                       self.resubscribe_rate)

        # start our heartbeat to the event relay process
        self.erc.start_heartbeat(name, self.alive_interval)
        
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

    # parse the options like normal but make sure we have a proxy server
    def parse_options(self):
        option.Interface.parse_options(self)
        # bomb out if we don't have a proxy server
        if len(self.args) < 1 :
            self.missing_parameter(self.parameters())
            self.print_help()
            sys.exit(1)
        else:
            self.name = self.args[0]
    


def do_work():
    # get an interface
    intf = U2AsInterface()

    # create a udp_proxy instance
    u2a = U2As(intf.name, (intf.config_host, intf.config_port))
    u2a.handle_generic_commands(intf)

    Trace.init( u2a.log_name, 'yes')
    # u2a._do_print({'levels':range(5, 400)}) # leave it here for debugging purposes
    u2a.start()
 
    while True:
        t_n = 'enstore_server'
        if thread_is_running(t_n):
            pass
        else:
            Trace.log(e_errors.INFO, "proxy_server %s (re)starting %s"%(intf.name, t_n))
            dispatching_worker.run_in_thread(t_n, u2a.serve_forever)
        time.sleep(10)
        

    Trace.alarm(e_errors.ALARM,"U2A proxy server %s finished (impossible)"%(intf.name,))
    
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


if __name__ == "__main__":   # pragma: no cover
    do_work()
 
