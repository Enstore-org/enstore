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

# enstore imports
import Trace
import log_client
import e_errors
import configuration_client

# enstore cache imports
#import cache.servers
import udp2amq

MY_NAME = "udp2amq_proxy"
MY_FULL_NAME = "UDP to AQMQP message proxy server"

debug = False
timing = False

class U2As(object):
    """
    Configurable UDP to AMQP proxy server

    """

    def  __init__(self, cs):
        # Obtain information from the configuration server cs.
        self.csc = configuration_client.ConfigurationClient(cs)

        # get my configuration
        self.conf = self.csc.get(MY_NAME) 
        if debug:
            print "DEBUG u2a, conf=%s" % self.conf
        
        if not e_errors.is_ok(self.conf):
            e_message = "Unable to acquire configuration info for %s: %s: %s" % \
                      (MY_NAME, self.conf['status'][0], self.conf['status'][1])
            Trace.log(e_errors.ERROR, e_message)
            if debug:
                print "DEBUG u2a, error in conf"
            raise e_errors.EnstoreError(e_errors.CONFIGDEAD)

        # get amqp broker configuration - common for all
        self.amqp_broker_conf = self.csc.get("amqp_broker")
        if debug:
            print "DEBUG u2a, amqp_broker_conf=%s" % self.amqp_broker_conf
         
        if not e_errors.is_ok(self.amqp_broker_conf):
            e_message = "Unable to acquire configuration info for %s: %s: %s" % \
                      (MY_NAME, self.amqp_broker_conf['status'][0], self.amqp_broker_conf['status'][1])
            Trace.log(e_errors.ERROR, e_message)
            if debug:
                print "DEBUG u2a, error in amqp_broker_conf"
            raise e_errors.EnstoreError(e_errors.CONFIGDEAD)

        if debug:
            print "DEBUG u2a, extract conf"
        try:
            brk = self.amqp_broker_conf['host_port']
            udp_srv = self.conf['host_port']
            target = self.conf['target_addr']
        except:
            if debug:
                print "DEBUG, got exception"

        if debug:
            print "DEBUG u2a creating u2a_srv" 
        #Open connection to qpid broker
        Trace.log(e_errors.INFO, "create udp server instance, qpid client instance")
#        self.u2a_srv = cache.servers.udp2amq.UDP2amq(udp_srv, use_raw=1, amq_broker=brk, target_addr=target)
        self.u2a_srv = udp2amq.UDP2amq(udp_srv, use_raw=1, amq_broker=brk, target_addr=target)

        if debug:
            print "DEBUG u2a created" 
        
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
                Trace.log(e_errors.ERROR, e_message)
                Trace.alarm(e_errors.ERROR, e_message, {})
                Trace.log(e_errors.ERROR, "cannot start raw requests receiver")
                raise e_errors.EnstoreError(e_errors.UNKNOWN)

        if not self.u2a_srv:
            return
        
        try:
            self.u2a_srv.start()
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            e_message = str(exc_type)+' '+str(exc_value)+' IS QPID BROKER RUNNING?'
            Trace.log(e_errors.ERROR, e_message)
            Trace.alarm(e_errors.ERROR, e_message, {})

            Trace.log(e_errors.ERROR, "CAN NOT ESTABLISH CONNECTION TO QPID BROKER ... QUIT!")
            raise e_errors.EnstoreError(e_errors.UNKNOWN)
        
        if debug:
            print "DEBUG u2a started in start()" 

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


if __name__ == "__main__":   # pragma: no cover
    conf_srv = (os.environ['ENSTORE_CONFIG_HOST'], int(os.environ['ENSTORE_CONFIG_PORT']))

    Trace.init(string.upper(MY_NAME))
    logc = log_client.LoggerClient(conf_srv, MY_NAME)

    try:
        # get a udp proxy instance
        u2a = U2As(conf_srv)
    except:
        exit(1)

    Trace.log(e_errors.INFO, '%s' % (sys.argv,))

    if debug:
        print "DEBUG u2a ready" 
    while True:
        try:
            Trace.log(e_errors.INFO, "%s (re)starting" % MY_FULL_NAME )
            u2a.start()
            if debug:
                print "DEBUG u2a started [main]"            
            # stop UDP2amq server if there was keyboard interrupt (TODO: shutdown message)
            while not u2a.u2a_srv.finished :
                try:
                    time.sleep(10)
                except KeyboardInterrupt:
                    Trace.log(e_errors.INFO, "%s Keyboard interrupt" % MY_FULL_NAME )
                finally:
                    Trace.log(e_errors.INFO, "%s stopping" % MY_FULL_NAME )
                    u2a.u2a_srv.stop()
                    if debug:
                        print "DEBUG u2a stopped" 
                    break
            
        except SystemExit, exit_code:
            sys.exit(exit_code)
        except:
            continue
        finally:
            del u2a
            Trace.trace(e_errors.ERROR, ("%s finished") % MY_FULL_NAME)
