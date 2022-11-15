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
import lmd
#import cache.messaging.messages

MY_NAME = "lmd"
MY_FULL_NAME = "Location Manager Dispatcher server"

debug = True
timing = False

class LMDs(object):
    """
    Configurable Library Manager Dispatcher server

    """

    def  __init__(self, cs):
        # Obtain information from the configuration server cs.
        self.csc = configuration_client.ConfigurationClient(cs)

        # get my configuration
        self.conf = self.csc.get(MY_NAME) 
        if debug: print "DEBUG lmd_srv, conf=%s" % self.conf
        
        if not e_errors.is_ok(self.conf):
            e_message = "Unable to acquire configuration info for %s: %s: %s" % \
                      (MY_NAME, self.conf['status'][0], self.conf['status'][1])
            Trace.log(e_errors.ERROR, e_message)
            if debug: print "DEBUG lmd_srv, error in conf"
            raise e_errors.EnstoreError(e_errors.CONFIGDEAD)

        # get amqp broker configuration - common for all
        self.amqp_broker_conf = self.csc.get("amqp_broker")
        if debug: print "DEBUG lmd_srv, amqp_broker_conf=%s" % self.amqp_broker_conf
         
        if not e_errors.is_ok(self.amqp_broker_conf):
            e_message = "Unable to acquire configuration info for %s: %s: %s" % \
                      (MY_NAME, self.amqp_broker_conf['status'][0], self.amqp_broker_conf['status'][1])
            Trace.log(e_errors.ERROR, e_message)
            if debug: print "DEBUG lmd_srv, error in amqp_broker_conf"
            raise e_errors.EnstoreError(e_errors.CONFIGDEAD)

        if debug: print "DEBUG lmd_srv, extract conf"
        try:
            brk = self.amqp_broker_conf['host_port']
            queue_in  = self.conf['queue_in']
            queue_out = self.conf['queue_out']
        except:
            if debug: print "DEBUG, got exception"

        if debug: print "DEBUG lmd_srv creating lmd_srv" 
        #Open connection to qpid broker
        Trace.log(e_errors.INFO, "create udp server instance, qpid client instance")
        self.lmd_srv = lmd.LMD(amq_broker=brk, myaddr=queue_in, target=queue_out)

        if debug: print "DEBUG lmd_srv created" 
        
    def start(self):
        if not self.lmd_srv:
            return
        
        try:
            self.lmd_srv.start()
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            e_message = str(exc_type)+' '+str(exc_value)+' IS QPID BROKER RUNNING?'
            Trace.log(e_errors.ERROR, e_message)
            Trace.alarm(e_errors.ERROR, e_message, {})

            Trace.log(e_errors.ERROR, "CAN NOT ESTABLISH CONNECTION TO QPID BROKER ... QUIT!")
            raise e_errors.EnstoreError(e_errors.UNKNOWN)
        
        if debug: print "DEBUG lmd_srv started in start()" 

    def stop(self):
        self.lmd_srv.stop()

if __name__ == "__main__":   # pragma: no cover
    conf_srv = (os.environ['ENSTORE_CONFIG_HOST'], int(os.environ['ENSTORE_CONFIG_PORT']))

    Trace.init(string.upper(MY_NAME))
    logc = log_client.LoggerClient(conf_srv, MY_NAME)

    Trace.log(e_errors.INFO, '%s' % (sys.argv,))

    if debug: print "DEBUG lmd_srv ready" 
    while True: # run forever
        try:
            # get LMD instance
            lmds = LMDs(conf_srv)
        except:
            Trace.log(e_errors.INFO, "%s can't create LDMs instance, will retry " % MY_FULL_NAME )
            time.sleep(10)
            continue

        try:
            Trace.log(e_errors.INFO, "%s (re)starting" % MY_FULL_NAME )
            lmds.start()
            if debug: print "DEBUG lmd_srv started [main]"            
            # stop LDM server if there was keyboard interrupt (TODO: shutdown message)
            while not lmds.lmd_srv.finished :
                try:
                    time.sleep(10)
                except KeyboardInterrupt:
                    Trace.log(e_errors.INFO, "%s Keyboard interrupt" % MY_FULL_NAME )
                    break

        except SystemExit, exit_code:
            sys.exit(exit_code)
        except:
            continue
        finally:
            Trace.log(e_errors.INFO, "%s stopping" % MY_FULL_NAME )
            lmds.lmd_srv.stop()
            if debug: print "DEBUG lmd_srv stopped" 
            del lmds
            Trace.trace(e_errors.ERROR, ("%s finished") % MY_FULL_NAME)
