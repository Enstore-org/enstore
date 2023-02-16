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
import logging

# enstore imports
import e_errors
import configuration_client

# enstore cache imports
import cache.stub.mw as mw

# there are multuple migrator workers - add specifics
MY_NAME = "mw"
MY_FULL_NAME = "Migration Worker server" 

class MWs(object):
    """
    Configurable Migration Worker server
    """
    def _assert_conf(self,cf_dict,cf_type):
        """assert status of configuration received from configuration server"""
        # @todo check type is dict and it has status field, or write log and throw yet another exception 
        if not e_errors.is_ok(cf_dict):
            self.log.error("Unable to acquire %s configuration info for %s, status: %s: %s",
                           cf_type, self.name, cf_dict['status'][0], cf_dict['status'][1])
            self.trace.debug("error in configuration")
            raise e_errors.EnstoreError(e_errors.CONFIGDEAD)
        return cf_dict

    def  __init__(self, cs, name):
        """
        Creates Migration Worker Server. Constructor gets configuration from enstore Configuration Server
        
        @type cs: (str,int)
        @param cs: Configuration Server Client (host,port) tuple, usually from environment
        @type name: str
        @param name: name - migration worker name
        """
        # @todo check 'name' is str or unicode, raise exception 
        # get loggers for log and trace
        self.log = logging.getLogger('log.encache.%s' % name)
        self.trace = logging.getLogger('trace.encache.%s' % name)

        # Obtain information from the configuration server cs
        self.csc = configuration_client.ConfigurationClient(cs)
        
        self.name = name
        self.config = {}
        
        # get my configuration
        conf = self.csc.get(self.name) 
        self.trace.debug("got configuration for %s as %s", self.name, self.conf)
        self.config["server"] = self._assert_conf(conf,"server")

        # get amqp broker configuration - common for all servers
        # @todo - change name in configuration file to make it more generic, "amqp"
        conf_amqp = self.csc.get("amqp_broker")
        self.trace.debug("amqp_broker_conf",self.amqp_broker_conf)
        self.config["amqp"] = self._assert_conf(conf_amqp,"amqp broker")
        
        self.trace.log("create %s server instance, qpid client instance",self.name)
        self.srv = mw.MigrationWorker(self.name,self.config)
        #self.srv = mw.MigrationWorker(amq_broker=brk, queue_work=queue_work, myaddr=queue_in, target=queue_out)
        self.trace.debug("server created")

    def start(self):
        if not self.srv:
            return
        
        try:
            self.srv.start()
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            self.log.critical("%s %s IS QPID BROKER RUNNING?",str(exc_type),str(exc_value))
            self.log.error("CAN NOT ESTABLISH CONNECTION TO QPID BROKER ... QUIT!")
            raise e_errors.EnstoreError(e_errors.UNKNOWN)
        
        self.trace.debug("server started in start()")

    def stop(self):
        self.srv.stop()
        self.trace.debug("server stopped in stop()")

if __name__ == "__main__":   # pragma: no cover
    # Test Unit
    import cache.en_logging.config_test_unit

    # get configuration server
    conf_srv = (os.environ['ENSTORE_CONFIG_HOST'], int(os.environ['ENSTORE_CONFIG_PORT']))

    # my name
    MY_ID=123
    name = "%s_%d" % (MY_NAME,MY_ID)
    MY_FULL_NAME = "%s %s" % (MY_FULL_NAME, MY_ID)

    # create and configure top level logger
    #   log.encache.messaging and log.encache.<migration workers> inherit from log.encache
    log, trace = cache.en_logging.config_test_unit.set_logging_console(name, full_name=MY_FULL_NAME)

    log.info('%s' % (sys.argv,))

    trace.info("ready") 
    while True: # serve forever
        try:
            # get MW instance
            server = MWs(conf_srv, name)
        except:
            log.error("can't create server instance, will retry " )
            time.sleep(10)
            continue

        try:
            log.info("(re)starting" )
            server.start()
            trace.debug("started [main]")            
            # stop server if there was keyboard interrupt 
            # TODO: shutdown message
            while not server.srv.finished :
                try:
                    time.sleep(10)
                except KeyboardInterrupt:
                    log.info("Keyboard interrupt")
                    break

        except SystemExit, exit_code:
            sys.exit(exit_code)
        except:
            continue
        finally:
            log.info("stopping" )
            server.srv.stop()
            trace.debug("stopped [main]") 
            del server
            log.info("finished")
