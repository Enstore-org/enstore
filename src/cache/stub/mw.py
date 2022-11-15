#!/usr/bin/env python

##############################################################################
#
# $Id$
##############################################################################


'''
    MW - Enstore File Cache Migration worker core functionality implementation

    implements everything what can work and be tested without full enstore installation
'''

# system imports
import sys
import time
import types
from multiprocessing import Pool,Process
import threading
import logging

# qpid / amqp
import qpid.messaging
import Queue

# enstore imports
import e_errors
import enstore_constants

# enstore cache imports
import cache.errors
#from cache.messaging.client import EnQpidClient
import cache.messaging.client2 as cmc
import cache.messaging.mw_client
import cache.messaging.pe_client

from cache.messaging.messages import MSG_TYPES as mt

debug = True
MAX_PROCESSES=5 # @todo configurable

# @todo define it here for now. move to common file


class MigrationWorker():
    '''
    Migration Worker core functionality implementation
    '''

#    def __init__(self, amq_broker=("localhost",5672), myaddr="mw", target="pe", auto_ack=True ):
    def __init__(self,name,conf):
        '''
        @type name: str
        @param name: name - migration worker name
        @type conf: dict
        @param conf: configuration dictionary (partial) received from Configuration Server or else
        
        conf["server"] - configuration of this server
        conf["amqp"] - configuration of amqp, such as qpid broker, else
        '''
        self.shutdown = False
        self.finished = False
        self.auto_ack = True  # auto ack incoming messages
        
        self.work_dict = {}
        
        self.log = logging.getLogger('log.encache.%s' % name)
        self.trace = logging.getLogger('trace.encache.%s' % name)

        self.trace.debug("extract configuration")
        try:
            self.name = name
            
            self.trace.debug("name=%s, conf=%s", name, conf)
            
            cfb = conf['amqp']['broker']
            cfs = conf['server']
            amq_broker = (cfb['host'],cfb['port'])
            queue_in   = cfs['queue_in']
            queue_work = cfs['queue_work']
            queue_reply  = cfs['queue_reply']
        except:
            self.trace.exception("got exception when extracting configuration form dictionary")
            # @todo - configuraion error, raise exception

        self.trace.debug("create clients")
        self.qpid_client = cmc.EnQpidClient(amq_broker, myaddr=queue_work, target=queue_reply)
        
        # start it here
        self.start()
        #XXX self.qpid_client.add_receiver("work",queue_work)
        
        ### self.pool = Pool(processes=MAX_PROCESSES) # pool of worker processes

    def _fetch_message(self):
        try:
            return self.qpid_client.rcv_default.fetch()
        except Queue.Empty:
            return None
        except qpid.messaging.MessagingError, e:
            self.log.error("fetch_message() exception %s", e)
            return None

    def _ack_message(self, msg):
        try:
            self.trace.debug("_ack_message(): sending acknowledge")
            self.qpid_client.ssn.acknowledge(msg)         
        except:
            exc, emsg = sys.exc_info()[:2]
            self.trace.debug("_ack_message(): Can not send auto acknowledge for the message. Exception e=%s msg=%s", str(exc), str(emsg))    
            pass

    def _send_reply(self,m):
        self.qpid_client.snd_default.send(m)

##############################################################################
# Message Processing logic
#

# Message handlers:
# - handler processes message m
# - handler "consumes" message, if it returns None - I'll ack the message to sender
#   if handler returns not None, it is handler's responsibility to ack message. AMQP broker will resent message if the message is not acked.
# - redelivered message have property redelivered = True  
# - sender supposed to set correlation_id unique for the message
#
# Normal course of action:
#    check args, start message processing, drop correlation_id on heap and return None
#
    def worker_purge(self,correlation_id=None):
        #@todo access to dictionary must be synchronized
        try:
            self.trace.debug("working on %s, correlation_id=%s",mt.MWC_PURGE,correlation_id)
            m=self.work_dict[correlation_id]
            time.sleep(2)
            # @todo - use original list, put something for now
            l = ["file1","file2"]
            self.trace.debug("WORKER m=%s",m)
            self.trace.debug("WORKER l=%s",l)
            reply = cache.messaging.mw_client.MWRPurged(orig_msg=m, content=l)
            self.trace.debug("WORKER reply=%s",reply)
            try:
                self._send_reply(reply)
                self.trace.debug("worker_purge() reply sent, reply=%s", reply)
            except Exception, e:
                self.trace.exception("worker_purge(), sending reply, exception")         
        except:
            self.trace.exception("worker %s, correlation_id=%s",mt.MWC_PURGE,correlation_id)
        finally:
            # @todo delete it in main, or do periodic cleanup 
            del self.work_dict[correlation_id]

    # work messages
    def handler_purge(self,m):
        self.trace.debug("process %s message %s",mt.MWC_PURGE,m)
        # @todo - use named tuple
        self.work_dict[m.correlation_id] = (m,"more info related to worker")
        # start processing
        kw={"correlation_id":m.correlation_id}
        t = threading.Thread(target=self.worker_purge,kwargs=kw)
        t.start()
        self.trace.debug("processing thread started %s,args=%s",t,kw)

    def handler_archive(self,m):
        self.trace.debug("process %s message %s",mt.MWC_ARCHIVE,m)

    def handler_stage(self,m):
        self.trace.debug("process %s message %s",mt.MWC_STAGE,m)

    # direct messages:
    def handler_status(self,m):
        self.trace.debug("process %s message %s",mt.MWC_STATUS,m)

    # map message type to processor
    
    handlers = {mt.MWC_PURGE : handler_purge,
                mt.MWC_ARCHIVE : handler_archive,
                mt.MWC_STAGE : handler_stage,
    }
    
    def handle_message(self,m):
        # @todo : check "type" present and is string or unicode
        cmd_type = m.properties["type"]
        
        try:
            h = self.handlers[cmd_type]
        except KeyError:
            raise cache.errors.EnCacheWrongCommand(cmd_type)
        
        self.trace.debug("handle message - type,handle=%s,%s", cmd_type,h)
        # can use these to exclude redelivered messages
        correlation_id = m.correlation_id
        redelivered = m.redelivered
        
        # @todo check if message is on heap for processing
        if redelivered :
            return None
        
        try:
            ret = h(self,m)
        except Exception,e:
            self.trace.exception("handle message - exception %s",e)
        return ret
        
    def serve_qpid(self):
        """
        read qpid messages from queue
        """
        self.qpid_client.start() 
        try:
            while not self.shutdown:
                # Fetch message from qpid queue
                message =  self._fetch_message()
                if not message:
                    continue
                self.trace.debug("got qpid message=%s", message)

                # debug HACK to use spout messages. @todo - check if original correlation ID is set; disable in production
                try:
                    message.correlation_id = message.properties["spout-id"]
                    self.trace.info("correlation_id is not set, setting it to spout-id %s", message.correlation_id ) 
                except:
                    pass
                #end DEBUG hack

                do_ack = False
                try:
                    do_ack = self.handle_message(message) is None
                    self.trace.debug("message processed correlation_id=%s, do_ack=%s", message.correlation_id, do_ack )   
                except Exception,e:
                    # @todo - print exception type cleanly
                    self.log.error("can not process message. Exception %s. Original message = %s",e,message)
             
                # Acknowledge ORIGINAL ticket thus we will not get it again
                if do_ack:
                    self._ack_message(message)

        # try / while 
        finally:
            self.qpid_client.stop()

    def start(self):
        # start server in separate thread 
        self.srv_thread = threading.Thread(target=self.serve_qpid) 
        self.srv_thread.start()                

    def stop(self):
        # tell serving thread to stop and wait until it finish    
        self.shutdown = True
        
        self.qpid_client.stop()
        self.srv_thread.join()
        
if __name__ == "__main__":   # pragma: no cover    

    # Test Unit
    import cache.en_logging.config_test_unit
    
    cache.en_logging.config_test_unit.set_logging_console()
    
    name = "mw_123"
    conf = {"amqp": {
                "broker" : {
                            "host":"dmsen06.fnal.gov",
                            "port":5672},
                    },
            "server":{
                      "queue_work" : "migrator",           # all workers get job from common Migration Worker queue
                       # queue name for messages sent directly to this worker, like MDW_STATUS
                       #    worker automatically create queue if it does not exist and deletes on exit
                      "queue_in" : "mw_123; {create: receiver, delete: receiver}",

#                      "queue_out" : "md",            # MW reply to Migration Dispatcher queue
                      "queue_reply" : "md_replies",            # MW reply to Migration Dispatcher queue
                      }
            }

    l_trace = logging.getLogger('trace.encache.%s' % name)
    l_trace.debug("start unit test")
    
    try:
        # instantiate MigrationWorker server
        mw = MigrationWorker(name,conf)
        # it starts in constructor
        #mw.start()
    except:
        l_trace.debug("Can't instantiate MigrationWorker, exiting")
        sys.exit(1)
        
    # stop mw server if there was keyboard interrupt
    while not mw.finished :
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            print "Keyboard interrupt at main thread"
            mw.stop()
            break
    
    del mw
    print "mw finished"
