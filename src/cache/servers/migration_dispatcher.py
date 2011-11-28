#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

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
import configuration_client
import Trace

# enstore cache imports
import file_cache_status
import cache.errors
#from cache.messaging.client import EnQpidClient
import cache.messaging.client2 as cmc
import cache.messaging.mw_client
import cache.messaging.pe_client
import cache.messaging.file_list as file_list
import cache.messaging.mw_client as mw_client

from cache.messaging.messages import MSG_TYPES as mt

debug = True
MAX_PROCESSES=5 # @todo configurable

# @todo define it here for now. move to common file

class MigrationList:
    def __init__(self, f_list=None, id = None, state = None, expected_reply = None):
        self.list_object = f_list # file list according to file_list.py class description
        self.status_requestor = None
        self.id = id # MigrationList id
        self.state = state # state of the file list from file_list.py
        self.expected_reply = expected_reply # reply expected for this file list
        
class MigrationDispatcher():

    def __init__(self, name, broker, queue_work, queue_reply, lock, migration_pool):
        '''
        @type name: str
        @param name: name - migration worker name
        '''
        self.shutdown = False
        self.finished = False
        self.auto_ack = True  # auto ack incoming messages
        self.lock = lock
        self.migration_pool = migration_pool
        
        self.log = logging.getLogger('log.encache.%s' % name)
        self.trace = logging.getLogger('trace.encache.%s' % name)

        self.name = name
        self.trace.debug("name=%s", name)

        self.reply_to = None
        self.stop_status_loop = None
        self.expected_status_reply = None

        amq_broker = (broker['host'], broker['port'])
        self.trace.debug("create clients")
        q_w = "%s; {create: always}"%(queue_work,)
        q_r = "%s; {create: always}"%(queue_reply,)
        self.qpid_client = cmc.EnQpidClient(amq_broker, myaddr=q_r, target=q_w)

        # start it here
        self.start()

    def _fetch_message(self):
       print "MD: FETCH"
       try:
          return self.qpid_client.rcv_default.fetch()
       except Queue.Empty:
          return None
       except qpid.messaging.MessagingError, e:
          self.log.error("fetch_message() exception %s", e)
          return None

    def _ack_message(self, msg):
       print "MD: ACK"
       try:
          self.trace.debug("_ack_message(): sending acknowledge")
          self.qpid_client.ssn.acknowledge(msg)         
       except:
          exc, emsg = sys.exc_info()[:2]
          self.trace.debug("_ack_message(): Can not send auto acknowledge for the message. Exception e=%s msg=%s", str(exc), str(emsg))    
          pass

    def _send_message(self,m):
        self.qpid_client.snd_default.send(m)

    def send_status_request(self):
        Trace.trace(10, "Starting send_status_request")
        status_cmd = cache.messaging.mw_client.MWCStatus(request_id=0)
        
        while 1:
            if self.stop_status_loop:
                break
            for key in self.migration_pool.keys():
                if self.migration_pool[key].status_requestor:
                    # If status requestor exist
                    # dispatcher had already received confirmation from
                    # migrator.
                    # Send status request to it
                    status_cmd = cache.messaging.mw_client.MWCStatus(request_id=0, correlation_id=key)
                    Trace.trace(10, "send_status_request sending %s"%(status_cmd,))
                    self.migration_pool[key].status_requestor.send(status_cmd)
            time.sleep(10)


    def handle_message(self,m):
        # @todo : check "type" present and is string or unicode
        msg_type = m.properties["en_type"]
        
        self.trace.debug("handle message %s type %s", m, msg_type)
        # can use these to exclude redelivered messages
        correlation_id = m.correlation_id
        redelivered = m.redelivered
        
        # @todo check if message is on heap for processing
        if redelivered :
            #return None
            pass
        if msg_type == mt.MWR_CONFIRMATION:
            # Received the confirmation form migrator
            # create sender on the queue declared in m.reply_to.
            # Correlation id is proagated for all mesaages related to the same request
            if not self.migration_pool[m.correlation_id].status_requestor:
                self.migration_pool[m.correlation_id].status_requestor = self.qpid_client.add_sender("mw_qpid_interface", m.reply_to)
        if msg_type == mt.MWR_ARCHIVED:
            Trace.trace(10, "handle_message:MWR_ARCHIVED. Before %s"%(self.migration_pool,))
            if self.migration_pool.has_key(m.correlation_id):
               del(self.migration_pool[m.correlation_id])
            Trace.trace(10, "handle_message:MWR_ARCHIVED. After %s"%(self.migration_pool,)) 
        if msg_type == mt.MWR_PURGED:
            Trace.trace(10, "handle_message:MWR_PURGED. Before %s"%(self.migration_pool,)) 
            del(self.migration_pool[m.correlation_id])
            Trace.trace(10, "handle_message:MWR_PURGED. After %s"%(self.migration_pool,)) 

        if msg_type == mt.MWR_STAGED:
            Trace.trace(10, "handle_message:MWR_STAGED. Before %s"%(self.migration_pool,)) 
            del(self.migration_pool[m.correlation_id])
            Trace.trace(10, "handle_message:MWR_STAGED. After %s"%(self.migration_pool,)) 
            
        if msg_type == mt.MWR_STATUS:
            # This message type can be received only as a result
            # status request sent from this migration dispatcher,
            # which means that the addressed migrator is doing work
            # for migration dispatcher
            
            Trace.trace(10, "handle_message: MWR_STATUS recieved %s %s"%(m.content, m.correlation_id))
            Trace.trace(10, "handle_message: MPOOL %s"%(self.migration_pool,))
            if  m.content['migrator_status'] == None:
                # Migrator restarted, but before restart it was doing
                # some work which could have failed.
                # in this case the request needs to be re-sent
                Trace.trace(10, "handle_message: Migrator replied with status None")
                Trace.trace(10, "handle_message: reposting request")
                self.migrate_list(self.migration_pool[m.correlation_id])
            else:
                if  m.content['migrator_status'] == self.migration_pool[m.correlation_id].expected_reply:
                    # expected reply is announced in the corresponding
                    # migration pool entry
                    # keyed by the correlation id
                    # all messages for a given action have the same correlation id.
                    Trace.trace(10, "handle_message: received expected status reply")
                else:
                    Trace.trace(10, "handle_message: received %s expected %s"%(m.content['migrator_status'],
                                                                               self.migration_pool[m.correlation_id].expected_reply))
                
        return True
        
    def serve_qpid(self):
        """
        read qpid messages from queue
        """
        self.qpid_client.start()
        print "QPID client started", self.shutdown
        try:
            while not self.shutdown:
                # Fetch message from qpid queue
                self.trace.debug("fetch message")
                message =  self._fetch_message()
                self.trace.debug("got qpid message1=%s", message)
                if not message:
                    continue
                self.trace.debug("got qpid message=%s", message)

                # debug HACK to use spout messages
                try:
                   self.trace.debug("got qpid message=%s", message)
                   if message.properties.has_key("spout-id"):
                       message.correlation_id = message.properties["spout-id"]
                       self.trace.info("correlation_id is not set, setting it to spout-id %s", message.correlation_id ) 
                except:
                   self.trace.info("exception setting it to spout-id %s", message.correlation_id ) 
                   pass
                #end DEBUG hack

                do_ack = False
                try:
                    do_ack = self.handle_message(message)
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
        self.qpid_srv_thread = threading.Thread(target=self.serve_qpid) 
        self.qpid_srv_thread.start()
        self.status_thread = threading.Thread(target=self.send_status_request, name="Status") 
        self.status_thread.start()
        
    def stop(self):
        # tell serving thread to stop and wait until it finish    
        self.shutdown = True
        
        self.qpid_client.stop()
        self.srv_thread.join()
        self.status_thread.join()


    def migrate_list(self, migration_list):
        if migration_list.state != file_list.ACTIVE:
            # send list to migration dispatcher queue
            if migration_list.list_object.list_type == mt.CACHE_WRITTEN:
               command = mw_client.MWCArchive(migration_list.list_object.file_list, correlation_id = migration_list.id)
               migration_list.expected_reply =  file_cache_status.ArchiveStatus.ARCHIVED
            elif migration_list.list_object.list_type == mt.CACHE_MISSED:
               command = mw_client.MWCStage(migration_list.list_object.file_list, correlation_id = migration_list.id)
               migration_list.expected_reply = file_cache_status.CacheStatus.CACHED 
            elif migration_list.list_object.list_type == mt.MDC_PURGE:
               command = mw_client.MWCPurge(migration_list.list_object.file_list, correlation_id = migration_list.id)
               migration_list.expected_reply = file_cache_status.CacheStatus.PURGED
            try:
                # send command to migrator queue
                # and set corresponding list state to ACTIVE
                # for monitoring of execution 
                self._send_message(command)
                migration_list.state = file_list.ACTIVE
            except:
                Trace.handle_error()
        
    def start_migration(self):
        print "STARTED MIGR_DISP"
        print "MIGR_DISP"
        for key in self.migration_pool.keys():
            print "KEY IN MIG", key, self.migration_pool[key]
            item = self.migration_pool[key]
            self.migrate_list(item)

if __name__ == "__main__":
    migrator = sys.argv[1]
    
    import cache.en_logging.config_test_unit
    
    cache.en_logging.config_test_unit.set_logging_console()
    
    l =  file_list.FileListWithCRC()
    i = file_list.FileListItemWithCRC(bfid = "GCMS130824090400000",
                                      nsid = "000100000000000000001A18",
                                      path = "/pnfs/fs/usr/data/moibenko/d2/LTO3/library_manager.py",
                                      libraries= ["LTO3"],
                                      crc = 1234L) # just put something here
    l.append(i)
    i = file_list.FileListItemWithCRC(bfid = "GCMS130824093000000",
                                      nsid = "000100000000000000001A48",
                                      path = "/pnfs/fs/usr/data/moibenko/d2/LTO3/library_manager_client.py",
                                      libraries= ["LTO3"],
                                      crc = 34L) # just put something here
                                      
    l.append(i)
    i = file_list.FileListItemWithCRC(bfid = "GCMS130824094700000",
                                      nsid = "000100000000000000001A78",
                                      path = "/pnfs/fs/usr/data/moibenko/d2/LTO3/media_changer.py",
                                      libraries= ["LTO3"],
                                      crc = 0L) # just put something here
                                      
    l.append(i)
    i = file_list.FileListItemWithCRC(bfid = "GCMS130824096300000",
                                      nsid = "000100000000000000001AA8",
                                      path = "/pnfs/fs/usr/data/moibenko/d2/LTO3/media_changer_client.py",
                                      libraries= ["LTO3"],
                                      crc = 4L) # just put something here
                                      
    l.append(i)
    i = file_list.FileListItemWithCRC(bfid = "GCMS130824100100000",
                                      nsid = "000100000000000000001AD8",
                                      path = "/pnfs/fs/usr/data/moibenko/d2/LTO3/mover.py",
                                      libraries= ["LTO3"],
                                      crc = 3L) # just put something here
                                      
    l.append(i)

    mc = MigrationClient(migrator)
    
    while 1:
        try:
            rc = raw_input("1 - archive, 2 - purge, 3 - stage, 0 - exit: ")
            if rc:
                rc = int(rc)
                if rc == 1:
                    arch_command = mw_client.MWCArchive(l.file_list)
                    #mc = MigrationClient(migrator)
                    print "ARCHIVING"
                    mc.expected_status_reply = file_cache_status.ArchiveStatus.ARCHIVED
                    mc._send_message(arch_command)
                elif rc == 2:
                    purge_command = mw_client.MWCPurge(l.file_list)
                    #mc = MigrationClient(migrator)
                    print "PURGING"
                    mc.expected_status_reply = file_cache_status.CacheStatus.PURGED
                    mc._send_message(purge_command)
                elif rc == 3:
                    stage_command = mw_client.MWCStage(l.file_list)
                    #mc = MigrationClient(migrator)
                    print "STAGING"
                    mc.expected_status_reply = file_cache_status.CacheStatus.CACHED
                    mc._send_message(stage_command)
                else:
                    mc.stop()
                    break
        except KeyboardInterrupt:
            print "Keyboard interrupt at main thread"
            mc.stop()
            break
    
    del mc
    print "mc finished"
