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


class MigrationClient():
    '''
    Migration Client for unit test core functionality implementation
    '''

    def __init__(self, name):
        '''
        @type name: str
        @param name: name - migration worker name
        @type conf: dict
        '''
        self.shutdown = False
        self.finished = False
        self.auto_ack = True  # auto ack incoming messages
        
        self.work_dict = {}
        
        self.log = logging.getLogger('log.encache.%s' % name)
        self.trace = logging.getLogger('trace.encache.%s' % name)

        self.trace.debug("extract configuration")
        intf = configuration_client.ConfigurationClientInterface(user_mode=0)
        csc = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))
        migrator_conf = csc.get(name)
        md = csc.get(migrator_conf['migration_dispatcher'])
        md_conf = csc.get(md)
        
        self.name = name
        self.trace.debug("name=%s", name)
        cfb = csc.get('amqp_broker')
        amq_broker = (cfb['host'],cfb['port'])
        #queue_in   = cfs['queue_in']
        queue_work = md['queue_work']
        queue_reply  = md['queue_reply']
        self.reply_to = None
        self.stop_status_loop = None
        self.expected_status_reply = None

        self.trace.debug("create clients")
        self.qpid_client = cmc.EnQpidClient(amq_broker, myaddr=queue_reply, target=queue_work)

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

    def _send_message(self,m):
        self.qpid_client.snd_default.send(m)

    def send_status_request(self):
        print "Starting send_status_request"
        status_cmd = cache.messaging.mw_client.MWCStatus(request_id=0)
        
        while 1:
            if self.stop_status_loop:
                self.stop_status_loop = None
                self.reply_to = None
                self.expected_status_reply = None
                break
            time.sleep(2)
            self.md_sender.send(status_cmd)
        

    def handle_message(self,m):
        # @todo : check "type" present and is string or unicode
        msg_type = m.properties["en_type"]
        
        self.trace.debug("handle message %s type %s", m, msg_type)
        # can use these to exclude redelivered messages
        correlation_id = m.correlation_id
        redelivered = m.redelivered
        
        # @todo check if message is on heap for processing
        if redelivered :
            return None
        if msg_type == mt.MWR_CONFIRMATION:
            print "mt.MWRConfirmation"
            print "MESSAGE", m
            if not self.reply_to:
                self.reply_to = m.reply_to
                self.md_sender = self.qpid_client.add_sender("mw_qpid_interface", self.reply_to)
                self.status_thread = threading.Thread(target=self.send_status_request,  name="Status Command") 
                self.status_thread.start()
        if msg_type == mt.MWR_STATUS:
            #print "STATUS", m.content['migrator_status']
            print "STATUS", m.content
            if  m.content['migrator_status'] == self.expected_status_reply:
                print "STOPPING LOOP"
                self.stop_status_loop = True
                
                
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
                    message.correlation_id = message.properties["spout-id"]
                    self.trace.info("correlation_id is not set, setting it to spout-id %s", message.correlation_id ) 
                except:
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
        self.srv_thread = threading.Thread(target=self.serve_qpid) 
        self.srv_thread.start()                

    def stop(self):
        # tell serving thread to stop and wait until it finish    
        self.shutdown = True
        
        self.qpid_client.stop()
        self.srv_thread.join()

if __name__ == "__main__":   # pragma: no cover
    migrator = sys.argv[1]
    
    import cache.en_logging.config_test_unit
    
    cache.en_logging.config_test_unit.set_logging_console()
    
    l =  file_list.FileList()
    i = file_list.FileListItem(bfid = "GCMS130824090400000",
                               nsid = "000100000000000000001A18",
                               path = "/pnfs/fs/usr/data/moibenko/d2/LTO3/library_manager.py",
                               libraries= ["LTO3"])
    l.append(i)
    i = file_list.FileListItem(bfid = "GCMS130824093000000",
                               nsid = "000100000000000000001A48",
                               path = "/pnfs/fs/usr/data/moibenko/d2/LTO3/library_manager_client.py",
                               libraries= ["LTO3"])
    l.append(i)
    i = file_list.FileListItem(bfid = "GCMS130824094700000",
                               nsid = "000100000000000000001A78",
                               path = "/pnfs/fs/usr/data/moibenko/d2/LTO3/media_changer.py",
                               libraries= ["LTO3"])
    l.append(i)
    i = file_list.FileListItem(bfid = "GCMS130824096300000",
                               nsid = "000100000000000000001AA8",
                               path = "/pnfs/fs/usr/data/moibenko/d2/LTO3/media_changer_client.py",
                               libraries= ["LTO3"])
    l.append(i)
    i = file_list.FileListItem(bfid = "GCMS130824100100000",
                               nsid = "000100000000000000001AD8",
                               path = "/pnfs/fs/usr/data/moibenko/d2/LTO3/mover.py",
                               libraries= ["LTO3"])
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
