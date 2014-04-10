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
#import cache.messaging.client2 as cmc
import cache.messaging.client as cmc

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

    def __str__(self):
        return "id %s list: %s"%(self.id, self.list_object)

class MigrationDispatcher():

    def __init__(self,
                 name,
                 broker,
                 queue_write,
                 queue_read,
                 queue_purge,
                 queue_reply,
                 lock,
                 migration_pool,
                 purge_pool,
                 clustered_configuration=None):
        '''
        @type name: str
        @param name - migration dispatcher name
        @param broker - qpid broker
        @param queue_write - queue for migration dispatcher write requests
        @param queue_read - queue for migration dispatcher read requests
        @param queue_purge - queue for migration dispatcher purge requests
        @param queue_reply - replies from migrators are sent to this queue
        @param migration_pool - pool where PE Server part puts lists to get served by Migration Dispatcher
        @param purge_pool - purge pool where PE Server part puts lists to get served by Migration Dispatcher
        @param clustered_configuration - indicates that clustered configuration is used or not
        '''
        self.shutdown = False
        self.finished = False
        self.auto_ack = True  # auto ack incoming messages
        self.lock = lock
        self.migration_pool = migration_pool
        self.purge_pool = purge_pool

        self.log = logging.getLogger('log.encache.%s' % name)
        self.trace = logging.getLogger('trace.encache.%s' % name)

        self.name = name
        self.trace.debug("name=%s", name)

        self.reply_to = None
        self.stop_status_loop = None
        self.expected_status_reply = None
        self.clustered_configuration = clustered_configuration # see dispatcher.__init__()

        amq_broker = (broker['host'], broker['port'])
        self.trace.debug("create clients")
        self.queue_write = queue_write
        self.queue_read = queue_read
        self.queue_purge = queue_purge
        # Each type of request has its own queue
        if self.clustered_configuration:
            # And its own exchange for clustered configuration.
            q_wr = "%s; {create:always,node:{type:topic,x-declare:{type:direct}}}"%(queue_write,)
            q_rd = "%s; {create:always,node:{type:topic,x-declare:{type:direct}}}"%(queue_read,)
            q_p = "%s; {create:always,node:{type:topic,x-declare:{type:direct}}}"%(queue_purge,)
        else:
            q_wr = "%s; {create: always}"%(queue_write,)
            q_rd = "%s; {create: always}"%(queue_read,)
            q_p = "%s; {create: always}"%(queue_purge,)
        q_r = "%s; {create:always}"%(queue_reply,)
        self.qpid_client_write = cmc.EnQpidClient(amq_broker, myaddr=q_r, target=q_wr)
        self.qpid_client_read = cmc.EnQpidClient(amq_broker, myaddr=None, target=q_rd)
        self.qpid_client_purge = cmc.EnQpidClient(amq_broker, myaddr=None, target=q_p)
        self.qpid_client_write.start()
        self.qpid_client_read.start()
        self.qpid_client_purge.start()

        self.qpid_client = cmc.EnQpidClient(amq_broker, myaddr=q_r, target=q_wr)

        # start it here
        self.start()

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

    def _create_sender(self, client, queue, routing_key):
        # Routing key is valuable only for clustered configuration
        # and is defined by disk library manager.
        # The routing key is not enough for separate queues based on the type of request.
        # Each request type has its own message queue defined by request type and disk library:
        #
        #
        #                           |--- read_LIB1
        #   exchange for cluster 1: |--- write_LIB1
        #                           |--- purge_LIB1
        #
        #                           |--- read_LIB2
        #   exchange for cluster 2: |--- write_LIB2
        #                           |--- purge_LIB2
        #

        key = "_".join((queue, routing_key))
        Trace.trace(10, "_create_sender for %s %s"%(queue, key))
        if self.clustered_configuration:
            snd = client.ssn.sender("%s/%s; {create:always,node:{type: topic, x-declare:{type:direct}}}"%(queue, key))
        else:
            snd = client.ssn.sender("%s; {create:always}"%(queue,))

        return snd

    def send_status_request(self):
        Trace.trace(10, "Starting send_status_request")
        status_cmd = mw_client.MWCStatus(request_id=0)

        while 1:
            for pool in (self.migration_pool, self.purge_pool):

                if self.stop_status_loop:
                    break
                for key in pool.keys():
                    try:
                        if pool[key].status_requestor:
                            # If status requestor exist
                            # dispatcher had already received confirmation from
                            # migrator.
                            # Send status request to it
                            status_cmd = mw_client.MWCStatus(request_id=0, correlation_id=key)
                            Trace.trace(10, "send_status_request sending %s"%(status_cmd,))
                            pool[key].status_requestor.send(status_cmd)
                    except KeyError:
                        Trace.handle_error()
                        Trace.log(e_errors.INFO,
                                  "error in send_status_request KeyError %s keys %s"%
                                  (key, pool.keys()))
            time.sleep(600)


    def handle_message(self,m):
        # @todo : check "type" present and is string or unicode
        msg_type = m.properties["en_type"]

        self.trace.debug("handle_message %s type %s", m, msg_type)
        # can use these to exclude redelivered messages
        correlation_id = m.correlation_id
        redelivered = m.redelivered
        Trace.trace(10, "handle_message:redelivered %s"%(redelivered,))

        # @todo check if message is on heap for processing
        if redelivered :
            # this may require additional processing
            pass
        if m.correlation_id in self.migration_pool:
            Trace.trace(10, "handle_message:in migration pool %s"%(self.migration_pool[m.correlation_id],))
            pool = self.migration_pool
            if self.migration_pool[m.correlation_id].list_object.list_type == mt.CACHE_MISSED:
                send_queue = self.queue_read
                client = self.qpid_client_read
            else:
                send_queue = self.queue_write
                client = self.qpid_client_write
        elif m.correlation_id in self.purge_pool:
            pool = self.purge_pool
            send_queue = self.queue_purge
            client = self.qpid_client_purge
        else:
            # Dispatcher must have been restarted and does not have
            # a record with this correlation id.
            return
        Trace.trace(10, "handle_message:in pool %s"%(pool[m.correlation_id],))

        if msg_type == mt.MWR_CONFIRMATION:
            # Received the confirmation from migrator
            # create sender on the queue declared in m.reply_to.
            # Correlation id is propagated for all mesaages related to the same request
            # first find which pool is the confirnmation for

            if not pool[m.correlation_id].status_requestor:
               self.lock.acquire()
               try:
                   pool[m.correlation_id].status_requestor = self.qpid_client.add_sender("mw_qpid_interface", m.reply_to)
               except:
                   pass # for now
               self.lock.release()

        if msg_type in (mt.MWR_ARCHIVED, mt.MWR_PURGED, mt.MWR_STAGED):
            if pool.has_key(m.correlation_id):
                self.lock.acquire()
                try:
                    del(pool[m.correlation_id])
                except:
                    pass
                self.lock.release()
            Trace.trace(10, "handle_message:After %s"%(pool,))

        if msg_type == mt.MWR_STATUS:
            # This message type can be received only as a result
            # status request sent from this migration dispatcher,
            # which means that the addressed migrator is doing work
            # for migration dispatcher

            Trace.trace(10, "handle_message: MWR_STATUS recieved %s %s"%(m.content, m.correlation_id))
            Trace.trace(10, "handle_message: MPOOL %s"%(pool,))
            if  m.content['migrator_status'] == None or m.content['migrator_status'] == mt.FAILED:
                # Migrator restarted, but before restart it was doing
                # some work which could have failed.
                # in this case the request needs to be re-sent
                if pool.has_key(m.correlation_id):
                    Trace.trace(10, "handle_message: Migrator replied with status %s"%
                                (m.content['migrator_status'],))
                    if m.content['migrator_status'] == mt.FAILED:
                        if m.content.has_key("detail") and m.content['detail'] == "REBUILD PACKAGE":
                            # migrator failed to write package
                            # delete corresponding migration_pool record
                            # so that it can be rebuilt later
                            del(pool[m.correlation_id])
                            Trace.trace(10, "deleted from migration pool to get rebuilt")

                    if pool.has_key(m.correlation_id):
                        Trace.trace(10, "handle_message: reposting request")
                        pool[m.correlation_id].state = file_list.FILLED
                        disk_library = pool[m.correlation_id].list_object.disk_library
                        #sender = self._create_sender(self.queue_work, disk_library)
                        sender = self._create_sender(client, send_queue, disk_library)


                        self.migrate_list(pool[m.correlation_id], sender)
            else:
                if pool.has_key(correlation_id):
                    if m.content['migrator_status'] == pool[m.correlation_id].expected_reply:
                        # expected reply is announced in the corresponding
                        # migration pool entry
                        # keyed by the correlation id
                        # all messages for a given action have the same correlation id.
                        Trace.trace(10, "handle_message: received expected status reply")
                        del(pool[m.correlation_id])

                    else:
                        Trace.trace(10, "handle_message: received %s expected %s"%
                                    (m.content['migrator_status'],
                                     self.pool[m.correlation_id].expected_reply))
        Trace.trace(10, "handle_message:returning %s"%(True,))

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
                    Trace.handle_error()
                    Trace.trace(10, "serve_qpid exception processing handle_message %s"%
                                (e,))
                    self.log.error("can not process message. Exception %s. Original message = %s",e,message)
                    do_ack = True # to not hold message in the queue

                # Acknowledge ORIGINAL ticket thus we will not get it again
                Trace.trace(10, "serve_qpid doack %s"%(do_ack,))
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


    def migrate_list(self, migration_list, send_client):
        if migration_list.state != file_list.ACTIVE:
            # send list to migration dispatcher queue
            if migration_list.list_object.list_type == mt.CACHE_WRITTEN:
               command = mw_client.MWCArchive(migration_list.list_object.file_list,
                                              correlation_id = migration_list.id)
               migration_list.expected_reply =  mt.ARCHIVED
            elif migration_list.list_object.list_type == mt.CACHE_MISSED:
               command = mw_client.MWCStage(migration_list.list_object.file_list, correlation_id = migration_list.id)
               migration_list.expected_reply = mt.CACHED
            elif migration_list.list_object.list_type == mt.MDC_PURGE:
               command = mw_client.MWCPurge(migration_list.list_object.file_list, correlation_id = migration_list.id)
               migration_list.expected_reply = mt.PURGED
            try:
                # send command to the queue defined in send_client
                # and set corresponding list state to ACTIVE
                # for monitoring of execution
                send_client.send(command)

                migration_list.state = file_list.ACTIVE
            except:
                Trace.handle_error()

    def start_migration(self, pool, key):
        try:
            item = pool[key]
        except KeyError, detail:
            Trace.log(e_errors.ERROR, "Error adding to list. No such key %s"%(key,))
            return
        if item.list_object.list_type == mt.CACHE_MISSED:
            send_queue = self.queue_read
            client = self.qpid_client_read
        elif item.list_object.list_type == mt.MDC_PURGE:
            send_queue = self.queue_purge
            client = self.qpid_client_purge
        elif item.list_object.list_type == mt.CACHE_WRITTEN:
            send_queue = self.queue_write
            client = self.qpid_client_write
        else:
            Trace.alarm(e_errors.WARNING, "Unknown message type %s"%(item,))
            return

        sender = self._create_sender(client, send_queue, item.list_object.disk_library)
        self.migrate_list(item, sender)



