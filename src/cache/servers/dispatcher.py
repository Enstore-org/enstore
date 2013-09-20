#!/usr/bin/env python

##############################################################################
#
# $Id$
#
##############################################################################

# system imports
import sys
import threading
import time
import types

# enstore imports
import enstore_constants
import e_errors
import configuration_client
import event_relay_client
import file_clerk_client
import event_relay_messages
import generic_server
import dispatching_worker
import monitored_server
import option
import Trace
import en_eval
import purge_files
import file_cache_status
import set_cache_status
import lmd_policy_selector
import dict_u2a
import migration_dispatcher
import mw
import cache.messaging.mw_client
from cache.messaging.messages import MSG_TYPES as mt
import cache.messaging.file_list as file_list
import cache.en_logging.en_logging

MY_NAME = enstore_constants.DISPATCHER

QPID_BROKER_NAME = "amqp_broker" # this must be in enstore_constants

class Dispatcher(mw.MigrationWorker,
                 dispatching_worker.DispatchingWorker,
                 generic_server.GenericServer):

   def __init__(self, csc, auto_ack=True):
        '''
        Constructor
        '''
        generic_server.GenericServer.__init__(self, csc, MY_NAME,
                                              function = self.handle_er_msg)
        self._lock = threading.Lock()
        self.my_conf = self.csc.get(self.name)
        self.shutdown = False
        self.finished = False
        Trace.init(self.log_name, "yes")

        # get all necessary information from configuration
        # configuration dictionary required by MigrationWorker
        self.dispatcher_configuration = {}
        self.dispatcher_configuration['server'] = {}
        self.dispatcher_configuration['server'] = self.my_conf
        self.queue_in_name = self.name.split('.')[0]
        self.dispatcher_configuration['server']['queue_in'] = "%s; {create: receiver, delete: receiver}"% \
                                                              (self.queue_in_name,) # dispatcher input control queue
        # queue_work is Policy Engine Server incoming queue (events from file_clerk for instance)
        # queue_reply is Policy Engine Server outcoming queue (to file_clerk for instance, but I do not know why it is needed
        # create these queues if they do not exist
        self.dispatcher_configuration['server']['queue_work'] ="%s; {create: always}"%\
                                                                (self.dispatcher_configuration['server']['queue_work'],)
        self.dispatcher_configuration['server']['queue_reply'] = "%s; {create: always}"%\
                                                       (self.dispatcher_configuration['server']['queue_reply'],)


        fc_conf = self.csc.get("file_clerk")
        self.fcc = file_clerk_client.FileClient(self.csc, bfid=0,
                                                server_address=(fc_conf['host'],
                                                                fc_conf['port']))

        self.policy_file = self.my_conf['policy_file']
        try:
            self.policy_selector = lmd_policy_selector.Selector(self.policy_file)
        except Exception, detail:
            Trace.log(e_errors.ALARM, "Can not create policy selector: %s" %(detail,))
            sys.exit(-1)
        
        # get amqp broker configuration - common for all servers
        self.dispatcher_configuration['amqp'] = {}
        self.dispatcher_configuration['amqp']['broker'] = self.csc.get("amqp_broker")

        # Clustered configuration assumes using
        # clusters of disk movers and migrators grouped around one disk library.
        # Cluster uses a dedicated cache system.
        # If no clustered configuration is defined
        # All disk movers and migrators use the same cache.
        
        self.clustered_configuration = self.my_conf.get("clustered_configuration")
        self.max_time_in_cache = self.my_conf.get("max_time_in_cache", 3600)
        self.check_watermarks = True # this allows to purge files ignoring watermarks
        self.purge_watermarks = self.my_conf.get("purge_watermarks", None)
        # purge_watermarks is a tuple
        # (start_purging_disk_avalable, start_purging_disk_avalable)
        # start_purging_disk_avalable - available space as a fraction of the capacity
        self.file_purger = purge_files.FilePurger(self.csc, self.max_time_in_cache, self.purge_watermarks)
        
        # create pools for lists
        self.file_deleted_pool = {}
        self.cache_missed_pool = {}
        self.cache_purge_pool = {}
        self.cache_written_pool = {}
        self.cache_staged_pool = {}

        # create migration pool
        self.migration_pool = {}

        # create purge work pool
        self.purge_pool = {}

        # create Migration Dispatcher
        self.md = migration_dispatcher.MigrationDispatcher("MD",
                                                           self.dispatcher_configuration['amqp']['broker'],
                                                           self.my_conf['migrator_work'],
                                                           self.my_conf['migrator_reply'],
                                                           self._lock,
                                                           self.migration_pool,
                                                           self.purge_pool,
                                                           self.clustered_configuration)
        # set event handlers
        self.handlers = {mt.FILE_DELETED:  self.handle_file_deleted,
                         mt.CACHE_MISSED:  self.handle_cache_missed,
                         mt.CACHE_PURGED:  self.handle_cache_purged,
                         mt.CACHE_WRITTEN: self.handle_cache_written,
                         mt.CACHE_STAGED:  self.handle_cache_staged,
                         }

        mw.MigrationWorker.__init__(self, self.name, self.dispatcher_configuration)



        self.alive_interval = monitored_server.get_alive_interval(self.csc,
                                                                  MY_NAME,
                                                                  self.my_conf)

        dispatching_worker.DispatchingWorker.__init__(self, (self.my_conf['hostip'],
	                                              self.my_conf['port']))
        self.logger, self.tracer = cache.en_logging.en_logging.set_logging(self.log_name)
        self.resubscribe_rate = 300

	self.erc = event_relay_client.EventRelayClient(self)
	self.erc.start_heartbeat(self.name,  self.alive_interval)
   
   ##############################################
   #### Configuration related methods
   ##############################################
   # reload policy when this method is called
   # by the request from the client
   def reload_policy(self, ticket):
      try:
         self.policy_selector.read_config()
         ticket['status'] = (e_errors.OK, None)
      except Exception, detail:
         ticket['status'] = (e_errors.ERROR, "Error loading policy for PE Server: %s"%(detail,))
         Trace.log(e_errors.ERROR, "reload_policy: %s" % (detail,))
      self.reply_to_caller(ticket)
         

   # send current policy to client
   def show_policy(self, ticket):
      try:
         ticket['dump'] = self.policy_selector.policydict
         ticket['status'] = (e_errors.OK, None)
         self.send_reply_with_long_answer(ticket)
      except Exception, detail:
         ticket['status'] = (e_errors.ERROR, "Error %s"%(detail,))
         Trace.log(e_errors.ERROR, "show_policy: %s" % (detail,))
         self.reply_to_caller(ticket)

   # send content of pools
   def show_queue(self, ticket):
      Trace.trace(10, "show_queue")
      ml = {}
      for key in self.migration_pool:
         ml[key] = {'id':self.migration_pool[key].id,
                    'list': self.migration_pool[key].list_object.file_list,
                    'policy': self.migration_pool[key].list_object.list_name,
                    'type': self.migration_pool[key].list_object.list_type,
                    'time_qd': time.ctime(self.migration_pool[key].list_object.creation_time),
                    }
      pl = {}
      for key in self.purge_pool:
         pl[key] = {'id':self.purge_pool[key].id,
                    'list': self.purge_pool[key].list_object.file_list,
                    'type': self.purge_pool[key].list_object.list_type,
                    'time_qd': time.ctime(self.purge_pool[key].list_object.creation_time),
                    }
      ticket['pools'] = {'cache_missed':self.cache_missed_pool,
                         'cache_purge': self.cache_purge_pool,
                         'cache_written': self.cache_written_pool,
                         'migration_pool': ml,
                         'purge_pool': pl,
                         }
      ticket['status'] = (e_errors.OK, None)
      Trace.trace(10, "show_queue: ticket %s"%(ticket,))
      try:
         self.send_reply_with_long_answer(ticket)
      except Exception, detail:
         ticket = {'status': (e_errors.ERROR, "Error %s"%(detail,))}
         Trace.log(e_errors.ERROR, "show_queue: %s" % (detail,))
         self.reply_to_caller(ticket)

   # send migration pool entry
   def show_id(self, ticket):
      if not self.migration_pool.has_key(ticket['id']):
         ticket['status'] = (e_errors.KEYERROR, "No such id %s"%(ticket['id'],))
         self.reply_to_caller(ticket)
         return
      ticket['status'] = (e_errors.OK, None)
      ticket['id_info'] = self.migration_pool[ticket['id']].list_object.file_list
      Trace.trace(10, "show_id: ticket %s"%(ticket,))
      try:
         self.send_reply_with_long_answer(ticket)
      except Exception, detail:
         ticket['status'] = (e_errors.ERROR, "Error %s"%(detail,))
         Trace.log(e_errors.ERROR, "show_id: %s" % (detail,))
         self.reply_to_caller(ticket)
      
   # delete migration pool entry
   def delete_list(self, ticket):
      if not self.migration_pool.has_key(ticket['id']):
         ticket['status'] = (e_errors.KEYERROR, "No such id %s"%(ticket['id'],))
         self.reply_to_caller(ticket)
         return
      ticket['status'] = (e_errors.OK, None)
      self._lock.acquire()
      try:
         del(self.migration_pool[ticket['id']])
      
      except Exception, detail:
         ticket['status'] = (e_errors.ERROR, "Error %s"%(detail,))
      self._lock.release()
         
      self.reply_to_caller(ticket)

   # flush all pending writes to migrator queue
   def flush(self, ticket):
      items = self.cache_written_pool.keys()
      ticket['status'] = (e_errors.OK, None)
      if items:
         try:
            for item in items:
               self.cache_written_pool[item].full = True
               self.move_to_migration_pool(self.cache_written_pool, item)
            self.md.start_migration()
         except Exception, detail:
            ticket['status'] = (e_errors.ERROR, "%s"%(detail,))
         ticket['draining'] = items
      else:
         ticket['status'] = (e_errors.OK, "Nothing to drain")
         
      self.reply_to_caller(ticket)
             
   # move list from a list pool to migration pool
   # @param - src - pool
   # @param - item_to_move item to move is a key in the pool
   def move_to_migration_pool(self, src, item_to_move):
      # src[item_to_move] is a file_list.FIleList or
      # file_list.FileListWithCRC
      src[item_to_move].creation_time = time.time()
      list_id = src[item_to_move].list_id
      Trace.trace(10, "move_to_migration_pool list_id %s"%(list_id,))
      self._lock.acquire()
      try:
         self.migration_pool[list_id] = migration_dispatcher.MigrationList(src[item_to_move],
                                                                           list_id,
                                                                           file_list.FILLED)
      except Exception, detail:
         Trace.log(e_errors.ERROR, "Error moving to migration pool: %s"%(detail,))
      if src.has_key(item_to_move):
         del(src[item_to_move])
      self._lock.release()

   # move list from a list pool to purge work pool
   # @param - src - pool
   # @param - item_to_move item to move is a key in the pool
   def move_to_purge_pool(self, item_to_move):
      # src[item_to_move] is a file_list.FIleList or
      # file_list.FileListWithCRC
      self.cache_purge_pool[item_to_move].creation_time = time.time()
      list_id = self.cache_purge_pool[item_to_move].list_id
      Trace.trace(10, "move_to_purge_pool list_id %s"%(list_id,))
      try:
         self.purge_pool[list_id] = migration_dispatcher.MigrationList(self.cache_purge_pool[item_to_move],
                                                                       list_id,
                                                                       file_list.FILLED)
      except Exception, detail:
         Trace.log(e_errors.ERROR, "Error moving to purge pool: %s"%(detail,))
      if self.cache_purge_pool.has_key(item_to_move):
         del(self.cache_purge_pool[item_to_move])

   # check pools and move lists to migration pool
   # if needed
   # run this periodically
   def check_pools(self):
      Trace.trace(10, "CHECK POOLS")
      kick_migration = False
      # special case for purging cache
      t = time.time()
      if t - self.time_to_purge >= self.purge_pool_to: # run this every f.purge_pool_to seconds
         files_to_purge = self.file_purger.files_to_purge(self.check_watermarks)
         if not self.check_watermarks:
            self.check_watermarks = True
         if len(files_to_purge) != 0:
            for f in files_to_purge:
               self.cache_purge_pool[f.list_id] = f
         self.time_to_purge = time.time()
         self.purge_pool_to = 11*60
      for pool in (self.file_deleted_pool,
                   self.cache_missed_pool,
                   self.cache_written_pool,
                   self.cache_staged_pool):
         Trace.trace(10, "check_pools %s"%(pool,))
         for key in pool.keys():
             if pool[key].list_expired():
               # List time expired,
               # pass this list to Migration Dispatcher
               Trace.trace(10, "check pools passing to migration dispatcher %s" % (key,))
               self.move_to_migration_pool(pool, key)
               kick_migration = True
      if kick_migration:
         self.md.start_migration()
      files_to_purge = []
      kick_migration = False
      for key in self.cache_purge_pool.keys():
          if self.cache_purge_pool[key].list_expired():
            # List time expired,
            # pass this list to Migration Dispatcher
            Trace.trace(10, "check pools passing purge to migration dispatcher %s" % (key,))
            for item in self.cache_purge_pool[key].file_list:
               files_to_purge.append({'bfid': item['bfid'], 
                                      'cache_status':file_cache_status.CacheStatus.PURGING_REQUESTED,
                                      'archive_status': None,        # we are not changing this
                                      'cache_location': None})       # we are not changing this yet
            
            rc = set_cache_status.set_cache_status(self.fcc, files_to_purge)
            if rc['status'][0] != e_errors.OK:
               Trace.log(e_errors.ERROR, "set_cache_status failed %s"%(rc,))

            self.move_to_purge_pool(key)
            kick_migration = True
            
      if kick_migration:
         self.md.start_migration()
         
   def check_pools_thread(self):
      self.purge_pool_to = 0 # to start purging right after the dispather starts
      self.time_to_purge = time.time()
      while not self.shutdown:
         Trace.trace(10, "check_pools_thread")
         self.check_pools()
         time.sleep(10)

   def start(self):
        self.check_thread = threading.Thread(target=self.check_pools_thread,
                                             name="Check_Pools")
        self.check_thread.start()
        super(Dispatcher, self).start()
      

   ########################################################
   ### Event Handlers
   ########################################################

   def handle_file_deleted(self, message):
       Trace.trace(10, "handle_file_deleted received: %s"%(message))
       pass

   def handle_cache_missed(self, message):
       Trace.trace(10, "handle_cache_missed received: %s"%(message))
       l_name = message.content['file']['name']
       if self.clustered_configuration:
          disk_library = message.content['enstore'].get('disk_library')
       else:
          disk_library = ""
       f_list = file_list.FileListWithCRC(id = message.correlation_id,
                                          list_type = message.properties["en_type"],
                                          list_name = l_name,
                                          disk_library = disk_library)
       
       self.cache_missed_pool[l_name] = f_list
       list_element = file_list.FileListItemWithCRC(bfid = message.content['enstore']['bfid'],
                                                    nsid = message.content['file']['id'],
                                                    path = message.content['file']['name'],
                                                    libraries = [message.content['enstore']['vc']['library']],
                                                    crc =  message.content['file']['complete_crc'])

       self.cache_missed_pool[l_name].append(list_element)
       # if this is a package trigger sending the list
       # to the migrator right away
       bfid = message.content['enstore']['bfid']
       if type(bfid) == types.UnicodeType:
          bfid = bfid.encode("utf-8")

       Trace.trace(10, "handle_cache_missed getting bfid info for %s"%(bfid,))
       rec = self.fcc.bfid_info(bfid)
       if rec ['status'][0] == e_errors.OK and message.content['enstore']['bfid'] == rec['package_id']:
          self.cache_missed_pool[l_name].full = True
          self.move_to_migration_pool(self.cache_missed_pool, l_name)
          self.md.start_migration()
       return True


   def handle_cache_purged(self, message):
       Trace.trace(10, "handle_cache_purged received: %s"%(message))
       pass

   def handle_cache_written(self, message):
       Trace.trace(10, "handle_cache_written: %s"%(message))
       Trace.trace(10, "handle_cache_written content: %s"%(message.content,))
       Trace.trace(10, "handle_cache_written correlation_id: %s"%(message.correlation_id,))
       Trace.trace(10, "handle_cache_written type: %s"%(type(message.content),))

       # convert key, value in content to ascii if they are strings
       
       new_content = dict_u2a.convert_dict_u2a(message.content)

       # copy new_content['file'] to new_content['enstore'] to have all
       # items in place for policy selector
       for k, v in new_content['file'].items():
          new_content['enstore'][k] = v
       new_content['enstore']['file_size'] = new_content['enstore']['size']

       Trace.trace(10, "handle_cache_written new content: %s"%(new_content,))
       policy = self.policy_selector.match_found_pe(new_content['enstore'])
       if policy:
          # check if the file list exists
          Trace.trace(10, "handle_cache_written pool: %s"%(self.cache_written_pool.keys(),))
          Trace.trace(10, "handle_cache_written policy: %s"%(policy,))
          if not policy['policy'] in self.cache_written_pool.keys():
             if self.clustered_configuration:
                disk_library = policy.get('disk_library')
             else:
                disk_library = ""
             # Create list
             f_list = file_list.FileListWithCRC(id = message.correlation_id,
                                                list_type = message.properties["en_type"],
                                                list_name = policy['policy'],
                                                minimal_data_size = policy['minimal_file_size'],
                                                maximal_file_count = policy['max_files_in_pack'],
                                                max_time_in_list = policy['max_waiting_time'],
                                                disk_library = disk_library)
             self.cache_written_pool[policy['policy']] = f_list
          list_element = file_list.FileListItemWithCRC(bfid = new_content['enstore']['bfid'],
                                                       nsid = new_content['enstore']['id'],
                                                       path = new_content['enstore']['name'],
                                                       libraries = [new_content['enstore']['vc']['library']],
                                                       crc = new_content['enstore']['complete_crc'])
          
          self.cache_written_pool[policy['policy']].append(list_element, new_content['enstore']['file_size'])
          if self.cache_written_pool[policy['policy']].full:
             # pass this list to Migration Dispatcher
             Trace.trace(10, "handle_cache_written passing to migration dispatcher")
             self.move_to_migration_pool(self.cache_written_pool, policy['policy'])
             self.md.start_migration()
             
       else:
          Trace.alarm(e_errors.ALARM, "Potential data loss. No policy for %s"%(new_content,))
          
       Trace.trace(10, "handle_cache_written: POLICY:%s"%(policy['policy'],))
       return True
       

   def handle_cache_staged(self, message):
       Trace.trace(10, "handle_cache_staged reeceived: %s"%(message))
       pass


class DispatcherInterface(generic_server.GenericServerInterface):
   pass

def do_work():
    # get an interface
    intf = DispatcherInterface()

    # create  Migrator instance
    dispatcher = Dispatcher((intf.config_host, intf.config_port))
    
    dispatcher.handle_generic_commands(intf)

    #dispatcher._do_print({'levels':range(5,20)})
    dispatcher.start()
    #dispatcher.logger.log(e_errors.ERROR, "DISP START")
    dispatcher.logger.info("DISP START") # jst to check if logger works
 
    while True:
        t_n = 'dispatcher'
        if thread_is_running(t_n):
            pass
        else:
            Trace.log(e_errors.INFO, "dispatcher %s (re)starting %s"%(dispatcher.name, t_n))
            #lm.run_in_thread(t_n, lm.mover_requests.serve_forever)
            dispatching_worker.run_in_thread(t_n, dispatcher.serve_forever)

        time.sleep(10)
        

    Trace.alarm(e_errors.ALARM,"dispatcher %s finished (impossible)"%(intf.name,))

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
