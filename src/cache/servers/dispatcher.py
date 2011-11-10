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
import lmd_policy_selector
import dict_u2a
import migration_dispatcher
import mw
import cache.messaging.mw_client
from cache.messaging.messages import MSG_TYPES as mt
import cache.messaging.file_list as file_list


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
        self.dispatcher_configuration['server']['queue_in'] = "%s; {create: receiver, delete: receiver}"%(self.queue_in_name,) # dispatcher input control queue


        print "MY_CONF", self.my_conf 

        self.policy_file = self.my_conf['policy_file']
        try:
            self.policy_selector = lmd_policy_selector.Selector(self.policy_file)
        except Exception, detail:
            Trace.log(e_errors.ALARM, "Can not create policy selector: %s" %(detail,))
            sys.exit(-1)
        
        # get amqp broker configuration - common for all servers
        self.dispatcher_configuration['amqp'] = {}
        self.dispatcher_configuration['amqp']['broker'] = self.csc.get("amqp_broker")
        
        # create pools for lists
        self.file_deleted_pool = {}
        self.cache_missed_pool = {}
        self.cache_purged_pool = {}
        self.cache_written_pool = {}
        self.cache_staged_pool = {}

        # create migration pool
        self.migrartion_pool = {}

        # reate Migration Dispatcher
        self.md = migration_dispatcher.MigrationDispatcher("M_D",
                                                      self.dispatcher_configuration['amqp']['broker'],
                                                      self.my_conf['migrator_work'],
                                                      self.my_conf['migrator_reply'],
                                                      self._lock,
                                                      self.migrartion_pool)

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
        print "alive interval", self.alive_interval


        dispatching_worker.DispatchingWorker.__init__(self, (self.my_conf['hostip'],
	                                              self.my_conf['port']))
        self.resubscribe_rate = 300

	self.erc = event_relay_client.EventRelayClient(self)
	self.erc.start_heartbeat(self.name,  self.alive_interval)


   # move list from a list pool to migration pool
   # @param - from - pool
   # @param - item_to_move 
   def move_to_migration_pool(self, src, item_to_move):
      self._lock.acquire()
      list_id = src[item_to_move].list_id
      Trace.trace(10, "move_to_migration_pool list_id %s"%(list_id,))
      self.migrartion_pool[list_id] = migration_dispatcher.MigrationList(src[item_to_move],
                                                                         list_id,
                                                                         file_list.FILLED)
      del(src[item_to_move])
      self._lock.release()
      
   # check pools and move lists to migration pool
   # if needed
   # run this periodically
   def check_pools(self):
      kick_migration = False
      for pool in (self.file_deleted_pool,
                   self.cache_missed_pool,
                   self.cache_purged_pool,
                   self.cache_written_pool,
                   self.cache_staged_pool):
         Trace.trace(10, "check_pools %s"%(pool,))
         for key in pool.keys():
             if pool[key].list_expired:
               # List time expired,
               # pass this list to Migration Dispatcher
               Trace.trace(10, "check pools passing to migration dispatcher %s" % (key,))
               self.move_to_migration_pool(pool, key)
               kick_migration = True
      if kick_migration:
         self.md.start_migration()
         
   def check_pools_thread(self):
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
       Trace.trace(10, "handle_cache_missed content: %s"%(message.content))
       Trace.trace(10, "handle_cache_missed correlation_id: %s"%(message.correlation_id))
       # pull out content of 'vc' subdictionary into dictionary
       # we need to do this because message format does not comply with
       # policy format
       for k, v in message.content['vc'].items():
          message.content[k] = v
       del(message.content['vc'])
       policy_string = self.policy_selector.match_found_pe(message.content)
       Trace.trace(10, "handle_cache_missed: POLICY:%s"%(policy_string,))
       
       pass

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
       rc, policy_string, min_package_size = self.policy_selector.match_found_pe(new_content['enstore'])
       if rc:
          # check if the file list exists
          if not policy_string in self.cache_written_pool.keys():
             # Create list
             f_list = file_list.FileListWithCRC(id = message.correlation_id,
                                                list_type = message.properties["en_type"],
                                                list_name = policy_string,
                                                minimal_data_size = min_package_size,
                                                maximal_file_count = 100,
                                                max_time_in_list = 30)
             self.cache_written_pool[policy_string] = f_list
          list_element = file_list.FileListItemWithCRC(bfid = new_content['enstore']['bfid'],
                                                       nsid = new_content['enstore']['id'],
                                                       path = new_content['enstore']['name'],
                                                       libraries = [new_content['enstore']['vc']['library']],
                                                       crc = new_content['enstore']['complete_crc'])
          
          self.cache_written_pool[policy_string].append(list_element, new_content['enstore']['file_size'])
          if self.cache_written_pool[policy_string].full:
             # pass this list to Migration Dispatcher
             Trace.trace(10, "handle_cache_written passing to migration dispatcher")
             self.move_to_migration_pool(self.cache_written_pool, policy_string)
             md.start_migration()
             
       else:
          Trace.alarm(e_errors.ALARM, "Potential data loss. No policy for %s"%(new_content,))
          
       Trace.trace(10, "handle_cache_written: POLICY:%s"%(policy_string,))
       
       
       pass
   def handle_cache_staged(self, message):
       Trace.trace(10, "handle_cache_written reeceived: %s"%(message))
       pass
   


class DispatcherInterface(generic_server.GenericServerInterface):
   pass
    
def do_work():
    # get an interface
    intf = DispatcherInterface()

    import cache.en_logging.config_test_unit
    
    cache.en_logging.config_test_unit.set_logging_console()

    # create  Migrator instance
    dispatcher = Dispatcher((intf.config_host, intf.config_port))
    dispatcher.handle_generic_commands(intf)

    #Trace.init(migrator.log_name, 'yes')
    dispatcher._do_print({'levels':range(5, 400)})
    dispatcher.start()
 
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
