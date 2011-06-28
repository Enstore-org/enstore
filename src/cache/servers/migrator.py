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
import threading
import types
import logging
import unicodedata

# enstore imports
import e_errors
import configuration_client
import event_relay_client
import info_client
import file_clerk_client
import event_relay_messages
import generic_server
import dispatching_worker
import monitored_server
import option
import Trace
import enstore_functions3
import encp_wrapper

# enstore cache imports
#import cache.stub.mw as mw
import mw
from cache.messaging.messages import MSG_TYPES as mt

# find a common prefix of 2 strings
# presenting file paths (beginning with 1st position) 
# find a common prefix of 2 strings (beginning with 1st position) 
def find_common_prefix(s1, s2):
    if len(s1) < len(s2): # swap s1 and s2
        t_s2 = s2
        s2 = s1
        s1 = t_s2
    s1_split = s1.split('/')
    s2_split = s2.split('/')
    common = []

    for i in range(len(s2_split)):
        if  s2_split[i] == s1_split[i]:
            common.append(s2_split[i])
        else:
            break
    p = '/'.join((common))
    #print "P", p
    return p


# find directory common for all files in a path
def find_common_dir(paths):
    # paths - list of absolute file paths
    dirs = []
    for path in paths:
        if path[0] != '/':
            return None # path must be absolute
        else:
            dirs.append(os.path.dirname(path))
    common_path = dirs[0]
    print len(dirs)
    for i in range(1, len(dirs)):
        print "D", i, dirs[i]
        common_path = find_common_prefix(common_path, dirs[i])
        print "COMD", common_path
    return common_path

class Migrator(dispatching_worker.DispatchingWorker, generic_server.GenericServer, mw.MigrationWorker):
    """
    Configurable Migrator
    """


    def load_configuration(self):
        self.my_conf = self.csc.get(self.name)
        #file_clerk_conf = self.csc.get('file_clerk')
        self.data_area = self.my_conf['data_area'] # area on disk where original files are stored
        self.archive_area = self.my_conf['archive_area'] # area on disk where archvived files are temporarily stored
        self.stage_area = self.my_conf['stage_area']  # area on disk where staged files are temporarily stored

        self.my_dispatcher = self.csc.get(self.my_conf['migration_dispatcher']) # migration dispatcher configuration
        self.my_broker = self.csc.get("amqp_broker") # amqp broker configuration
        
        # configuration dictionary required by MigrationWorker
        self.migration_worker_configuration = {}
        self.migration_worker_configuration['server'] = self.my_dispatcher
        self.migration_worker_configuration['server']['queue_in'] = self.name.split('.')[0] # migrator input control queue
        # get amqp broker configuration - common for all servers
        # @todo - change name in configuration file to make it more generic, "amqp"
        self.migration_worker_configuration['amqp'] = {}
        self.migration_worker_configuration['amqp']['broker'] = self.csc.get("amqp_broker")
        if_conf = self.csc.get("info_server")
        fc_conf = self.csc.get("file_clerk")
        self.info_client = info_client.infoClient(self.csc,
                                                  server_address=(if_conf['host'],
                                                                  if_conf['port']))
        self.fcc = file_clerk_client.FileClient(self.csc, bfid=0,
                                                server_address=(fc_conf['host'],
                                                                fc_conf['port']))

    def  __init__(self, name, cs):
        """
        Creates Migrator. Constructor gets configuration from enstore Configuration Server
        
        """
        # Obtain information from the configuration server cs.
        self.csc = cs

        generic_server.GenericServer.__init__(self, self.csc, name,
					      function = self.handle_er_msg)


        Trace.init(self.log_name, 'yes')

        try:
            self.load_configuration()
        except:
            Trace.handle_error()
            sys.exit(-1)

        self.alive_interval = monitored_server.get_alive_interval(self.csc,
                                                                  name,
                                                                  self.my_conf)
        dispatching_worker.DispatchingWorker.__init__(self, (self.my_conf['hostip'],
                                                             self.my_conf['port']),
                                                      use_raw=0)
        
        Trace.log(e_errors.INFO, "create %s server instance, qpid client instance %s"%
                  (self.name,
                   self.migration_worker_configuration['amqp']))

        mw.MigrationWorker.__init__(self, name, self.migration_worker_configuration)
        #self.srv = mw.MigrationWorker(self.name, self.migration_worker_configuration)
        #self.srv.set_handler(mt.MWC_ARCHIVE, self.handle_write_to_tape)
        self.set_handler(mt.MWC_ARCHIVE, self.handle_write_to_tape)
        Trace.log(e_errors.INFO, "Migrator %s instance created"%(self.name,))

        self.resubscribe_rate = 300
        self.erc = event_relay_client.EventRelayClient(self, function = self.handle_er_msg)
        Trace.erc = self.erc # without this Trace.notify takes 500 times longer
        self.erc.start([event_relay_messages.NEWCONFIGFILE],
                       self.resubscribe_rate)

        # start our heartbeat to the event relay process
        self.erc.start_heartbeat(name, self.alive_interval)

        
        # get amqp broker configuration - common for all servers
    """
    def start(self):
        if not self.srv:
            return
        
        try:
            self.srv.start()
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            self.log.critical("%s %s IS QPID BROKER RUNNING?",str(exc_type),str(exc_value))
            self.log.error("CAN NOT ESTABLISH CONNECTION TO QPID BROKER ... QUIT!")
            sys.exit(1)
        
        #self.trace.debug("server started in start()")
    
    def stop(self):
        self.srv.stop()
        #self.trace.debug("server stopped in stop()")

    """

    # pack files into a single aggregated file
    # if there are multiple files in request list
    # 
    def pack_files(self, request_list):
        if not request_list:
            return None
        cache_file_list = []
        ns_file_list = []
        bfids = []
        # create list of files to pack
        for component in request_list:
            cache_file_path = enstore_functions3.file_id2path(self.data_area,
                                                              component['nsid']) # pnfsId
            ### Before packaging file we need to make sure that the packaged file
            ### containing these files is not already written.
            ### This can be done by checking duplicate files map.
            ### Implementation must be HERE!!!!
            
            bfids.append(component['bfid'])
            # Check if such file exists in cache.
            # This should be already checked anyway.
            if os.path.exists(cache_file_path):
                cache_file_list.append((cache_file_path, component['path']))
                ns_file_list.append(component['path']) # file path in name space
            else:
                Trace.log(e_errors.ERROR, "File aggregation failed. File %s does not exist in cache"%(cache_file_path))
                return None
        if len(cache_file_path) == 1: # single file
            src = cache_file_path
            dst = self.request_list[0][ 'path'] # complete file path in name space
        else: # multiple files
            # Create a special file containing the names
            # of file as known in the namespace.
            # This file can be used for metadate recovery.
            #
            # find the destination directory
            dst = find_common_dir(ns_file_list)
            if not dst:
                Trace.log(e_errors.ERROR, "File aggregation failed. Can not find common destination directory")
                return None

            # Create tar file name
            t = time.time()
            t_int = long(t)
            fraction = int(t-t_int)*1000 # this gradation allows 1000 distinct file names 
            src_fn = ".package-%s.%sZ"%(time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(t_int)),fraction)

            # Create archive directory
            archive_dir = os.path.join(self.archive_area, src_fn)
            
            src_path = "%s.tar"%(os.path.join(archive_dir, src_fn),)
            special_file_name = "README.1st"
 
            if not os.path.exists(archive_dir):
                os.makedirs(archive_dir)
                
            os.chdir(archive_dir)
            special_file_name = "README.1st"
            special_file = open(special_file_name, 'w')
            special_file.write("List of cached files and original names\n")

            for f, c_f in cache_file_list:
                special_file.write("%s %s\n"%(f, c_f))
            
            special_file.close()

            # Create tar file
            #os.system("tar --force-local -czf %s.tgz %s"%(src_fn, special_file_name))
            os.system("tar --force-local -cf %s %s"%(src_path, special_file_name))

            # Append cached files to archive
            for f, junk in cache_file_list:
                Trace.trace(10, "pack_files: add %s to %s"%(f, src_path))
                rtn = os.system("tar --force-local -rPf %s.tar %s"%(src_path, f))
                print "RTN", rtn

        # Qpid converts strings to unicode.
        # Encp does not like this.
        # Convert unicode to ASCII strings.
        if type(src_path) == types.UnicodeType:
            src_path = unicodedata.normalize("NFKD", src_path).encode("ascii", "ignore")
        if type(dst) == types.UnicodeType:
            dst = unicodedata.normalize("NFKD", dst).encode("ascii", "ignore")
        dst_path = "%s.tar"%(os.path.join(dst, src_fn))
            
        return src_path, dst_path, bfids

    # write aggregated file to tape
    def write_to_tape(self, request_list):
        Trace.trace(10, "write_to_tape: request %s"%(request_list,))
        #sys.exit(0)
        if request_list:
            src_file_path, dst_file_path, bfid_list = self.pack_files(request_list)
            if not (src_file_path and dst_file_path):
                # aggregation failed
                raise e_errors.EnstoreError(None, "Write to tape failed: no files to write", e_errors.NO_FILES)
        else:
            raise e_errors.EnstoreError(None, "Write to tape failed: no files to write", e_errors.NO_FILES)

        Trace.trace(10, "write_to_tape: will write %s into %s"%(src_file_path, dst_file_path,))
        #return
        print "DST", type(dst_file_path)
        encp = encp_wrapper.Encp()
        args = ["encp", "--disable-redirection", src_file_path,dst_file_path]  
        cmd = "encp %s %s"%(src_file_path, dst_file_path)
        rc = encp.encp(args)
        Trace.trace(10, "write_to_tape: encp returned %s"%(rc,))

        if rc != 0:
            raise e_errors.EnstoreError(None, "encp write to tape failed", rc)

        # register file in file db
        
        res = self.info_client.find_file_by_path(dst_file_path)
        Trace.trace(10, "write_to_tape: find file %s returned %s"%(dst_file_path, res))
        
        if e_errors.is_ok(res['status']):
            dst_bfid = res['bfid']
            #res = self.fcc.register_copies(bfid_list, dst_bfid)
        if not e_errors.is_ok(res['status']):
            raise e_errors.EnstoreError(None, "Write to tape failed: %s"%(res['status'][1],), res['status'][0])

        # The file from cache was successfully written to tape.
        # Remove temporary archie directory
        os.remove(os.path.dirname(src_file_path))
        
    def handle_write_to_tape(self, message):
        Trace.trace(10, "handle_write_to_tape received: %s"%(message))
        self.work_dict[message.correlation_id] = message
        self.write_to_tape(message.content)
        

    # read aggregated file from tape
    def read_from_tape(self):
        encp = encp_wrapper.Encp()
        pass

    # unpack aggregated file
    def unpack_files(self):
        pass
        
class MigratorInterface(generic_server.GenericServerInterface):
    def __init__(self):
        # fill in the defaults for possible options
        generic_server.GenericServerInterface.__init__(self)


    migrator_options = {}

    # define the command line options that are valid
    def valid_dictionaries(self):
        return generic_server.GenericServerInterface.valid_dictionaries(self) \
               + (self.migrator_options,)

    parameters = ["migrator"]

    # parse the options like normal but make sure we have a library manager
    def parse_options(self):
        option.Interface.parse_options(self)
        # bomb out if we don't have a library manager
        if len(self.args) < 1 :
            self.missing_parameter(self.parameters())
            self.print_help()
            sys.exit(1)
        else:
            self.name = self.args[0]
    
def do_work():
    # get an interface
    intf = MigratorInterface()

    import cache.en_logging.config_test_unit
    
    cache.en_logging.config_test_unit.set_logging_console()

    # create a udp_proxy instance
    migrator = Migrator(intf.name, (intf.config_host, intf.config_port))
    migrator.handle_generic_commands(intf)

    #Trace.init(migrator.log_name, 'yes')
    migrator._do_print({'levels':range(5, 400)}) # no manage_queue
    migrator.start()
 
    while True:
        t_n = 'migrator'
        if thread_is_running(t_n):
            pass
        else:
            Trace.log(e_errors.INFO, "migrator %s (re)starting %s"%(intf.name, t_n))
            #lm.run_in_thread(t_n, lm.mover_requests.serve_forever)
            dispatching_worker.run_in_thread(t_n, migrator.serve_forever)

        time.sleep(10)
        

    Trace.alarm(e_errors.ALARM,"migrator %s finished (impossible)"%(intf.name,))
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
