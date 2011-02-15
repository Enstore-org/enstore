#!/usr/bin/env python

###############################################################################
#
# $Id$
# Migrates files between cache and tape
#
###############################################################################
# system imports
import sys
import os
import os.path
import time
import errno
import string
import socket
import select
import threading

# enstore imports
import hostaddr
import callback
import dispatching_worker
import generic_server
import edb
import Trace
import e_errors
import configuration_client
import volume_family
import esgdb
import enstore_constants
import monitored_server
import inquisitor_client
import cPickle
import event_relay_messages
import udp_common
import enstore_functions2
import enstore_functions3


import file_clerk_client
import encp_wrapper
import cache.messaging.client

MY_NAME = "Migrator"

# internal request status
PACKING, ARCHIVING, STAGING, UNPACKING = ["PACKING", "ARCHIVING", "STAGING", "UNPACKING"]

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

class Migrator(dispatching_worker.DispatchingWorker,
               #generic_server.GenericServer,
               MigratorMethods):
    
    def load_configurtation(self):
        self.my_conf = self.csc.get(self.name)
        #file_clerk_conf = self.csc.get('file_clerk')
        self.data_area = self.my_conf['data_area'] # area on disk where original files are stored
        self.archive_area = self.my_conf['archive_area'] # area on disk where archvived files are temporarily stored
        self.stage_area = self.my_conf['stage_area']  # area on disk where staged files are temporarily stored
        self.queue_name = self.my_conf['queue_in'] # amq queue name
        self.reply_queue_name = self.my_conf['queue_out'] # amq queue name for replies
        self.broker = self.my_conf['amqp_broker'] # amq message broker
        self.max_workers = self.my_conf.get('max_workers',1) # maximal number of worker processes
        
    def __init__(self, migrator, csc):
        self.name = migrator
        self.csc = csc # configuration server client
        self.load_configuration()
        self.request_list = None # request list
        #self.aggregated_file = None is this needed?? AM!!
        self.fcc = file_clerk_client.FileClient(self.csc, bfid=0) # file clerk client

        generic_server.GenericServer.__init__(self, self.csc, migrator,
					      function = self.handle_er_msg)
        dispatching_worker.DispatchingWorker.__init__(self, (self.my_conf['hostip'],
                                                             self.my_conf['port']))
        self.qpid_client = cache.messaging.client.EnQpidClient(self.broker, self.queue_name, self.reply_queue_name)
        self.manager = multiprocessing.Manager()
        self.lock = self.manager.Lock()
        self.requests = self.manager.dict() # shared between processes request dictionary
        self.active_workers = self.manager.Value("i", 0) # number of active worker processes

        self.request = None # this is a request copy for individual process
 


    # increment active_workers
    def inc_workers(self):
        self.lock.acquire()
        if self.active_workers.value < self.max_workers:
            self.active_workers.value = self.active_workers + 1
        self.lock.release()

    # decrement active_workers
    def dec_workers(self):
        self.lock.acquire()
        if self.active_workers.value > 0
            self.active_workers.value = self.active_workers - 1
        self.lock.release()
    
    # send status of completed work to
    # migration dispatcher
    def work_done(self, status):
        # amq_name - name of amq queue
        pass

    # pack files into a single aggregated file
    # if there are multiple files in request list
    # 
    def pack_files(self):
        if not self.request:
            return None
        cache_file_list = []
        ns_file_list = []
        # create list of files to pack
        for component in self.request:
            cache_file_path = enstore_functions3.file_id2path(self.data_area,
                                                              component[1]) # pnfsId
            # Check if such file exists in cache.
            # This should be already checked anyway.
            if os.path.exists(cache_file_path):
                cache_file_list.append(cache_file_path)
                ns_file_list.append(component[2]) # file path in name space
            else:
                Trace.log(e_errors.ERROR, "File aggregation failed. File %s does not exist in cache"%(cache_file_path))
                return None
        if len(cache_file_path) == 1: # single file
            src = cache_file_path
            dst = self.self.request[0][2] # complete file path in name space
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

            special_file = open(os.path.join(archive_dir, README.1st). 'w')
            special_file.write("List of cached files and original names\n")

            for f, c_f in ns_file_list, cache_file_list:
                special_file.write("%s %s\n"%(f, c_f))

            # Create tar file
            t = time.time()
            t_int = long(t)
            fraction = int(t-t_int)*1000 # this gradation allows 1000 distinct file names 
            src_fn = ".package-%s.%sZ"%(time.strftime("%Y-%m-%dT%H:%M:%S", t_int),fraction)
            src = os.path.join(self.archive_area, src_fn)
            os.chdir(self.archive_area)
            os.system("tar -czf %s.tgz %s"%(src_fn, README.1st))

            # Append cached files to archive
            for f in cache_file_list:
                rtn = os.system("tar -Prf %s.tgz %s"%(src_fn, f))
                print "RTN", rtn

        return src, dst

    # write aggregated file to tape
    def write_to_tape(self):
        if self.request:
            if len(self.request) > 1: # multiple files need an aggregation
                src_file_path, dst_file_path = self.pack_files(self.request)
                if not (src_file_path and dst_file_path):
                    # aggregation failed
                    self.work_done("Failed: no files to write to tape") # replace with status AM!!
                    return
            else:
                # find destination file path in name space
                src_file_path = enstore_functions3.file_id2path(self.data_area,
                                                                self.request[0][1]) # pnfsId
                dst_file_path = self.self.request[0][2]
        else:
            self.work_done("Failed: no files to write to tape") # replace with status AM!!
            return
        encp = encp_wrapper.Encp()
        args = ["encp", src_file_path, dst_file_path]
        try:
            rc = encp.encp(args)
        except:
            rc = 1
            self.work_done("Failed: encp write to tape failed") # replace with status AM!!
        
    # read aggregated file from tape
    def read_from_tape(self):
        encp = encp_wrapper.Encp()
        pass

    # unpack aggregated file
    def unpack_files(self):
        pass
        

    # get requests from migration dispatcher
    # this runs in a separate thread
    def get_requests(self):
        self.qpid_client.start()
        while self.run:
            if self.active_workers >= self.max_workers:
                # do nothiing
                time.sleep(1)
                continue
            msg = self.qpid_client.rcv.fetch()
            if msg:
                self.request = msg.content
                # request structure:
                # {"list_id": id,   # - unique list id
                #  "operation": op, # - archive, stage, purge
                #  "list" :[[bfid1, # - bit file id
                #   pnfsId1,        # - name space file id
                #   path1,          # - complete file path in name space
                #     [lm1,         # - list if libaries from pnfs tag
                #     lmA]],
                #   [bfidN,          # - bit file id
                #   pnfsIdN,        # - name space file id
                #   pathN,          # - complete file path
                #     [lm1,         # - list if libraries from pnfs tag
                #     lmB]]
                # ]}  
                
                self.request['correlation_id'] =  msg.correlation_id
                self.lock.acqure()
                try:
                    self.requests[request['list_id']] = self.request
                except Exception

                # start worker
                proc = multiprocessing.Process(target=self.process_request)
                try:
                    proc.start()
                    self.inc_workers()
                except: # make this better AM!
                    exc, detail, tb = sys.exc_info()
                    print detail

                ssn.acknowledge(sync=False) # investigate how this works AM!

    # process request
    # this runs in a separate thread (process)
    def process_request(self):
        if self.request['operation'] == 'archive':
            self.write_to_tape()
        elif self.request['operation'] == 'stage':
            self.read_from_tape()
        else:
           self.work_done("Failed: only archive and stage operations are permitted") # replace with status AM!!
        del(self.requests[self.request['list_id']])
        self.dec_workers()
            



class MigratorInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
        generic_server.GenericServerInterface.__init__(self)


    migrator_options = {}

    # define the command line options that are valid
    def valid_dictionaries(self):
        return generic_server.GenericServerInterface.valid_dictionaries(self) \
               + (self.migrator_options,)

    paramaters = ["migrator_name"]

    # parse the options like normal but make sure we have a migrator
    def parse_options(self):
        option.Interface.parse_options(self)
        # bomb out if we don't have a library manager
        if len(self.args) < 1 :
            self.missing_parameter(self.parameters())
            self.print_help()
            sys.exit(1)
        else:
            self.name = self.args[0]


if __name__=="__main__":
    migrator = Migrator("M1.migrator")
