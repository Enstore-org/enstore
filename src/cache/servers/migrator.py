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
import copy
import errno
import logging
import statvfs

# enstore imports
import e_errors
import configuration_client
import event_relay_client
import file_clerk_client
import info_client
import event_relay_messages
import generic_server
import dispatching_worker
import monitored_server
import option
import Trace
import enstore_functions2
import enstore_functions3
import encp_wrapper
import file_cache_status

# enstore cache imports
#import cache.stub.mw as mw
import mw
import cache.messaging.mw_client
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
        self.stage_area = self.my_conf.get('stage_area','')   # area on disk where staged files are stored
        self.tmp_stage_area = self.my_conf['tmp_stage_area']  # area on disk where staged files are temporarily stored

        self.my_dispatcher = self.csc.get(self.my_conf['migration_dispatcher']) # migration dispatcher configuration
        self.purge_watermarks = None
        if self.my_dispatcher:
          self.purge_watermarks = self.my_dispatcher.get("purge_watermarks", None) 
            
        self.my_broker = self.csc.get("amqp_broker") # amqp broker configuration
        
        # configuration dictionary required by MigrationWorker
        self.migration_worker_configuration = {'server':{}}
        self.migration_worker_configuration['server']['queue_work'] = self.my_dispatcher['migrator_work']
        self.migration_worker_configuration['server']['queue_reply'] = self.my_dispatcher['migrator_reply']        
        
        self.queue_in_name = self.name.split('.')[0]
        self.migration_worker_configuration['server']['queue_in'] = "%s; {create: receiver, delete: receiver}"%(self.queue_in_name,) # migrator input control queue
        # get amqp broker configuration - common for all servers
        # @todo - change name in configuration file to make it more generic, "amqp"
        self.migration_worker_configuration['amqp'] = {}
        self.migration_worker_configuration['amqp']['broker'] = self.csc.get("amqp_broker")
        self.status = None # internal status of migrator to report to Migration Dispatcher
        fc_conf = self.csc.get("file_clerk")
        self.fcc = file_clerk_client.FileClient(self.csc, bfid=0,
                                                server_address=(fc_conf['host'],
                                                                fc_conf['port']))
        ic_conf = self.csc.get("info_server")
        self.infoc = info_client.infoClient(self.csc, 
                                            server_address=(ic_conf['host'],
                                                            ic_conf['port']))
        Trace.trace(10, "d_a %s a_a %s s_a %s t_s_a %s"%(self.data_area,
                                                         self.archive_area,
                                                         self.stage_area,
                                                         self.tmp_stage_area))

    def  __init__(self, name, cs):
        """
        Creates Migrator. Constructor gets configuration from enstore Configuration Server
        
        """
        # Obtain information from the configuration server cs.
        self.csc = cs

        generic_server.GenericServer.__init__(self, self.csc, name,
					      function = self.handle_er_msg)


        Trace.init(self.log_name, 'yes')
        self._do_print({'levels':range(5, 400)})
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

        """
        Leave these here so far, as there may be an argument for have a separate handler for eact type of the message

        self.set_handler(mt.MWC_ARCHIVE, self.handle_write_to_tape)
        self.set_handler(mt.MWC_PURGE, self.handle_purge)
        self.set_handler(mt.MWC_STAGE, self.handle_stage_from_tape)
        self.set_handler(mt.MWC_STATUS, self.handle_status)
        """
        
        # we want all message types processed by one handler
        self.handlers = {}
        for request_type in (mt.MWC_PURGE,
                             mt.MWC_ARCHIVE,
                             mt.MWC_STAGE,
                             mt.MWC_STATUS):
            self.handlers[request_type] = self.handle_request
        
        Trace.log(e_errors.INFO, "Migrator %s instance created"%(self.name,))

        self.resubscribe_rate = 300
        self.erc = event_relay_client.EventRelayClient(self, function = self.handle_er_msg)
        Trace.erc = self.erc # without this Trace.notify takes 500 times longer
        self.erc.start([event_relay_messages.NEWCONFIGFILE],
                       self.resubscribe_rate)

        # start our heartbeat to the event relay process
        self.erc.start_heartbeat(name, self.alive_interval)

    # file clerk set_cache_status currently can not send long messages
    # this is a temporary solution
    def set_cache_status(self, set_cache_params):
        Trace.trace(10, "set_cache_status: cache_params %s"%(len(set_cache_params),))
        # create a local copy of set_cache_params,
        # because the array will be modified by pop()
        local_set_cache_params = copy.copy(set_cache_params)
        list_of_set_cache_params = []
        tmp_list = []
        while len(local_set_cache_params) > 0:
            param = local_set_cache_params.pop()
            if len(str(tmp_list)) + len(str(param)) < 15000: # the maximal message size is 16384
                tmp_list.append(param)
            else:
               list_of_set_cache_params.append(tmp_list)
               tmp_list = []
               tmp_list.append(param)
        if not list_of_set_cache_params:
            # tmp_list size did not exceed 15000
            list_of_set_cache_params.append(tmp_list)
        Trace.trace(10, "set_cache_status: params %s"%(list_of_set_cache_params,))
               
        # now send all these parameters
        for param_list in list_of_set_cache_params:
            Trace.trace(10, "set_cache_status: sending set_cache_status %s"%(param_list,))
            
            rc = self.fcc.set_cache_status(param_list)
            Trace.trace(10, "set_cache_status: set_cache_status 1 returned %s"%(rc,))
        return rc

    # pack files into a single aggregated file
    # if there are multiple files in request list
    # 
    def pack_files(self, request_list):
        Trace.trace(10, "pack_files: request_list %s"%(request_list,))
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
            Trace.trace(10, "pack_files: cache_file_path %s"%(cache_file_path,))
            
            if type(component['bfid']) == types.UnicodeType:
                component['bfid'] = component['bfid'].encode("utf-8")
            bfids.append(component['bfid'])
            # Check if such file exists in cache.
            # This should be already checked anyway.
            if os.path.exists(cache_file_path):
                cache_file_list.append((cache_file_path, component['path'], component['complete_crc']))
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

            Trace.trace(10, "pack_files: cache_file_list: %s"%(cache_file_list,))
            for f, c_f, crc in cache_file_list:
                special_file.write("%s %s %s\n"%(f, c_f, crc))
            
            special_file.close()

            # Create tar file
            #os.system("tar --force-local -czf %s.tgz %s"%(src_fn, special_file_name))
            os.system("tar --force-local -cf %s %s"%(src_path, special_file_name))

            # Append cached files to archive
            for f, junk, junk in cache_file_list:
                Trace.trace(10, "pack_files: add %s to %s"%(f, src_path))
                rtn = os.system("tar --force-local -rPf %s %s"%(src_path, f))

        # Qpid converts strings to unicode.
        # Encp does not like this.
        # Convert unicode to ASCII strings.
        if type(src_path) == types.UnicodeType:
            src_path = src_path.encode("utf-8")
        if type(dst) == types.UnicodeType:
            dst = dst.encode("utf-8")
        dst_path = "%s.tar"%(os.path.join(dst, src_fn))
        Trace.trace(10, "pack_files: returning %s %s %s"%(src_path, dst_path, bfids))
           
        return src_path, dst_path, bfids

    # write aggregated file to tape
    def write_to_tape(self, request):
        # save request
        rq = copy.copy(request)
        Trace.trace(10, "write_to_tape: request %s"%(rq.content,))
        # save correlation_id
        request_list = rq.content['file_list']
        #sys.exit(0)
        if request_list:
            # check if files can be written to tape
            write_enabled_counter = 0
            for component in request_list:
                bfid = component['bfid']
                # Convert unicode to ASCII strings.
                if type(bfid) == types.UnicodeType:
                    bfid = bfid.encode("utf-8")
                rec = self.fcc.bfid_info(bfid)

                if (rec['status'][0] == e_errors.OK and
                    (rec['archive_status'] not in
                     (file_cache_status.ArchiveStatus.ARCHIVED, file_cache_status.ArchiveStatus.ARCHIVING)) and
                    (rec['deleted'] == "no")): # file can be already deleted by the archiving time
                    write_enabled_counter = write_enabled_counter + 1
            if write_enabled_counter != len(request_list):
                Trace.log(e_errors.ERROR, "No files will be archived, because some of them or all are already archived or being archived")
                return True
            packed_file = self.pack_files(request_list)
            if packed_file:
                src_file_path, dst_file_path, bfid_list = packed_file
            else:
                src_file_path = dst_file_path = None
            if not (src_file_path and dst_file_path):
                # aggregation failed
                raise e_errors.EnstoreError(None, "Write to tape failed: no files to write", e_errors.NO_FILES)
        else:
            raise e_errors.EnstoreError(None, "Write to tape failed: no files to write", e_errors.NO_FILES)

        # Change archive_status
        for bfid in bfid_list:
            # fill the argumet list for
            # set_cache_status method
            set_cache_params = []
            set_cache_params.append({'bfid': bfid,
                                     'cache_status':None,# we are not changing this
                                     'archive_status': file_cache_status.ArchiveStatus.ARCHIVING,
                                     'cache_location': None})       # we are not changing this
        rc = self.set_cache_status(set_cache_params)
        Trace.trace(10, "write_to_tape: will write %s into %s"%(src_file_path, dst_file_path,))
        #return
        encp = encp_wrapper.Encp()
        args = ["encp", src_file_path, dst_file_path]  
        cmd = "encp %s %s"%(src_file_path, dst_file_path)
        rc = encp.encp(args)
        Trace.trace(10, "write_to_tape: encp returned %s"%(rc,))

        # encp finished
        if rc != 0:
            set_cache_params = []
            # Change archive_status
            for bfid in bfid_list:
                set_cache_params.append({'bfid': bfid,
                                         'cache_status':None,# we are not changing this
                                         'archive_status': "null",
                                         'cache_location': None})       # we are not changing this
                        
            #rc = self.fcc.set_cache_status(set_cache_params)
            rc = self.set_cache_status(set_cache_params)
            raise e_errors.EnstoreError(None, "encp write to tape failed", rc)
        
        # register archive file in file db
        res = self.infoc.find_file_by_path(dst_file_path)
        Trace.trace(10, "write_to_tape: find file %s returned %s"%(dst_file_path, res))
        
        if e_errors.is_ok(res['status']):
            dst_bfid = res['bfid']
            update_time = time.localtime(time.time())
            package_files_count = len(bfid_list)
            bfid_list.append(dst_bfid)
            for bfid in bfid_list:
                del(rec) # to avoid interference
                # read record
                rec = self.fcc.bfid_info(bfid)
                rec['archive_status'] = file_cache_status.ArchiveStatus.ARCHIVED
                rec['package_id'] = dst_bfid
                rec['package_files_count'] = package_files_count
                rec['active_package_files_count'] = package_files_count
                rec['archive_mod_time'] = time.strftime("%Y-%m-%d %H:%M:%S", update_time)
                Trace.trace(10, "write_to_tape: sending modify record %s"%(rec,))
                rc = self.fcc.modify(rec)
            #########
                                                 
        else:
            raise e_errors.EnstoreError(None, "Write to tape failed: %s"%(res['status'][1],), res['status'][0])

        # The file from cache was successfully written to tape.
        # Remove temporary file
        Trace.trace(10, "write_to_tape: removing temporary file %s"%(src_file_path,))
        
        os.remove(src_file_path)

        # remove README.1st
        os.remove(os.path.join(os.path.dirname(src_file_path), "README.1st")) 
        # Remove temporary archive directory
        try:
            os.removedirs(os.path.dirname(src_file_path))
        except OSError, detail:
            Trace.log(e_errors.ERROR, "write_to_tape: error removind directory: %s"%(detail,))
            pass
        
            
        status_message = cache.messaging.mw_client.MWRArchived(orig_msg=rq)
        try:
            self._send_reply(status_message)
        except Exception, e:
            self.trace.exception("sending reply, exception %s", e)
            return False
        
        return True # completed successfully, the request will be acknowledged


    # check all conditions for purging the file
    def really_purge(self, f_info):
        if (f_info['status'][0] == e_errors.OK and
            (f_info['archive_status'] == file_cache_status.ArchiveStatus.ARCHIVED) and # file is on tape
            ((f_info['cache_status'] != file_cache_status.CacheStatus.STAGING) and   # why would we purge the file which is being staged?
             (f_info['cache_status'] != file_cache_status.CacheStatus.STAGING_REQUESTED) and   # why would we purge the file which is being staged?
             (f_info['cache_status'] != file_cache_status.CacheStatus.PURGING) and   # file is being pugred
             (f_info['cache_status'] != file_cache_status.CacheStatus.PURGED))):   # file is pugred
            if self.purge_watermarks:
                directory = f_info['cache_location']
                try:
                    stats = os.statvfs(directory)
                    avail = long(stats[statvfs.F_BAVAIL])*stats[statvfs.F_BSIZE]
                    total = long(stats[statvfs.F_BAVAIL])*stats[statvfs.F_BSIZE]*1.
                    fr_avail = avail/total
                    if fr_avail > 1 - self.purge_watermarks[1]:
                        rc = True
                    else:
                        rc = False
                except OSError:
                    rc = True # file was removed before, proceed with purge anyway
            else:
                rc = True
        else:
            rc = False
        Trace.trace(10, "really_purge %s %s"%(f_info['bfid'], rc))
        return rc

    # purge files from disk
    def purge_files(self, request):
        # save request
        rq = copy.copy(request)
        Trace.trace(10, "purge_files: request %s"%(rq.content,))
        request_list = rq.content['file_list']
        set_cache_params = []
        for component in request_list:
            bfid = component['bfid']
            # Convert unicode to ASCII strings.
            if type(bfid) == types.UnicodeType:
                bfid = bfid.encode("utf-8")

            rec = self.fcc.bfid_info(bfid)
            if self.really_purge(rec):
                rec['cache_status'] = file_cache_status.CacheStatus.PURGING
                Trace.trace(10, "purge_files: purging %s"%(rec,))
                set_cache_params.append({'bfid': bfid,
                                         'cache_status': file_cache_status.CacheStatus.PURGING,
                                         'archive_status': None,        # we are not changing this
                                         'cache_location': rec['cache_location']})       # we are not changing this

        rc = self.set_cache_status(set_cache_params)
        Trace.trace(10, "purge_files: set_cache_status 1 returned %s"%(rc,))
               
        for item in set_cache_params:
            try:
                Trace.trace(10, "purge_files: removing %s"%(item['cache_location'],))
                os.remove(item['cache_location'])
            except OSError, detail:
                if detail.args[0] != errno.ENOENT:
                    Trace.trace(10, "purge_files: can not remove %s: %s"%(item['cache_location'], detail))
                    Trace.log(e_errors.ERROR, "purge_files: can not remove %s: %s"%(item['cache_location'], detail))
            except Exception, detail:
                Trace.trace(10, "purge_files: can not remove %s: %s"%(item['cache_location'], detail))
                Trace.log(e_errors.ERROR, "purge_files: can not remove %s: %s"%(item['cache_location'], detail))
                
            try:
                os.removedirs(os.path.dirname(item['cache_location']))
                item['cache_status'] = file_cache_status.CacheStatus.PURGED
                Trace.log(e_errors.INFO, "purge_files: purged %s"%(item['cache_location'],))
            except OSError, detail:
                if detail.args[0] != errno.ENOENT:
                    Trace.log(e_errors.ERROR, "purge_files: error removind directory: %s"%(detail,))
                    Trace.trace(10, "purge_files: error removind directory: %s"%(detail,))
                else:
                    item['cache_status'] = file_cache_status.CacheStatus.PURGED
                    Trace.log(e_errors.INFO, "purge_files: purged %s"%(item['cache_location'],))
                    
            except Exception, detail:
                Trace.log(e_errors.ERROR, "purge_files: error removind directory: %s"%(detail,))

        rc = self.set_cache_status(set_cache_params)
        Trace.trace(10, "purge_files: set_cache_status 2 returned %s"%(rc,))

        status_message = cache.messaging.mw_client.MWRPurged(orig_msg=rq)
        try:
            self._send_reply(status_message)
        except Exception, e:
            self.trace.exception("sending reply, exception %s", e)
            return False
        
        return True # completed successfully, the request will be acknowledged

    # read aggregated file from tape
    def read_from_tape(self, request):
        # save request
        rq = copy.copy(request)
        Trace.trace(10, "read_from_tape: request %s"%(rq.content,))
        request_list = rq.content['file_list']
        # the request list must:
        # 1. Have a single component OR if NOT
        # 2. Have the same package bfid

        bfid_info = []
        files_to_stage = []
        if len(request_list) == 1:
            # check if the package staging is requested
            bfid = request_list[0]['bfid']
            if type(bfid) == types.UnicodeType:
                bfid = bfid.encode("utf-8")
            rec = self.fcc.bfid_info(bfid)
            Trace.trace(10, "read_from_tape: rec %s"%(rec,))

            if rec['status'][0] == e_errors.OK:
                package_id = rec.get('package_id', None)
                if package_id == bfid:
                    # request to stage a package
                    # get all information about children
                    rc = self.fcc.get_children(package_id)
                    Trace.trace(10, "read_from_tape, bfid_data %s"%(rc,))
                    if rc ['status'][0] != e_errors.OK:
                        Trace.log(e_errors.ERROR, "Package staging failed %s %s"%(package_id, rc ['status']))
                        return True # return True so that the message is confirmed
                    else:
                       bfid_info = rc['children'] 
                else: # single file
                    bfid_info.append(rec)
                package_name = rec['pnfs_name0']

            else:
                Trace.log(e_errors.ERROR, "Package staging failed %s %s"%(package_id, rec ['status']))
                return True # return True so that the message is confirmed
        else:
            package_id = None
            for component in request_list:
                bfid = component['bfid']
                # Convert unicode to ASCII strings.
                if type(bfid) == types.UnicodeType:
                    bfid = bfid.encode("utf-8")

                rec = self.fcc.bfid_info(bfid)
                if rec['status'][0] == e_errors.OK:
                    if not package_id:
                        # read the package id if the First file
                        package_id = rec.get('package_id', None)
                        if package_id:
                            package_info = self.fcc.bfid_info(package_id)
                            package_name = package_info['pnfs_name0']
                    if package_id != rec.get('package_id', None):
                        Trace.log(e_errors.ERROR,
                                  "File does not belong to the same package and will not be staged %s %s"%
                                  (rec['bfid'], rec['pnfs_name0']))
                    else:
                        bfid_info.append(rec)

        package_name = self.fcc.bfid_info(bfid) 
        set_cache_params = [] # this list is needed to send set_cache_status command to file clerk

        # Internal list of bfid data is built
        # Create a list of files to get staged
        for  component in bfid_info:
            bfid = component['bfid']
            Trace.trace(10, "read_from_tape: rec %s"%(component,))
            if component['archive_status'] == file_cache_status.ArchiveStatus.ARCHIVED:  # file is on tape and it can be staged
                # check the state of each file
                if component['cache_status'] == file_cache_status.CacheStatus.CACHED:
                    # File is in cache and available immediately.
                    # How could it get into a stage list anyway?
                    continue
                elif component['cache_status'] == file_cache_status.CacheStatus.STAGING_REQUESTED:
                    # file clerk sets this when opens a file
                    if component['bfid'] != package_id: # we stage files in the package, not the package itself
                        files_to_stage.append(component)
                        set_cache_params.append({'bfid': bfid,
                                                 'cache_status':file_cache_status.CacheStatus.STAGING,
                                                 'archive_status': None,        # we are not changing this
                                                 'cache_location': None})       # we are not changing this yet
                if component['cache_status'] == file_cache_status.CacheStatus.STAGING:
                    # File is being staged
                    # Log this for the further investigation in
                    # case the file was not staged.
                    Trace.log(e_errors.INFO, "File is being staged %s %s"%(component['bfid'], component['pnfs_name0']))
                    continue
                else:
                    continue
                    
        Trace.trace(10, "read_from_tape:  files to stage %s %s"%(len(files_to_stage), files_to_stage))
        if len(files_to_stage) != 0:
            #rc = self.fcc.set_cache_status(set_cache_params)
            Trace.trace(10, "read_from_tape: will stage %s"%(set_cache_params,))
            rc = self.set_cache_status(set_cache_params)
            if rc['status'][0] != e_errors.OK:
                Trace.log(e_errors.ERROR, "Package staging failed %s %s"%(package_id, rc ['status']))
                return True # return True so that the message is confirmed
            
            for rec in files_to_stage:
                # create a temporary directory for staging a package
                # use package name for this
                stage_fname = os.path.basename(package_name['pnfs_name0'])
                # file name looks like:
                # /pnfs/fs/usr/data/moibenko/d2/LTO3/.package-2011-07-01T09:41:46.0Z.tar
                tmp_stage_dirname = "".join((".",stage_fname.split(".")[1]))
                tmp_stage_dir_path = os.path.join(self.tmp_stage_area, tmp_stage_dirname)
                if not os.path.exists(tmp_stage_dir_path):
                    try:
                        os.makedirs(tmp_stage_dir_path)
                    except:
                        pass
                tmp_stage_file_path = os.path.join(tmp_stage_dir_path, stage_fname)
            
            # now stage the package file
            if not os.path.exists(tmp_stage_file_path):
                # stage file from tape if it does not exist
                encp = encp_wrapper.Encp()
                args = ["encp", "--skip-pnfs", "--get-bfid", package_id, tmp_stage_file_path]  
                Trace.trace(10, "read_from_tape: sending %s"%(args,))
                encp = encp_wrapper.Encp()

                rc = encp.encp(args)
                Trace.trace(10, "read_from_tape: encp returned %s"%(rc,))
                if rc != 0:
                    # cleanup dirctories
                    try:
                        os.removeedirs(tmp_stage_dir_path)
                    except:
                        pass

                    # change cache_status back
                    for rec in set_cache_params:
                        rec['cache_status'] = file_cache_status.CacheStatus.PURGED
                    #rc = self.fcc.set_cache_status(set_cache_params)
                    rc = self.set_cache_status(set_cache_params)
                    return False

            # unpack files
            os.chdir(tmp_stage_dir_path)
            #if len(files_to_stage) > 1:
            # untar packaged files
            # if package contains more than one file
            os.system("tar --force-local -xf %s"%(stage_fname,))

            # clear set_cache_params
            set_cache_params = []
            
            # move files to their original location
            for rec in files_to_stage:
                src = rec.get('location_cookie', None)

                if not self.stage_area:
                    # file gets staged into the path
                    # defined by location_cookie
                    dst = src
                src = src.lstrip("/")
                if self.stage_area:
                    # file gets staged into the path
                    # defined by location_cookie
                    # prepended by stage_area
                    dst = os.path.join(self.stage_area, src)
                Trace.trace(10, "read_from_tape: src %s s_a %s dst %s"%(src,self.stage_area, dst)) 
                Trace.trace(10, "read_from_tape: d_a %s a_a %s s_a %s t_s_a %s"%(self.data_area,
                                                                                 self.archive_area,
                                                                                 self.stage_area,
                                                                                 self.tmp_stage_area))
                Trace.trace(10, "read_from_tape: renaming %s to %s"%(src, dst))
                # create a destination directory
                if not os.path.exists(os.path.dirname(dst)):
                    try:
                        Trace.trace(10, "read_from_tape creating detination directory %s for %s"%(os.path.dirname(dst), dst))
                        os.makedirs(os.path.dirname(dst))
                    except Exception, detail:
                        Trace.log(e_errors.ERROR, "Package staging failed %s %s"%(package_id, detail))
                        return False
                
                os.rename(src, dst)
                set_cache_params.append({'bfid': rec['bfid'],
                                         'cache_status':file_cache_status.CacheStatus.CACHED,
                                             'archive_status': None,        # we are not changing this
                                             'cache_location': dst})
            
            #rc = self.fcc.set_cache_status(set_cache_params)
            rc = self.set_cache_status(set_cache_params)
            if rc['status'][0] != e_errors.OK:
                Trace.log(e_errors.ERROR, "Package staging failed %s %s"%(package_id, rc ['status']))
                return True # return True so that the message is confirmed

            # remove the rest (README.1st)
            Trace.trace(10, "read_from_tape: current dir %s"%(os.getcwd(),))
            rc = enstore_functions2.shell_command("rm -rf *")
            rc = enstore_functions2.shell_command("rm -rf .*")
            # the following files are created
            # -rw-r--r-- 1 root root      0 Jul 12 11:29 .(use)(1)(.package-2011-07-12T11:03:51.0Z.tar)
            # -rw-r--r-- 1 root root      0 Jul 12 11:29 .(use)(2)(.package-2011-07-12T11:03:51.0Z.tar)
            # -rw-r--r-- 1 root root      0 Jul 12 11:29 .(use)(3)(.package-2011-07-12T11:03:51.0Z.tar)
            # -rw-r--r-- 1 root root      0 Jul 12 11:29 .(use)(4)(.package-2011-07-12T11:03:51.0Z.tar)
            

            #remove the temporary directory
            os.removedirs(tmp_stage_dir_path)
        status_message = cache.messaging.mw_client.MWRStaged(orig_msg=rq)
        try:
            self._send_reply(status_message)
        except Exception, e:
            self.trace.exception("sending reply, exception %s", e)
            return False

        return True

    # unpack aggregated file
    def unpack_files(self):
        pass

    # return migrator status
    def migrator_status(self):
        return self.status
    


    workers = {mt.MWC_ARCHIVE: write_to_tape,
               mt.MWC_PURGE:   purge_files,
               mt.MWC_STAGE:   read_from_tape,
               }

    # handle all types of requests
    def handle_request(self, message):
        Trace.trace(10, "handle_request received: %s"%(message))
        if self.work_dict.has_key(message.correlation_id):
            # the work on this request is in progress
            return False
        self.work_dict[message.correlation_id] = message
        # prepare work:
        request_type = message.properties["en_type"]
        if request_type in (mt.MWC_ARCHIVE, mt.MWC_PURGE, mt.MWC_STAGE):
            if request_type == mt.MWC_ARCHIVE:
                self.status = file_cache_status.ArchiveStatus.ARCHIVING
            elif request_type == mt.MWC_PURGE:
                self.status = file_cache_status.CacheStatus.PURGING
            elif request_type ==mt.MWC_STAGE:
                self.status = file_cache_status.CacheStatus.STAGING
            confirmation_message = cache.messaging.mw_client.MWRConfirmation(orig_msg=message,
                                                                             content=message.content,
                                                                             reply_to=self.queue_in_name,
                                                                             correlation_id=message.correlation_id)
            # reply now to report name of the queue for inquiry commands
            # such as MWC_STATUS
            try:
                Trace.trace(10, "Sending confirmation")
                self._send_reply(confirmation_message)
            except Exception, e:
                self.trace.exception("sending reply, exception %s", e)
                return False
            # run worker
            if self.workers[request_type](self, message):
                # work has completed successfully
                del(self.work_dict[message.correlation_id])
                if request_type == mt.MWC_ARCHIVE:
                    self.status = file_cache_status.ArchiveStatus.ARCHIVED
                elif request_type == mt.MWC_PURGE:
                    self.status = file_cache_status.CacheStatus.PURGED
                elif request_type ==mt.MWC_STAGE:
                    self.status = file_cache_status.CacheStatus.CACHED
                    
                return True
            else:
                return False
            
        elif request_type in (mt.MWC_STATUS): # there could more reuquest of such nature in the future
            content = {"migrator_status": self.status, "name": self.name} # this may need more details
            status_message = cache.messaging.mw_client.MWRStatus(orig_msg=message,
                                                                 content= content)
            try:
                Trace.trace(10, "Sending status %s"%(status_message,))
                self._send_reply(status_message)
            except Exception, e:
                self.trace.exception("sending reply, exception %s", e)
                return False
            # work has completed successfully
            del(self.work_dict[message.correlation_id])
            return True
        return True

    """
    Leave these here so far, as there may be an argument for have a separate handler for eact type of the message
     def handle_write_to_tape(self, message):
        Trace.trace(10, "handle_write_to_tape received: %s"%(message))
        if self.work_dict.has_key(message.correlation_id):
            # the work on this request is in progress
            return False
        self.work_dict[message.correlation_id] = message
        self.status = file_cache_status.ArchiveStatus.ARCHIVING
        # conifrm receipt of request
        confirmation_message = cache.messaging.mw_client.MWRConfirmation(orig_msg=message, content=message.content, reply_to=self.queue_in_name)
        self.trace.debug("WORKER reply=%s",confirmation_message)
        try:
            self._send_reply(confirmation_message)
            self.trace.debug("worker_purge() reply sent, reply=%s", confirmation_message)
        except Exception, e:
            self.trace.exception("worker_purge(), sending reply, exception")         
        if self.write_to_tape(message.content):
            # work has completed successfully
            del(self.work_dict[message.correlation_id])
            self.status = file_cache_status.ArchiveStatus.ARCHIVED
            return True
        else:
            return False

    def handle_purge(self, message):
        Trace.trace(10, "handle_purge received: %s"%(message))
        if self.work_dict.has_key(message.correlation_id):
            # the work on this request is in progress
            return False
        self.work_dict[message.correlation_id] = message
        self.status = file_cache_status.CacheStatus.PURGING

        # conifrm receipt of request
        confirmation_message = cache.messaging.mw_client.MWRConfirmation(orig_msg=message, content=message.content, reply_to=self.queue_in_name)
        self.trace.debug("WORKER reply=%s",confirmation_message)
        try:
            self._send_reply(confirmation_message)
            self.trace.debug("worker_purge() reply sent, reply=%s", confirmation_message)
        except Exception, e:
            self.trace.exception("worker_purge(), sending reply, exception")         
        if self.purge_files(message.content):
            # work has completed successfully
            del(self.work_dict[message.correlation_id])
            self.status = file_cache_status.CacheStatus.PURGED
            return True
        else:
            return False
            
        
    def handle_stage_from_tape(self, message):
        Trace.trace(10, "handle_stage_from_tape received: %s"%(message))
        if self.work_dict.has_key(message.correlation_id):
            # the work on this request is in progress
            return False
        self.work_dict[message.correlation_id] = message
        self.status = file_cache_status.CacheStatus.STAGING
        # conifrm receipt of request
        confirmation_message = cache.messaging.mw_client.MWRConfirmation(orig_msg=message, content=message.content, reply_to=self.queue_in_name)
        self.trace.debug("WORKER reply=%s",confirmation_message)
        try:
            self._send_reply(confirmation_message)
            self.trace.debug("worker_purge() reply sent, reply=%s", confirmation_message)
        except Exception, e:
            self.trace.exception("worker_purge(), sending reply, exception")         
        if self.read_from_tape(message.content):
            # work has completed successfully
            del(self.work_dict[message.correlation_id])
            self.status = file_cache_status.CacheStatus.CACHED
            return True
        else:
            return False
    
    # handle status command from migration dispatcher
    def handle_status(self, message):
        Trace.trace(10, "handle_status received: %s"%(message))
        self.work_dict[message.correlation_id] = message
        status_message = cache.messaging.mw_client.MWRStatus(orig_msg=message, content={"migrator_status": self.status, "name": self.name})
        self.trace.debug("WORKER status reply=%s", status_message)
        try:
            self._send_reply(status_message)
            self.trace.debug("worker_purge() reply sent, reply=%s", status_message)
        except Exception, e:
            self.trace.exception("worker_purge(), sending reply, exception")         

            # work has completed successfully
            del(self.work_dict[message.correlation_id])
            self.status = file_cache_status.CacheStatus.CACHED
            return True
        else:
            return False
    """
        
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

    # create  Migrator instance
    migrator = Migrator(intf.name, (intf.config_host, intf.config_port))
    migrator.handle_generic_commands(intf)

    #Trace.init(migrator.log_name, 'yes')

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
