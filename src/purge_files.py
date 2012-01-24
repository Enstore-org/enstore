#!/usr/bin/env python

##############################################################################
#
# $Id$
#
# Make decision on purging cached files
##############################################################################
# system import
import sys
import os
import time
import statvfs

# enstore imports
import configuration_client
import edb
import info_client
import e_errors
import file_cache_status
import Trace
import cache.messaging.file_list as file_list
from cache.messaging.messages import MSG_TYPES as mt

MAX_TIME_IN_CACHE = 3600

class FilePurger:
    # init is stole from file_clerk
    def  __init__(self, csc, max_time_in_cache=None, watermarks=None):
        # Obtain information from the configuration server.
        self.csc = csc
        configuration_client.ConfigurationClient(csc)
        try:
            dbInfo = self.csc.get('database')
            dbHome = dbInfo['db_dir']
            try:  # backward compatible
                jouHome = dbInfo['jou_dir']
            except:
                jouHome = dbHome
        except:
            dbHome = os.environ['ENSTORE_DIR']
            jouHome = dbHome


        if max_time_in_cache:
            self.max_time_in_cache = max_time_in_cache
        else:
            self.max_time_in_cache = MAX_TIME_IN_CACHE

        # purge_watermarks is a tuple
        # (start_purging_disk_avalable, start_purging_disk_avalable)
        # start_purging_disk_avalable - available space as afraction of the capacity
        self.purge_watermarks = watermarks
        
        ic_conf = self.csc.get("info_server")
        self.infoc = info_client.infoClient(self.csc, 
                                            server_address=(ic_conf['host'],
                                                            ic_conf['port']))
        #Open conection to the Enstore DB.
        try:
            # proper default values are supplied by edb.FileDB constructor
            self.filedb_dict = edb.FileDB(host=dbInfo.get('db_host',None),
                                          port=dbInfo.get('db_port',None),
                                          user=dbInfo.get('dbuser',None),
                                          database=dbInfo.get('dbname',None))
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            message = str(exc_type)+' '+str(exc_value)+' IS POSTMASTER RUNNING?'
            print message
            print "CAN NOT ESTABLISH DATABASE CONNECTION ... QUIT!"
            sys.exit(1)

    # check available space
    # checks if disk space is beween watermarks
    # and if yes returns True
    def available_space_low(self, f_info):
        directory = f_info['cache_location']
        try:
            stats = os.statvfs(directory)
            avail = long(stats[statvfs.F_BAVAIL])*stats[statvfs.F_BSIZE]
            total = long(stats[statvfs.F_BAVAIL])*stats[statvfs.F_BSIZE]*1.
            fr_avail = avail/total
            if fr_avail < 1 - self.purge_watermarks[0]:
                return True
            else:
                return False
        except OSError:
            return False
        except:
            return False
            
            

    def purge_this_file(self, f_info, check_watermarks=True):
        rc = False
        if (f_info['cache_status'] == file_cache_status.CacheStatus.CACHED and
            f_info['archive_status'] == file_cache_status.ArchiveStatus.ARCHIVED):
            # check how long is this file in cache
            t = time.mktime(time.strptime(f_info['cache_mod_time'], "%Y-%m-%d %H:%M:%S"))
            if time.time() - t > self.max_time_in_cache:
                rc = True
                if f_info['cache_location'] == f_info['location_cookie']: # same location
                    # When cache_location and location_cookie
                    # the same cache pool configured for writes and reads.
                    # we want to clean this pool,
                    # but the final decision will be done by migrator.
                    Trace.trace(10, "purge_this_file: same location %s %s"%(f_info['bfid'], f_info['cache_location']))
                    return True
                if check_watermarks and self.purge_watermarks:
                    rc = self.available_space_low(f_info)
        return rc
            

    def files_to_purge(self, check_watermarks=True):
        q = "select * from cached_files;"
        res = self.filedb_dict.query_getresult(q)
        total_in_cache = 0L
        total_purge_candidates = 0L
        purge_candidates = []
        f_list = None
        for bfid in res:
            f_info = self.infoc.bfid_info(bfid[0])
            if f_info['status'][0] == e_errors.OK:
                total_in_cache = total_in_cache + 1
                if self.purge_this_file(f_info, check_watermarks):
                    purge_candidates.append(f_info)
                    total_purge_candidates = total_purge_candidates + 1
        if total_purge_candidates:
            # create a list of files to purge
            list_id = "%s"%(int(time.time()),)
            f_list = file_list.FileList(id = list_id,
                                        list_type = mt.MDC_PURGE,
                                        list_name = "Purge")
            for f in purge_candidates:
                list_element = file_list.FileListItem(bfid = f['bfid'],
                                                      nsid = f['pnfsid'],
                                                      path = f['pnfs_name0'],
                                                      libraries = [f['library']])
                f_list.append(list_element)

            f_list.full = True
        Trace.trace(10, "Total %s Purge Candidates %s"%(total_in_cache, total_purge_candidates))
        
        return f_list
    
if __name__ == "__main__":
    # get a file purger
    config_host = os.getenv('ENSTORE_CONFIG_HOST')
    config_port = int(os.getenv('ENSTORE_CONFIG_PORT'))
    csc = configuration_client.ConfigurationClient((config_host, config_port))
    fp = FilePurger(csc)
    fc = fp.files_to_purge()
    print fc
