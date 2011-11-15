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

# enstore imports
import configuration_client
import edb
import info_client
import e_errors
import file_cache_status
import cache.messaging.file_list as file_list
from cache.messaging.messages import MSG_TYPES as mt

MAX_TIME_IN_CACHE = 3600

class FilePurger:
    # init is stole from file_clerk
    def  __init__(self, csc, max_time_in_cache=None):
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

    def purge_this_file(self, f_info):
        rc = False
        if (f_info['cache_status'] == file_cache_status.CacheStatus.CACHED and
            f_info['archive_status'] == file_cache_status.ArchiveStatus.ARCHIVED):
            # check how long is this file in cache
            t = time.mktime(time.strptime(f_info['cache_mod_time'], "%Y-%m-%d %H:%M:%S"))
            if time.time() - t > self.max_time_in_cache:
                rc = True
        return rc
            
    def files_to_purge(self):
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
                if self.purge_this_file(f_info):
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
        print "Total %s Purge Candidates %s"%(total_in_cache, total_purge_candidates)
        
        return f_list
    
if __name__ == "__main__":
    # get a file purger
    config_host = os.getenv('ENSTORE_CONFIG_HOST')
    config_port = int(os.getenv('ENSTORE_CONFIG_PORT'))
    csc = configuration_client.ConfigurationClient((config_host, config_port))
    fp = FilePurger(csc)
    fc = fp.files_to_purge()
    print fc
