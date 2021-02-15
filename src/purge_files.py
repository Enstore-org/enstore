#!/usr/bin/env python

##############################################################################
#
#
# Make decision on purging cached files
##############################################################################
# system import
from __future__ import print_function
import sys
import os
import time
import itertools

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
# Positioning parameres in bfid info
P_BFID = 0
P_CACHE_STATUS = 1
P_ARCHIVE_STATUS = 2
P_CACHE_MOD_TIME = 3
P_CACHE_LOCATION = 4
P_LOCATION_COOKIE = 5
P_LIBRARY = 6
DB_CONNECTION_TO = 10
DB_CONNECTION_RETRY = 60


class FilePurger:
    # init is stole from file_clerk
    def __init__(self, csc, max_time_in_cache=None,
                 watermarks=None, libraries=None):
        # Obtain information from the configuration server.
        self.csc = csc
        try:
            dbInfo = self.csc.get('database')
            dbHome = dbInfo['db_dir']
            try:  # backward compatible
                jouHome = dbInfo['jou_dir']
            except BaseException:
                jouHome = dbHome
        except BaseException:
            dbHome = os.environ['ENSTORE_DIR']
            jouHome = dbHome

        if max_time_in_cache:
            self.max_time_in_cache = max_time_in_cache
        else:
            self.max_time_in_cache = MAX_TIME_IN_CACHE

        # purge_watermarks is a tuple
        # (start_purging_disk_avalable, start_purging_disk_avalable)
        # start_purging_disk_avalable - available space as afraction of the
        # capacity
        self.purge_watermarks = watermarks

        self.libraries = None
        if libraries:
            # libraries to use for read purge query for clustered systems
            self.libraries = itertools.cycle(libraries)

        ic_conf = self.csc.get("info_server")
        self.infoc = info_client.infoClient(self.csc,
                                            server_address=(ic_conf['host'],
                                                            ic_conf['port']))
        # Try to connect to enstore DB.
        # This is needed in case when computer running enstore DB is
        # restarting.
        retry_cnt = 0
        for retry_cnt in range(DB_CONNECTION_RETRY):
            # Open conection to the Enstore DB.
            try:
                # proper default values are supplied by edb.FileDB constructor
                self.filedb_dict = edb.FileDB(host=dbInfo.get('db_host', None),
                                              port=dbInfo.get('db_port', None),
                                              user=dbInfo.get('dbuser', None),
                                              database=dbInfo.get('dbname', None))
                Trace.log(e_errors.INFO, "Connected to enstore DB")
                break
            except BaseException:
                exc_type, exc_value = sys.exc_info()[:2]
                message = str(exc_type) + ' ' + str(exc_value) + \
                    ' IS POSTMASTER RUNNING?'
                Trace.log(e_errors.WARNING, "%s. Re-trying" % (message,))
                time.sleep(DB_CONNECTION_TO)
        else:
            print("CAN NOT ESTABLISH DATABASE CONNECTION ... QUIT!")
            sys.exit(1)

    # check available space
    # checks if disk space is beween watermarks
    # and if yes returns True
    def available_space_low(self, f_info):
        directory = os.path.dirname(f_info[4])
        Trace.trace(10, "available_space_low: %s" % (directory,))
        try:
            stats = os.statvfs(directory)
            avail = float(stats.f_bavail)
            total = float(stats.f_blocks)
            fr_avail = avail / total
            Trace.trace(
                10, "available_space_low: %s %s" %
                (fr_avail, self.purge_watermarks[0],))
            if fr_avail < 1 - self.purge_watermarks[0]:
                return True
            else:
                return False
        except OSError:
            return False
        except BaseException:
            return False

    def purge_this_file(self, f_info, check_watermarks=True):
        # f_info is a list:
        # f_info[0] - bfid
        # f_info[1] - cache_status
        # f_info[2] - archive_status
        # f_info[3] - cache_mod_time
        # f_info[4] - cache_location
        # f_info[5] - location_cookie
        # f_info[6] - disk library (not used here)

        rc = False
        Trace.trace(10, "purge_this_file: bfid %s CACHE_STATUS %s ARCHIVE_STATUS %s CACHE_MOD_TIME %s max_t %s" %
                    (f_info[P_BFID],
                     f_info[P_CACHE_STATUS],
                     f_info[P_ARCHIVE_STATUS],
                     f_info[P_CACHE_MOD_TIME],
                     self.max_time_in_cache))
        if (f_info[P_CACHE_STATUS] == file_cache_status.CacheStatus.CACHED and
                f_info[P_ARCHIVE_STATUS] == file_cache_status.ArchiveStatus.ARCHIVED):
            # check how long is this file in cache
            t = time.mktime(
                time.strptime(
                    f_info[P_CACHE_MOD_TIME],
                    "%Y-%m-%d %H:%M:%S"))
            if time.time() - t > self.max_time_in_cache:
                rc = True
                if f_info[P_CACHE_LOCATION] == f_info[P_LOCATION_COOKIE]:  # same location
                    # When cache_location and location_cookie
                    # the same cache pool configured for writes and reads.
                    # we want to clean this pool,
                    # but the final decision will be done by migrator.
                    Trace.trace(
                        10, "purge_this_file: same location %s %s" %
                        (f_info[P_BFID], f_info[P_CACHE_LOCATION]))
                    return True
                if check_watermarks and self.purge_watermarks:
                    rc = self.available_space_low(f_info)

        return rc

    def files_to_purge_with_query(self, query, check_watermarks=True):
        Trace.trace(10, "files_to_purge_with_query command. %s" % (query,))
        res = self.filedb_dict.query_getresult(query)
        if not res:
            return []
        total_in_cache = len(res)
        Trace.trace(
            10, "files_to_purge_with_query in cache. %s" %
            (total_in_cache,))
        libraries = {}
        purge_candidates = []
        list_of_file_lists = []
        total_purge_candidates = 0
        for f_info in res:
            if not f_info[P_LIBRARY] in libraries.keys():
                libraries[f_info[P_LIBRARY]] = {'per_lib_purge_candidates': 0,
                                                'purge_candidates': []
                                                }
            if self.purge_this_file(f_info, check_watermarks):
                libraries[f_info[P_LIBRARY]]['purge_candidates'].append(
                    f_info[P_BFID])
                libraries[f_info[P_LIBRARY]]['per_lib_purge_candidates'] += 1
        for library in libraries.keys():
            f_counter = 0
            l_counter = 0
            per_lib_purge_candidates = libraries[library]['per_lib_purge_candidates']
            Trace.trace(
                10, "files_to_purge_with_query cand. for %s %s" %
                (library, per_lib_purge_candidates,))
            while f_counter < per_lib_purge_candidates:
                # create a list of files to purge
                if l_counter == 0:
                    # create list of elements with conatant size 500 or
                    # total_purge_candidates
                    l_size = min(per_lib_purge_candidates, 500)
                    # create a list of files to purge
                    list_id = "%s_%s" % (int(time.time()), f_counter)
                    f_list = file_list.FileList(id=list_id,
                                                list_type=mt.MDC_PURGE,
                                                list_name="Purge_%s" % (
                                                    list_id),
                                                disk_library=library)
                purge_candidates = libraries[library]['purge_candidates']
                while l_counter < l_size:
                    if f_counter < per_lib_purge_candidates:
                        list_element = file_list.FileListItem(bfid=purge_candidates[f_counter],
                                                              nsid="",
                                                              path="",
                                                              libraries=[])
                        f_list.append(list_element)
                        l_counter += 1
                        f_counter += 1
                    else:
                        break

                f_list.full = True
                l_counter = 0
                list_of_file_lists.append(f_list)

            total_purge_candidates += per_lib_purge_candidates

        Trace.log(e_errors.INFO, "Total %s Purge Candidates %s" %
                  (total_in_cache, total_purge_candidates))

        return list_of_file_lists

    def files_to_purge(self, check_watermarks=True):
        WRITE_QUERY = """
        SELECT f.bfid,
               f.cache_status,
               f.archive_status,
               f.cache_mod_time,
               f.cache_location,
               f.location_cookie,
               v.library
        FROM file f,
             volume v,
             cached_files cf
        WHERE f.bfid=cf.bfid
           AND f.volume=v.id
           AND f.cache_status='CACHED'
           AND f.archive_status='ARCHIVED'
           AND f.cache_mod_time < CURRENT_TIMESTAMP - interval '{}'
           AND f.cache_location=f.location_cookie
        ORDER BY f.cache_mod_time asc limit 1000
        """
        QUERY_BY_LIBRARY = """
        SELECT f.bfid,
               f.cache_status,
               f.archive_status,
               f.cache_mod_time,
               f.cache_location,
               f.location_cookie,
               v.library
        FROM file f,
             volume v,
             cached_files cf
        WHERE f.bfid=cf.bfid
           AND f.volume=v.id
           AND f.cache_status='CACHED'
           AND f.archive_status='ARCHIVED'
           AND f.cache_mod_time < CURRENT_TIMESTAMP - interval '{}'
           AND f.cache_location!=f.location_cookie
           AND v.library='{}'
        ORDER by f.cache_mod_time asc limit 1000
        """
        QUERY_NO_LIBRARY = """
        SELECT f.bfid,
               f.cache_status,
               f.archive_status,
               f.cache_mod_time,
               f.cache_location,
               f.location_cookie,
               v.library
        FROM file f,
             volume v,
             cached_files cf
        WHERE f.bfid=cf.bfid
           AND f.volume=v.id
           AND f.cache_status='CACHED'
           AND f.archive_status='ARCHIVED'
           AND f.cache_mod_time < CURRENT_TIMESTAMP - interval '{}'
           AND f.cache_location!=f.location_cookie
        ORDER by f.cache_mod_time asc limit 1000
        """

        Trace.trace(10, "files_to_purge")
        # comment for queries:
        # The limit 1000 was found empirically.
        # For bigger number of total purge candidates
        # something happens when putting requests in qpid queue.
        # Qpid shows that messages are in the queue, but they do not get fetched
        # by purge migrators.

        # comment for write_q:
        # To check that file is in write cache compare cache_location and location_cookie.
        # If they are the same the file is in write cache.

        write_q = WRITE_QUERY.format(self.max_time_in_cache)

        # comment for read_q:
        # To check that file is in read cache compare cache_location and location_cookie.
        # If they are different the file is in read cache.

        if self.libraries:
            # pick library from the circular list
            library = next(self.libraries)
            Trace.trace(10, "files_to_purge: library to query %s" % (library,))
            read_q = QUERY_BY_LIBRARY.format(self.max_time_in_cache, library)
        else:
            read_q = QUERY_NO_LIBRARY.format(self.max_time_in_cache)

        write_purge_list = self.files_to_purge_with_query(
            write_q, check_watermarks)
        Trace.trace(10, "write query returned %s" % (write_purge_list,))
        read_purge_list = self.files_to_purge_with_query(
            read_q, check_watermarks)
        Trace.trace(10, "read query returned %s" % (read_purge_list,))
        return write_purge_list + read_purge_list


if __name__ == "__main__":
    import getopt
    short_opt = "hp"
    long_opt = ["help", "purge"]

    def usage(name):
        print("Usage: %s [options] [<library list>]" % (name,))
        print("\t -p [--purge]           : purge files")
        print("\t -h [--help]            : show this message")

    try:
        opts, args = getopt.getopt(sys.argv[1:], short_opt, long_opt)
    except BaseException:
        usage(sys.argv[0])
        sys.exit(-1)

    purge = False
    try:
        for o, a in opts:
            if o in ["-p", "--purge"]:
                purge = True
            else:
                usage(sys.argv[0])
                sys.exit(-1)
    except BaseException:
        usage(sys.argv[0])
        sys.exit(-1)

    config_host = os.getenv('ENSTORE_CONFIG_HOST')
    config_port = int(os.getenv('ENSTORE_CONFIG_PORT'))
    csc = configuration_client.ConfigurationClient((config_host, config_port))
    Trace.init("PURGER", "yes")

    libs = args or None

    purge_watermarks = None
    max_time_in_cache = MAX_TIME_IN_CACHE
    if purge:
        # get watermarks
        disp = csc.get("dispatcher")
        if disp:
            purge_watermarks = disp.get("purge_watermarks")
            max_time_in_cache = disp.get(
                "max_time_in_cache", MAX_TIME_IN_CACHE)

            # connect to qpid queues
            import cache.messaging.client as qpid_client
            import cache.messaging.md_client as md_client
            amqp_broker_dict = csc.get("amqp_broker")
            pe_queue = "%s; {create: always}" % (disp['queue_work'],)
            reply_queue = "%s; {create: always}" % (disp['queue_reply'],)
            en_qpid_client = qpid_client.EnQpidClient((amqp_broker_dict['host'],
                                                       amqp_broker_dict['port']),
                                                      reply_queue,
                                                      pe_queue)
            en_qpid_client.start()

    fp = FilePurger(csc, max_time_in_cache, purge_watermarks, libs)
    #fp = FilePurger(csc,max_time_in_cache, None, libs)

    fc = fp.files_to_purge()
    if fc and purge:
        for item in fc:
            md_purge_command = md_client.MDCPurge(item.file_list)
            md_purge_command.properties["disk_library"] = item.disk_library
            md_purge_command.properties["list_id"] = item.list_id
            md_purge_command.properties["list_name"] = item.list_name
            Trace.trace(10, "Sending %s" % (md_purge_command,))
            en_qpid_client.send(md_purge_command)

    sys.exit(0)
