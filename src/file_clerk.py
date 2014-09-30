#!/usr/bin/env python

##############################################################################
#
# $Id$
#
##############################################################################

# system import

import Queue
import multiprocessing
import os
import select
import socket
import string
import sys
import threading
import time
import types


# enstore imports
import traceback
import callback
import dispatching_worker
import generic_server
import monitored_server
import enstore_constants
import edb
import Trace
import e_errors
import configuration_client
import hostaddr
import event_relay_messages
import enstore_functions3
import udp_server
import file_cache_status
import enstore_files
import enstore_functions2

SEQUENTIAL_QUEUE_SIZE=enstore_constants.SEQUENTIAL_QUEUE_SIZE
PARALLEL_QUEUE_SIZE=enstore_constants.PARALLEL_QUEUE_SIZE
MY_NAME = enstore_constants.FILE_CLERK
MAX_CONNECTION_FAILURE = enstore_constants.MAX_CONNECTION_FAILURE
MAX_THREADS = enstore_constants.MAX_THREADS

# we have run into issues with file_clerk locking up
# when number of max threads was more than number of max
# connections. Set it to max number of threads + 1 (counting
# main thread)

MAX_CONNECTIONS=MAX_THREADS+1
AMQP_BROKER = "amqp_broker"
FILES_IN_TRANSITION_CHECK_INTERVAL = enstore_constants.FILES_IN_TRANSITION_CHECK_INTERVAL

SELECT_FILES_IN_TRANSITION="""
SELECT f.bfid,
       f.cache_status,
       f.cache_mod_time
FROM file f,
files_in_transition fit
WHERE f.bfid=fit.bfid
   AND f.archive_status IS NULL
   AND f.cache_status='CACHED'
   AND f.deleted='n'
   AND f.cache_mod_time < CURRENT_TIMESTAMP - interval '1 day'
"""

SELECT_ALL_FILES_IN_TRANSITION="""
SELECT f.bfid,
       f.cache_status,
       f.cache_mod_time
FROM file f,
     files_in_transition fit
WHERE f.bfid=fit.bfid
   AND f.archive_status IS NULL
   AND f.cache_status='CACHED'
   AND f.deleted='n'
"""

isSFA=False

try:
    import cache.messaging.client as qpid_client
    try:
        import cache.messaging.pe_client as pe_client
        isSFA=True
    except ImportError, msg:
        Trace.log(e_errors.INFO,"Failed to import cache.messaging.pe_client: %s"%(str(msg),))
	pass
    try:
        import cache.en_logging.en_logging
    except ImportError, msg:
        Trace.log(e_errors.INFO,"Failed to import cache.en_logging.en_logging: %s"%(str(msg),))
	pass
except ImportError, msg:
    Trace.log(e_errors.INFO,"Failed to import cache.messaging.client: %s"%(str(msg),))
    pass

# time2timestamp(t) -- convert time to "YYYY-MM-DD HH:MM:SS"
# copied from migrate.py
def time2timestamp(t):
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))


class FileClerkInfoMethods(dispatching_worker.DispatchingWorker):
    ### This class of File Clerk methods should only be readonly operations.
    ### This class is inherited by Info Server (to increase code reuse)
    ### and we don't want the Info Server to have the ability to modify
    ### anything.  Also, any privledged/admin inquiries should not go
    ### here either.

    def  __init__(self, csc):
        # Obtain information from the configuration server.
        self.csc = configuration_client.ConfigurationClient(csc)
        self.keys = self.csc.get(MY_NAME) #wait forever???
        if not e_errors.is_ok(self.keys):
            message = "Unable to acquire configuration info for %s: %s: %s" % \
                      (MY_NAME, self.keys['status'][0], self.keys['status'][1])
            Trace.log(e_errors.ERROR, message)
            sys.exit(1)

        #Setup the ability to handle requests.
        dispatching_worker.DispatchingWorker.__init__(
            self, (self.keys['hostip'], self.keys['port']))

    ####################################################################


    # These extract value functions are used to get a value from the ticket
    # and perform validity checks in a consistant fashion.  These functions
    # duplicated in volume_clerk.py; they should be made more generic to
    # eliminate maintaining two sets of identical code.

    def extract_value_from_ticket(self, key, ticket, fail_None = False):
        try:
            value = ticket[key]
        except KeyError, detail:
            message =  "%s: key %s is missing" % (MY_NAME, detail)
            ticket["status"] = (e_errors.KEYERROR, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            return None

        if fail_None and value == None:
            message =  "%s: key %s is None" % (MY_NAME, key,)
            ticket["status"] = (e_errors.KEYERROR, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            return None

        return value

    def extract_bfid_from_ticket(self, ticket, key = "bfid",
                                 check_exists = True):

        return_record = False
        if hasattr(self, "filedb_dict"):
            return_record = True

        bfid = self.extract_value_from_ticket(key, ticket, fail_None = True)
        if not bfid:
            if return_record:
                #extract_value_from_ticket handles its own errors.
                return None, None
            else:
                return None

        #Check bfid format.
        if not enstore_functions3.is_bfid(bfid):
            message = "%s: bfid %s not valid" % (MY_NAME, bfid,)
            ticket["status"] = (e_errors.WRONG_FORMAT, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            if return_record:
                return None, None
            else:
                return None

        if check_exists and return_record:
            #Make sure the bfid exists.   (getattr() keeps pychecker quite.)
	    record = getattr(self, 'filedb_dict', {})[bfid]
            if not record:
                message = "%s: no such bfid %s" % (MY_NAME, bfid,)
                ticket["status"] = (e_errors.NO_FILE, message)
                Trace.log(e_errors.ERROR, message)
                self.reply_to_caller(ticket)
                return None, None

        if check_exists and return_record:
            return bfid, record
        else:
            return bfid

    def extract_external_label_from_ticket(self, ticket,
                                           key = "external_label",
                                           check_exists = True):

        return_record = False
        if hasattr(self, "volumedb_dict"):
            return_record = True

        external_label = self.extract_value_from_ticket(key, ticket,
                                                        fail_None = True)
        if not external_label:
            if return_record:
                #extract_value_from_ticket handles its own errors.
                return None, None
            else:
                return None

        #Check volume/external_label format.
        if not enstore_functions3.is_volume(external_label):
            message = "%s: external_label %s not valid" \
                      % (MY_NAME, external_label,)
            ticket["status"] = (e_errors.WRONG_FORMAT, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            if return_record:
                return None, None
            else:
                return None

        if check_exists and return_record:
            #Make sure the volume exists.   (getattr() keeps pychecker quite.)
            record = getattr(self, 'volumedb_dict', {})[external_label]
            if not record:
                message = "%s: no such external_label %s" \
                          % (MY_NAME, external_label,)
                ticket["status"] = (e_errors.NO_VOLUME, message)
                Trace.log(e_errors.ERROR, message)
                self.reply_to_caller(ticket)
                return None, None
        else:
            record = None

        if check_exists and return_record:
            return external_label, record
        else:
            return external_label

    ####################################################################

    #### DONE
    def get_user_sockets(self, ticket):
        try:
            file_clerk_host, file_clerk_port, listen_socket = callback.get_callback()
            listen_socket.listen(4)
            ticket["file_clerk_callback_addr"] = (file_clerk_host, file_clerk_port)

            self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.control_socket.connect(ticket['callback_addr'])
            callback.write_tcp_obj(self.control_socket, ticket)
            r, w, x = select.select([listen_socket], [], [], 15)
            if not r:
                listen_socket.close()
                return 0
            data_socket, address = listen_socket.accept()
            if not hostaddr.allow(address):
                data_socket.close()
                listen_socket.close()
                return 0
            self.data_socket = data_socket
            listen_socket.close()
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.handle_error(exc,msg)
            return 0
        return 1

    ####################################################################

    #### DONE
    # get_all_bfids(external_label) -- get all bfids of a particular volume

    def get_all_bfids(self, external_label):
        q = "select bfid, location_cookie from file, volume\
             where volume.label = '%s' and \
                   file.volume = volume.id \
                   order by location_cookie;"%(external_label)
        res = self.filedb_dict.query_getresult(q)
        bfids = []
        for i in res:
            bfids.append(i[0])
        return bfids

    ####################################################################

    ###
    ### These functions are called via dispatching worker.
    ###

    # show_state -- show internal configuration values
    def show_state(self, ticket):
        ticket['state'] = {}
        for i in self.__dict__.keys():
            ticket['state'][i] = `self.__dict__[i]`
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### DONE
    def get_crcs(self, ticket):

        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.

        complete_crc=record["complete_crc"]
        sanity_cookie=record["sanity_cookie"]
        ticket["status"]=(e_errors.OK, None)
        ticket["complete_crc"]=complete_crc
        ticket["sanity_cookie"]=sanity_cookie
        self.reply_to_caller(ticket)

    # DONE
    # return all info about a certain bfid - this does everything that the
    # read_from_hsm method does, except send the ticket to the library manager
    def bfid_info(self, ticket):

        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.

        #Copy all file information we have to user's ticket.  Copy the info
        # one key at a time to avoid cyclic dictionary references.
        for key in record.keys():
            ticket[key] = record[key]

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10, "bfid_info bfid=%s" % (bfid,))
        return

    # _find_copies(bfid) -- find all copies
    def _find_copies(self, bfid):
        q = "select alt_bfid from file_copies_map where bfid = '%s';"%(bfid)
        bfids = []
        try:
            for i in self.filedb_dict.query_getresult(q):
                bfids.append(i[0])
        except:
            pass
        return bfids

    # find_copies(self, ticket) -- find all copies of bfid
    # this might need recurrsion in the future!
    def find_copies(self, ticket):

        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.

        try:
            bfids = self._find_copies(bfid)
            ticket["copies"] = bfids
            ticket["status"] = (e_errors.OK, None)
	    #
	    # edb module raises underlying DB errors as EnstoreError.
	    #
        except e_errors.EnstoreError, msg:
            ticket["copies"] = []
            ticket["status"] = (msg.type, str(msg))
	except:
	    ticket["copies"] = []
	    ticket["status"] = (str(sys.exc_info()[0]),str(sys.exc_info()[1]))
        self.reply_to_caller(ticket)
        return

    # _find_original(bfid) -- find its original
    # there should eb at most one original!
    def _find_original(self, bfid):
        q = "select bfid from file_copies_map where alt_bfid = '%s';"%(bfid)
        try:
            res = self.filedb_dict.query_getresult(q)
            if len(res):
                return res[0][0]
        except:
            pass
        return None

    # find_original(bfid) -- server version
    def find_original(self, ticket):

        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.

        try:
            original = self._find_original(bfid)
            ticket["original"] = original
            ticket["status"] = (e_errors.OK, None)
	    #
	    # edb module raises underlying DB errors as EnstoreError.
	    #
        except e_errors.EnstoreError, msg:
            ticket["original"] = None
            ticket["status"] = (msg.type, str(msg))
	except:
            ticket["original"] = None
	    ticket["status"] = (str(sys.exc_info()[0]),str(sys.exc_info()[1]))
        self.reply_to_caller(ticket)
        return

    # find any information if this file has been involved in migration
    # or duplication
    def __find_migrated(self, bfid):
        src_list = []
        dst_list = []

        q = "select src_bfid,dst_bfid from migration where (dst_bfid = '%s' or src_bfid = '%s') ;" % (bfid, bfid)

        res = self.filedb_dict.query_getresult(q)
        for row in res:
            src_list.append(row[0])
            dst_list.append(row[1])

        return src_list, dst_list

    # report if this file has been migrated to or from another volume
    def find_migrated(self, ticket):

        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.


        try:
            src_bfid, dst_bfid = self.__find_migrated(bfid)
            ticket["src_bfid"] = src_bfid
            ticket["dst_bfid"] = dst_bfid
            ticket["status"] = (e_errors.OK, None)
	    #
	    # edb module raises underlying DB errors as EnstoreError.
	    #
        except e_errors.EnstoreError, msg:
            ticket["src_bfid"] = None
            ticket["dst_bfid"] = None
            ticket["status"] = (msg.type, str(msg))
	except:
            ticket["src_bfid"] = None
            ticket["dst_bfid"] = None
	    ticket["status"] = (str(sys.exc_info()[0]),str(sys.exc_info()[1]))
        self.reply_to_caller(ticket)
        return

    # find any information if this file has been involved in migration
    # or duplication
    def __find_migration_info(self, bfid, find_src, find_dst, order_by):
        src_res = []
        dst_res = []

        if find_src:
            q = "select * from migration where src_bfid = '%s' order by %s;" \
                % (bfid, order_by)
            Trace.log(e_errors.INFO, q)
	    #
	    # convert datetime.datetime to string
	    #
            src_res = edb.sanitize_datetime_values(self.filedb_dict.query_dictresult(q))
            Trace.log(e_errors.INFO, src_res)

        if find_dst:
            q = "select * from migration where dst_bfid = '%s' order by %s;" \
                % (bfid, order_by)
            Trace.log(e_errors.INFO, q)
	    #
	    # convert datetime.datetime to string
	    #
            dst_res =  edb.sanitize_datetime_values(self.filedb_dict.query_dictresult(q))
            Trace.log(e_errors.INFO, dst_res)

        return src_res + dst_res

    # report any information if this file has been involved in migration
    # or duplication
    def find_migration_info(self, ticket):
        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.

        # extract the additional information if source and/or destination
        # information is requested.
        find_src = self.extract_value_from_ticket('find_src', ticket)
        if find_src == None:
            return #extract_value_from_ticket handles its own errors.
        find_dst = self.extract_value_from_ticket('find_dst', ticket)
        if find_dst == None:
            return #extract_value_from_ticket handles its own errors.

        # extrace the column name of the timestamp field that will be used
        # to sort the responses.
        order_by = self.extract_value_from_ticket('order_by', ticket)
        if order_by == None:
            return #extract_value_from_ticket handles its own errors.

        try:
            ticket['migration_info'] = self.__find_migration_info(bfid,
                                                                  find_src,
                                                                  find_dst,
                                                                  order_by)
            ticket['status'] = (e_errors.OK, None)
        except e_errors.EnstoreError, msg:
            ticket['status'] = (msg.type, str(msg))
	except:
	    ticket["status"] = (str(sys.exc_info()[0]),str(sys.exc_info()[1]))
        self.reply_to_caller(ticket)
        return

    #### DONE
    # __has_undeleted_file(self, vol) -- check if all files are deleted

    def __has_undeleted_file(self, vol):
        Trace.log(e_errors.INFO, 'checking if files of volume %s are deleted'%(vol))
        q = "select bfid from file, volume \
             where volume.label = '%s' and \
                   file.volume = volume.id and \
                   file.deleted = 'n';"%(vol)
	res = self.filedb_dict.query(q)
	return len(res)

    #### DONE
    # has_undeleted_file -- server service

    def has_undeleted_file(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label or external_label == (None, None):
            return #extract_external_lable_from_ticket handles its own errors.

        # catch any failure
        try:
            result = self.__has_undeleted_file(external_label)
            ticket["status"] = (e_errors.OK, result)
	    #
	    # edb module raises underlying DB errors as EnstoreError.
	    #
        except e_errors.EnstoreError, msg:
            ticket["status"] = (msg.type, str(msg))
	except:
	    ticket["status"] = (str(sys.exc_info()[0]),str(sys.exc_info()[1]))
        # and return to the caller
        self.reply_to_caller(ticket)
        return

    #### DONE
    # exist_bfids -- check if a, or a list of, bfid(s) exists/exist

    def exist_bfids(self, ticket):
        try:
            bfids = ticket['bfids']
        except KeyError, detail:
            message = "%s: key %s is missing" % (MY_NAME, detail,)
            ticket["status"] = (e_errors.KEYERROR, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            return

        ##########################################################
        #Extra checks to make sure everything is correct.

        # To do: need to fill this in

        ##########################################################

        if type(bfids) == type([]):    # a list
            result = []
            for i in bfids:
                rec = self.filedb_dict[i]
                if rec:
                    result.append(1)
                else:
                    result.append(0)
        else:
            rec = self.filedb_dict[bfids]
            if rec:
                result = 1
            else:
                result = 0

        ticket['result'] = result
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### DONE
    # get_bfids(self, ticket) -- get bfids of a certain volume
    #        This is almost the same as tape_list() yet it does not
    #        retrieve any information from primary file database

    def get_bfids(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label or external_label == (None, None):
            return #extract_external_lable_from_ticket handles its own errors.

        bfids = self.get_all_bfids(external_label)

        ticket["status"] = (e_errors.OK, None)

        self.reply_to_caller(ticket)

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        # write to data socket
        callback.write_tcp_obj(self.data_socket,ticket)
        callback.write_tcp_obj_new(self.data_socket, bfids)
        self.data_socket.close()
        # write to control socket
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
        return

    # get_bfids(self, ticket) -- get bfids of a certain volume
    #        This is almost the same as tape_list() yet it does not
    #        retrieve any information from primary file database
    #
    # This is even newer and better implementation that replaces
    # get_bfids().  Now the network communications are done using
    # send_reply_with_long_answer().
    def get_bfids2(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label or external_label == (None, None):
            return #extract_external_lable_from_ticket handles its own errors.

        # get bfids
        bfid_list = self.get_all_bfids(external_label)

        # send the reply
        ticket['bfids'] = bfid_list
        ticket["status"] = (e_errors.OK, None)
        try:
            self.send_reply_with_long_answer(ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "get_bfids2: %s" % (str(msg),))
            return

        return

    #If export_format is False, the default, then the raw names from the
    # db query are returned in the dictionary.  If export_format is True,
    # then the edb.py file DB export_format() function is called to
    # rename the keys from the query.
    # If all_files is False then get list of files, only resided on tape,
    # do not include members of packages.
    def __tape_list(self, external_label, export_format = False, all_files = True, skip_unknown = False):
        q = """
            SELECT f.bfid,
                f.crc,
                f.deleted,
                f.drive,
                v.label AS label,
                f.location_cookie,
                f.pnfs_path,
                f.pnfs_id,
                f.sanity_size,
                f.sanity_crc,
                f.size,
                f.package_id,
                f.archive_status,
                f.cache_status
            FROM file f,
                volume v
            WHERE f.volume = v.id
                AND v.label='{}'
            ORDER BY f.location_cookie
	    """
        res = self.filedb_dict.query_dictresult(q.format(external_label))
        # convert to external format
        file_list = []
	location_cookies={}
        for file_info in res:
            if export_format:
                # used for tape_list3()
                value = self.filedb_dict.export_format(file_info)
            else:
                # used for tape_list2()
                value = file_info
	    lc = value.get("location_cookie")
            if value.get("pnfsid") :
                location_cookies[lc] = location_cookies.get(lc,0)+1
	    if value['deleted'] == 'unknown' and skip_unknown :
		    continue
            if not value.has_key('pnfs_name0'):
                value['pnfs_name0'] = "unknown"
            file_list.append(value)
            if file_info.get("bfid",None) == file_info.get("package_id",None) and all_files :
	       result = self.filedb_dict.query_dictresult("select f.bfid, f.crc, f.deleted, f.drive , \
	       f.location_cookie, f.pnfs_path, f.pnfs_id, \
	       f.sanity_size, f.sanity_crc, f.size, \
	       f.package_id, f.archive_status, f.cache_status \
	       from file f where f.package_id='%s' and f.bfid != '%s'"%((file_info.get("bfid",None),)*2))
	       for finfo in result:
		       finfo["location_cookie"] = file_info.get("location_cookie",None)
		       finfo["label"] = file_info.get("label",None)
		       if export_format:
			       value = self.filedb_dict.export_format(finfo)
		       else:
			       value = finfo
		       if not value.has_key('pnfs_name0'):
			       value['pnfs_name0'] = "unknown"
		       file_list.append(value)

	if skip_unknown:
	   duplicate_cookies = [ cookie for cookie, count in location_cookies.iteritems() if count > 1 ]
	   if duplicate_cookies :
	      Trace.alarm(e_errors.WARNING, "Volume {} contains duplicate cookie(s) : {}".format(external_label,
												 string.join(duplicate_cookies," ")))
        return file_list

    #### DONE
    def tape_list(self, ticket):
        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label or external_label == (None, None):
            return #extract_external_lable_from_ticket handles its own errors.

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket,ticket)

        # log the activity
        Trace.log(e_errors.INFO, "start listing " + external_label)

        vol = self.__tape_list(external_label,
			       all_files = ticket.get("all", True),
			       skip_unknown = ticket.get("skip_unknown",False))

        # finishing up

        callback.write_tcp_obj_new(self.data_socket, vol)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
        Trace.log(e_errors.INFO, "finish listing " + external_label)
        return

    # This is the newer implementation that off load to client
    def tape_list2(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label or external_label == (None, None):
            return #extract_external_lable_from_ticket handles its own errors.

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket, ticket)

        # log the activity
        Trace.log(e_errors.INFO, "start listing " + external_label + " (2)")

        vol = self.__tape_list(external_label,
			       all_files = ticket.get("all", True),
			       skip_unknown = ticket.get("skip_unknown",False))



        # finishing up

        callback.write_tcp_obj_new(self.data_socket, vol)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket, ticket)
        self.control_socket.close()

        # log the activity
        Trace.log(e_errors.INFO, "finish listing " + external_label + " (2)")
        return

    # This is even newer and better implementation that replaces
    # tape_list2().  Now the network communications are done using
    # send_reply_with_long_answer().
    def tape_list3(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label or external_label == (None, None):
            return #extract_external_lable_from_ticket handles its own errors.

        # log the activity
        Trace.log(e_errors.INFO, "start listing " + external_label + " (3)")

        # start communication
        ticket["status"] = (e_errors.OK, None)
        try:
            control_socket = self.send_reply_with_long_answer_part1(ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "tape_list3(): %s" % (str(msg),))
            return

        #Make sure the socket exists.
        if not control_socket:
            return

        # get reply
        file_info = self.__tape_list(external_label,
				     export_format = True,
				     all_files = ticket.get("all", True),
				     skip_unknown = ticket.get("skip_unknown",False))

        ticket['tape_list'] = file_info

        # send the reply
        try:
            self.send_reply_with_long_answer_part2(control_socket, ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "tape_list3(): %s" % (str(msg),))
            return

        # log the activity
	Trace.log(e_errors.INFO, "finish listing " + external_label + " (3)")

    #### DONE
    # list_active(self, ticket) -- list the active files on a volume
    #     only the /pnfs path is listed
    #     the purpose is to generate a list for deletion before the
    #     deletion of a volume

    def list_active(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label or external_label == (None, None):
            return #extract_external_lable_from_ticket handles its own errors.

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket,ticket)

        q = "select bfid, crc, deleted, drive, volume.label, \
                    location_cookie, pnfs_path, pnfs_id, \
                    sanity_size, sanity_crc, size \
             from file, volume \
             where \
                 file.volume = volume.id and volume.label = '%s' \
             order by location_cookie;"%(external_label)

        res = self.filedb_dict.query_dictresult(q)

        alist = []

        for ff in res:
            value = self.filedb_dict.export_format(ff)
            if not value.has_key('deleted') or value['deleted'] != "yes":
                if value.has_key('pnfs_name0') and value['pnfs_name0']:
                    alist.append(value['pnfs_name0'])

        # finishing up

        callback.write_tcp_obj_new(self.data_socket, alist)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
        return

    def __list_active2(self, external_label):
        q = "select pnfs_path from \
    		(select pnfs_path, location_cookie \
    		from file, volume \
    		where \
    			file.volume = volume.id and volume.label = '%s' and\
    			deleted = 'n' and not pnfs_path is null and \
    			pnfs_path != '' order by location_cookie) a1;"%(
    		 external_label)

    	return self.filedb_dict.query_getresult(q)


    # list_active2(self, ticket) -- list the active files on a volume
    #	 only the /pnfs path is listed
    #	 the purpose is to generate a list for deletion before the
    #	 deletion of a volume

    def list_active2(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label or external_label == (None, None):
            return #extract_external_lable_from_ticket handles its own errors.

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

    	# get a user callback
    	if not self.get_user_sockets(ticket):
    		return
    	callback.write_tcp_obj(self.data_socket, ticket)

        res = self.__list_active2(external_label)

    	# finishing up

    	callback.write_tcp_obj_new(self.data_socket, res)
    	self.data_socket.close()
    	callback.write_tcp_obj(self.control_socket,ticket)
    	self.control_socket.close()
    	return

    # This is even newer and better implementation that replaces
    # list_active2().  Now the network communications are done using
    # send_reply_with_long_answer().
    def list_active3(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label or external_label == (None, None):
            return #extract_external_lable_from_ticket handles its own errors.

        # start communication
        ticket["status"] = (e_errors.OK, None)
        try:
            control_socket = self.send_reply_with_long_answer_part1(ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "list_active3(): %s" % (str(msg),))
            return

        #Make sure the socket exists.
        if not control_socket:
            return

        # get reply
        file_info = self.__list_active2(external_label)
        ticket['active_list'] = file_info

        # send the reply
        try:
            self.send_reply_with_long_answer_part2(control_socket, ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "list_active3(): %s" % (str(msg),))
            return

    def __show_bad(self):
        q = "select label, bad_file.bfid, size, path \
             from bad_file, file, volume \
             where \
                 bad_file.bfid = file.bfid and \
                 file.volume = volume.id;"
        return self.filedb_dict.query_dictresult(q)


    def show_bad(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket,ticket)

        res = self.__show_bad()

        # finishing up

        callback.write_tcp_obj_new(self.data_socket, res)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket, ticket)
        self.control_socket.close()
        return

    # This is even newer and better implementation that replaces
    # show_bad().  Now the network communications are done using
    # send_reply_with_long_answer().
    def show_bad2(self, ticket):

        # get bad files
        bad_files = self.__show_bad()

        # send the reply
        ticket['bad_files'] = bad_files
        ticket["status"] = (e_errors.OK, None)
        try:
            self.send_reply_with_long_answer(ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "show_bad2: %s" % (str(msg),))
            return

        return

    def get_children(self, ticket):
	    #
	    # this function return files that belong to package back to caller
	    #
	    bfid, record = self.extract_bfid_from_ticket(ticket)
	    if not bfid:
		    return #extract_bfid_from_ticket handles its own errors.
	    #
	    # check that this is indeed the package file.
	    #
	    if bfid != record['package_id'] or \
	       type(record['package_id']) == types.NoneType :
		    ticket["status"] = (e_errors.ERROR, "This is not a package file")
		    self.reply_to_caller(ticket)
		    return
            field = ticket.get("field",None)
	    file_records = None
            if field :
                try:
                    colnames, res =  self.filedb_dict.dbaccess.query_with_columns("select * from file limit 1")
                    if field not in colnames :
                        ticket["status"] = (e_errors.WRONGPARAMETER, "No such field %s"%(field,))
                        self.reply_to_caller(ticket)
                        return
                    res =  self.filedb_dict.query_getresult("select %s from file where package_id = '%s' and deleted='n' and bfid<>'%s'"%(field, bfid,bfid))
                    file_records = [ r[0] for r in res ]
                except Exception as e:
                    ticket["status"] = (e_errors.DATABASE_ERROR, str(e))
                    self.reply_to_caller(ticket)
                    return
            else :
                #
                # retrieve the children
                #
                q = "select bfid from file where package_id = '%s' and deleted='n' and bfid <> '%s' "%(bfid,bfid)
                res = self.filedb_dict.query_getresult(q)
                file_records = [ self.filedb_dict[r[0]] for r in res ]

	    ticket["status"] = (e_errors.OK, None)
	    ticket["children"] = file_records

	    try:
		    self.send_reply_with_long_answer(ticket)
	    except (socket.error, select.error), msg:
		    Trace.log(e_errors.INFO, "get_children: %s" % (str(msg),))



class FileClerkMethods(FileClerkInfoMethods):

    def __init__(self, csc):
        global isSFA
        FileClerkInfoMethods.__init__(self, csc)

        # find the brand
        Trace.log(e_errors.INFO, "find the brand")
        try:
            brand = self.csc.get('file_clerk')['brand']
            Trace.log(e_errors.INFO, "The brand is %s" % (brand))
        except:
            brand = string.upper(string.split(os.uname()[1], ".")[0][:2])+'MS'
            Trace.log(e_errors.INFO,
                      "No brand is found, using '%s'" % (brand,))
	self.en_qpid_client = None
	self.amqp_broker_dict = None
	if isSFA:
		self.amqp_broker_dict = self.csc.get(AMQP_BROKER,None)
		if self.amqp_broker_dict and self.amqp_broker_dict["status"][0] == e_errors.OK :
			dispatcher_conf = self.csc.get('dispatcher', None)
			if dispatcher_conf and dispatcher_conf["status"][0] == e_errors.OK :
				try:
					cache.en_logging.en_logging.set_logging(self.log_name)
				except NameError, msg:
					# import error already reported
					pass
				fc_queue = "%s; {create: always}"%(dispatcher_conf['queue_reply'],)
				pe_queue = "%s; {create: always}"%(dispatcher_conf['queue_work'],)
				self.en_qpid_client = qpid_client.EnQpidClient((self.amqp_broker_dict['host'],
										self.amqp_broker_dict['port']),
									       fc_queue,
									       pe_queue)
				self.en_qpid_client.start()
			else:
				if dispatcher_conf :
					Trace.log(e_errors.INFO,  dispatcher_conf["status"][1])
				else:
					Trace.log(e_errors.INFO,  "Failed to extract 'dispatcher' from configuration")
		else:
			if self.amqp_broker_dict:
				Trace.log(e_errors.INFO,  self.amqp_broker_dict["status"][1])
			else:
				Trace.log(e_errors.INFO,  "Failed to extract '%s' from configuration"%(AMQP_BROKER,))



        #Set the brand.
        self.set_brand(brand)

        #Retrieve database information from the configuration.
        Trace.log(e_errors.INFO,"determine dbHome and jouHome")

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

        self.sequentialQueueSize     = dbInfo.get('sequential_queue_size',SEQUENTIAL_QUEUE_SIZE)
	self.parallelQueueSize       = dbInfo.get('parallel_queue_size',PARALLEL_QUEUE_SIZE)
	self.numberOfParallelWorkers = dbInfo.get('max_threads',MAX_THREADS)
        self.max_connections         = self.numberOfParallelWorkers+1

        #Open conection to the Enstore DB.
        Trace.log(e_errors.INFO, "opening file database using edb.FileDB")
        try:
		# proper default values are supplied by edb.FileDB constructor
		self.filedb_dict = edb.FileDB(host=dbInfo.get('db_host',None),
					      port=dbInfo.get('db_port',None),
					      user=dbInfo.get('dbuser',None),
					      database=dbInfo.get('dbname',None),
					      jou=jouHome,
					      max_connections=self.max_connections)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            message = str(exc_type)+' '+str(exc_value)+' IS POSTMASTER RUNNING?'
            Trace.log(e_errors.ERROR, message)
            Trace.alarm(e_errors.ERROR, message, {})
            Trace.log(e_errors.ERROR, "CAN NOT ESTABLISH DATABASE CONNECTION ... QUIT!")
            sys.exit(1)

	self.sequentialThreadQueue = Queue.Queue(self.sequentialQueueSize)
	self.sequentialWorker = dispatching_worker.ThreadExecutor(self.sequentialThreadQueue,self)
	self.sequentialWorker.start()

	self.parallelThreadQueue = Queue.Queue(self.parallelQueueSize)

	self.parallelThreads = []
	for i in range(self.numberOfParallelWorkers):
		worker = dispatching_worker.ThreadExecutor(self.parallelThreadQueue,self)
		self.parallelThreads.append(worker)
		worker.start()


    ###
    ### These functions are internal file_clerk functions.
    ###

    def invoke_function(self, function, args=()):
        if  function.__name__  in ("tape_list3",
				   "set_cache_status",
				   "get_children",
				   "alive",
				   "set_children",
				   "get_bfids",
				   "get_bfids2",
				   "show_state",
				   "find_copies",
				   "replay",
				   "get_brand",
				   "get_crcs",
				   "has_undeleted_file",
				   "open_bitfile",
				   "open_bitfile_for_package",
                                   "bfid_info") :
		Trace.trace(5, "Putting on parallel thread queue %d %s"%(self.parallelThreadQueue.qsize(),function.__name__))
		self.parallelThreadQueue.put([function.__name__, args])

	elif  function.__name__ == "quit":
		# needs to run on main thread
		apply(function,args)
	else:
		Trace.trace(5, "Putting on sequential thread queue %d %s"%(self.sequentialThreadQueue.qsize(),function.__name__))
		self.sequentialThreadQueue.put([function.__name__, args])

    # set_brand(brand) -- set brand

    def set_brand(self, brand):
        self.brand = brand
        return

    def get_brand(self, ticket):
        ticket['brand'] = self.brand
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### DONE
    # A bit file id is defined to be a 64-bit number whose most significant
    # part is based on the time, and the least significant part is a count
    # to make it unique

    def unique_bit_file_id(self):
        bfid = long(time.time()*100000)
        while self.filedb_dict.has_key(self.brand+str(bfid)):
            bfid = bfid + 1
        return self.brand+str(bfid)

    # register_copy(original, copy) -- register copy of original
    def register_copy(self, original, copy):
        Trace.log(e_errors.INFO,
                  'register copy %s of original %s' % (copy, original))
	q = "insert into file_copies_map (bfid, alt_bfid) values ('%s', '%s');"%(original, copy)
        try:
            res = self.filedb_dict.insert(q)
        except:
            return 1
        return

    # log_copies(bfid, n) -- log number of copies to make
    def log_copies(self, bfid, n):
        Trace.log(e_errors.INFO, "log_copies: %s, %d"%(bfid, n))
        q = "insert into active_file_copying (bfid, remaining) \
                values ('%s', %d);"%(bfid, n)
        try:
            res = self.filedb_dict.insert(q)
        except:
            return 1
        return

    # _made_copy(bfid) -- decrease copies count
    #                    if the count becomes zero, delete the record
    def _made_copy(self, bfid):
        q = "select * from active_file_copying where bfid = '%s';"%(bfid)
        res = self.filedb_dict.query_dictresult(q)
        if not res:
            Trace.log(e_errors.ERROR, "made_copy(): %s does not have copies"%(bfid))
            return 0
        if res[0]['remaining'] <= 1:
            # all done, delete this entry
            q = "delete from active_file_copying where bfid = '%s';"%(bfid)
            try:
                res = self.filedb_dict.remove(q)
            except:
                return 1
        else: # decrease the number
            q = "update active_file_copying set remaining = %d where bfid = '%s';"%(res[0]['remaining'] - 1, bfid)
            try:
                res = self.filedb_dict.insert(q)
            except:
                return 1
        return 0

    ####################################################################

    ###
    ### These functions are called via dispatching worker.
    ###

    #### DONE
    # we need a new bit field id for each new file in the system
    def new_bit_file(self, ticket):
        # input ticket is a file clerk part of the main ticket
        # create empty record and control what goes into database
        # do not pass ticket, for example to the database!
        record = {'pnfsid':'','drive':'','pnfs_name0':'','deleted':'unknown'}

        record["external_label"]   = ticket["fc"]["external_label"]
        record["location_cookie"]  = ticket["fc"]["location_cookie"]
        record["size"]             = ticket["fc"]["size"]
        record["sanity_cookie"]    = ticket["fc"]["sanity_cookie"]
        record["complete_crc"]     = ticket["fc"]["complete_crc"]
	record["original_library"] = ticket["fc"].get("original_library",None)

        # uid and gid
        if ticket["fc"].has_key("uid"):
            record["uid"] = ticket["fc"]["uid"]
        if ticket["fc"].has_key("gid"):
            record["gid"] = ticket["fc"]["gid"]

        # does it have bfid?
        if ticket["fc"].has_key("bfid"):
            bfid = ticket["fc"]["bfid"]
            # make sure the brand is right
            if bfid[:len(self.brand)] != self.brand:
                msg = "new_bit_file(): wrong brand %s (%s)"%(bfid, self.brand)
                Trace.log(e_errors.ERROR, msg)
                ticket["status"] = (e_errors.FILE_CLERK_ERROR, msg)
                self.reply_to_caller(ticket)
                return
            # make sure the bfid is right
            if bfid[len(self.brand):].isdigit():
                msg = "new_bit_file(): invalid bfid %s"%(bfid,)
                Trace.log(e_errors.ERROR, msg)
                ticket["status"] = (e_errors.ERROR, msg)
                self.reply_to_caller(ticket)
                return
            # make sure the bfid does not exist
            if self.filedb_dict[bfid]:
                msg = "new_bit_file(): %s exists"%(bfid,)
                Trace.log(e_errors.ERROR, msg)
                ticket["status"] = (e_errors.BFID_EXISTS, msg)
                self.reply_to_caller(ticket)
                return
        else:
            # get a new bit file id
            bfid = self.unique_bit_file_id()

        # check for copy
        original_bfid = None
        if ticket["fc"].has_key("original_bfid"):
            # check if it is valid
            original_bfid = ticket["fc"].get("original_bfid")
            original_file = self.filedb_dict[original_bfid]
            if not original_file:
                msg = "new_bit_file(copy): original bfid %s does not exist"%(original_bfid)
                Trace.log(e_errors.ERROR, msg)
                ticket["status"] = (e_errors.NO_FILES, msg)
                self.reply_to_caller(ticket)
                return
            # check size
            if original_file['size'] != record['size']:
                msg = "new_bit_file(copy): wrong size %d, (%s, %d)"%(
                    record['size'], original_bfid, original_file['size'])
                Trace.log(e_errors.ERROR, msg)
                ticket["status"] = (e_errors.FILE_CLERK_ERROR, msg)
                self.reply_to_caller(ticket)
                return
            # check crc
            if original_file['complete_crc'] != record["complete_crc"]:
                msg = "new_bit_file(copy): wrong crc %d, (%s, %d)"%(
                     record["complete_crc"], original_bfid, original_file['complete_crc'])
                Trace.log(e_errors.ERROR, msg)
                ticket["status"] = (e_errors.FILE_CLERK_ERROR, msg)
                self.reply_to_caller(ticket)
                return
            # check sanity_cookie
            if original_file['sanity_cookie'] != record["sanity_cookie"]:
                msg = "new_bit_file(copy): wrong sanity_cookie %s, (%s, %s)"%(
                     `record["sanity_cookie"]`, original_bfid, `original_file['sanity_cookie']`)
                Trace.log(e_errors.ERROR, msg)
                ticket["status"] = (e_errors.FILE_CLERK_ERROR, msg)
                self.reply_to_caller(ticket)
                return

        record["bfid"] = bfid
	now = time.time()
	if ticket["fc"].get("mover_type",None) == "DiskMover":
		record["cache_status"] = file_cache_status.CacheStatus.CREATED
		record["cache_mod_time"] = time2timestamp(now)

        # record it to the database
        self.filedb_dict[bfid] = record

        # if it is a copy, register it
        if original_bfid:
            if self.register_copy(original_bfid, bfid):
                msg = "new_bit_file(copy): failed to register copy %s, %s"%(original_bfid, bfid)
                Trace.log(e_errors.ERROR, msg)
                ticket["status"] = (e_errors.FILE_CLERK_ERROR, msg)
                self.reply_to_caller(ticket)
                return

        count = ticket["fc"].get("copies", 0)
        if count > 0:
            if self.log_copies(bfid, count):
                msg = "new_bit_file(copy): failed to log copy count %s, %d"%(bfid, count)
                Trace.log(e_errors.ERROR, msg)
                ticket["status"] = (e_errors.FILE_CLERK_ERROR, msg)
                self.reply_to_caller(ticket)
                return

        ticket["fc"]["bfid"] = bfid
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10,'new_bit_file bfid=%s'%(bfid,))
        return

    #### DONE
    # add_file_record() -- create a file record
    #
    # This is very dangerous!
    #
    # The bfid must be None or one that has not already existed

    def add_file_record(self, ticket):

        if ticket.has_key('bfid'):
            bfid = ticket['bfid']
            # to see if the bfid has already been used
            record = self.filedb_dict[bfid]
            if record:
                msg = 'bfid "%s" has already been used'%(bfid)
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.BFID_EXISTS, msg)
                self.reply_to_caller(ticket)
                return
        else:
            bfid = self.unique_bit_file_id()
            ticket['bfid'] = bfid

        # handle branding

        if bfid[0] in string.letters:
            sequence = long(bfid[4:]+'L')
            while self.filedb_dict.has_key(self.brand+str(sequence)):
                sequence = sequence + 1
            bfid = self.brand+str(sequence)

        # extracting the values
        try:
            complete_crc = ticket['complete_crc']
            deleted = ticket['deleted']
            drive = ticket['drive']
            external_label = ticket['external_label']
            location_cookie = ticket['location_cookie']
            pnfs_name0 = ticket['pnfs_name0']
            pnfsid = ticket['pnfsid']
            pnfsvid = ticket.get('pnfsvid')
            sanity_cookie = ticket['sanity_cookie']
            size = ticket['size']
        except KeyError, detail:
            message =  "%s: add_file_record() -- key %s is missing" \
                      % (MY_NAME, detail,)
            ticket["status"] = (e_errors.KEYERROR, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            return

        record = {}
        record['bfid'] = bfid
        record['complete_crc'] = complete_crc
        record['deleted'] = deleted
        record['drive'] = drive
        record['external_label'] = external_label
        record['location_cookie'] = location_cookie
        record['pnfs_name0'] = pnfs_name0
        record['pnfsid'] = pnfsid
        if pnfsvid:
            record['pnfsvid'] = pnfsvid
        record['sanity_cookie'] = sanity_cookie
        record['size'] = size

        # handle uid and gid
        if ticket.has_key("uid"):
            record["uid"] = ticket["uid"]
        if ticket.has_key("gid"):
            record["gid"] = ticket["gid"]

        # assigning it to database
        self.filedb_dict[bfid] = record
        Trace.log(e_errors.INFO, 'assigned: '+`record`)
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### DONE
    # modify_file_record() -- modify file record
    #
    # This is very dangerous!
    #
    # bfid must exist

    def modify_file_record(self, ticket):
        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.

        # better log this
        Trace.log(e_errors.INFO, "start modifying "+`record`)

        # modify the values
        for k in ticket.keys():
	    # can not change bfid!
	    if k != 'bfid' and record.has_key(k):
	        record[k] = ticket[k]

        # assigning it to database
        self.filedb_dict[bfid] = record
        Trace.log(e_errors.INFO, 'modified to '+`record`)
        ticket['status'] = (e_errors.OK, None)
	self.reply_to_caller(ticket)
	return

    def modify_file_records(self, ticket):
        try:
	    records = ticket["list"]
	    for record in records:
	        bfid = record["bfid"]
	        self.filedb_dict[bfid] = record
	    ticket['status'] = (e_errors.OK, None)
	except (IndexError, KeyError), msg:
	    ticket['status'] = (e_errors.ERROR, msg)
	finally:
	    del(ticket["list"])
	    self.reply_to_caller(ticket)

    #### DONE
    # update the database entry for this file - add the pnfs file id
    def set_pnfsid(self, ticket):

        bfid, record = self.extract_bfid_from_ticket(ticket.get('fc', {}))
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.

        pnfsid = self.extract_value_from_ticket('pnfsid', ticket.get('fc', {}))
        if not pnfsid:
            return #extract_value_from_ticket handles its own errors.

        # temporary workaround - sam doesn't want to update encp too often
        pnfsvid = ticket["fc"].get("pnfsvid")
        pnfs_name0 = ticket["fc"].get("pnfs_name0")

        # start (10/18/00) adding which drive we used to write the file
        drive = ticket["fc"].get("drive","unknown:unknown")

        # start (7/26/2004) adding which user wrote the file
        uid = ticket["fc"].get("uid", None)
        gid = ticket["fc"].get("gid", None)

        # add the pnfsid
        record["pnfsid"] = pnfsid
        record["drive"] = drive
        # temporary workaround - see above
        if pnfsvid != None:
            record["pnfsvid"] = pnfsvid
        if pnfs_name0 != None:
            record["pnfs_name0"] = pnfs_name0
        if uid != None:
            record["uid"] = uid
        if gid != None:
            record["gid"] = gid
        record["deleted"] = "no"
	record["original_library"] = ticket["fc"].get("original_library",None)
	record["file_family_width"] = ticket["fc"].get("file_family_width",None)
	if ticket["fc"].get("mover_type",None) == "DiskMover":
		record["cache_status"] = file_cache_status.CacheStatus.CACHED
		record["cache_location"] = record.get("location_cookie",None)

        # take care of the copy count
        original = self._find_original(bfid)
        if original:
            self._made_copy(original)

	# record our changes
	self.filedb_dict[bfid] = record
	ticket["status"] = (e_errors.OK, None)
	#
	# send event to PE
	#
	if self.en_qpid_client :
		if ticket["fc"].get("mover_type",None) == "DiskMover":
			event = pe_client.evt_cache_written_fc(ticket,record)
			try:
				self.en_qpid_client.send(event)
			except:
				ticket["status"] = (str(sys.exc_info()[0]),str(sys.exc_info()[1]))

	self.reply_to_caller(ticket)

	Trace.trace(12,'set_pnfsid %s'%(ticket,))
	return

    def open_bitfile(self, ticket):
	    #
	    # ticket contains only bfid
	    #
	    bfid, record = self.extract_bfid_from_ticket(ticket)
	    if not bfid:
		    return #extract_bfid_from_ticket handles its own errors.
	    if record["cache_status"]  in [file_cache_status.CacheStatus.CACHED,
					   file_cache_status.CacheStatus.STAGING_REQUESTED,
					   file_cache_status.CacheStatus.STAGING]:
		    ticket["status"] = (e_errors.OK, None)
		    self.reply_to_caller(ticket)
		    return
	    record["cache_status"] = file_cache_status.CacheStatus.STAGING_REQUESTED;
	    #
	    # record change in DB
	    #
	    self.filedb_dict[bfid] = record
	    ticket["status"] = (e_errors.OK, None)
	    ticket["fc"] = record
	    ticket["fc"]["disk_library"] = record["library"]
	    if self.en_qpid_client :
		    event = pe_client.evt_cache_miss_fc(ticket,record)
		    try:
			    self.en_qpid_client.send(event)
		    except:
			    ticket["status"] = (str(sys.exc_info()[0]),str(sys.exc_info()[1]))
	    self.reply_to_caller(ticket)
	    return

    def open_bitfile_for_package(self, ticket):
	    #
	    # this function changes the status of package file and
	    # we rely on DB trigger to change the status of all files in the package
	    #
	    bfid, record = self.extract_bfid_from_ticket(ticket)
	    if not bfid:
		    return #extract_bfid_from_ticket handles its own errors.

	    if record["cache_status"]  in [file_cache_status.CacheStatus.CACHED,
					   file_cache_status.CacheStatus.STAGING_REQUESTED,
					   file_cache_status.CacheStatus.STAGING]:
		    ticket["status"] = (e_errors.OK, None)
		    self.reply_to_caller(ticket)
		    return
	    #
	    # check that this is indeed the package file. Package file
	    #
	    if bfid != record['package_id'] or \
	       type(record['package_id']) == types.NoneType :
		    ticket["status"] = (e_errors.ERROR, "This is not a package file")
		    self.reply_to_caller(ticket)
		    return
	    record["cache_status"] = file_cache_status.CacheStatus.STAGING_REQUESTED
	    self.filedb_dict[bfid] = record
	    q3 = "select v.library from file f, volume v where v.id=f.volume and f.package_id = '%s' and f.deleted='n' and f.bfid!=f.package_id limit 1"%(record["package_id"],)
	    ticket["status"] = (e_errors.OK, None)
	    ticket["fc"] = record
	    library=None
	    try:
		    library=self.filedb_dict.query_getresult(q3)[0][0]
	    except:
		    ticket["status"] =  (str(sys.exc_info()[0]),str(sys.exc_info()[1]))
		    self.reply_to_caller(ticket)
		    return
	    ticket["fc"]["disk_library"] = library
	    self.reply_to_caller(ticket)
	    #
	    # update children + package in one go
	    #
	    t = time2timestamp(time.time())
	    q1 = "update file set cache_status='%s' where package_id='%s' and deleted='n' and cache_status!='%s'"%(file_cache_status.CacheStatus.STAGING_REQUESTED,
														   record["package_id"],
														   file_cache_status.CacheStatus.CACHED)
	    q2 = "update file set cache_mod_time='%s' where package_id='%s' and deleted='n' and cache_status='%s'"%(t,
														    record["package_id"],
														    file_cache_status.CacheStatus.CACHED)


	    for q in (q1,q2):
		    self.filedb_dict.update(q)

	    if self.en_qpid_client :
		    event = pe_client.evt_cache_miss_fc(ticket,record)
		    self.en_qpid_client.send(event)

    def set_children(self, ticket):
	    #
	    # this function operates on package file and modifies
	    # their fields in db
	    #
	    bfid, record = self.extract_bfid_from_ticket(ticket)
	    if not bfid:
		    return #extract_bfid_from_ticket handles its own errors.
	    #
	    # check that this is indeed the package file. Package file
	    #
	    if bfid != record['package_id'] or \
	       type(record['package_id']) == types.NoneType :
		    ticket["status"] = (e_errors.ERROR, "This is no a package file")
		    self.reply_to_caller(ticket)
		    return
	    #
	    # modify values for package file
	    #
	    for k in ticket.keys():
		    if k != 'bfid' and record.has_key(k):
			    record[k] = ticket[k]
	    #
	    # retrieve the children. Note that the query below retrieves the parent
	    # as well
	    #
	    q = "select bfid from file where package_id = '%s' and deleted='n'"%(bfid,)
	    res = self.filedb_dict.query_getresult(q)
	    file_records = []
	    for i in res:
		    child_record = self.filedb_dict[i[0]]
		    #
		    # modify values for files in package
		    #
		    for k in ticket.keys():
			    if k != 'bfid' and record.has_key(k):
				    child_record[k] = ticket[k]
		    #
		    # record change in db
		    #
		    self.filedb_dict[i[0]] = child_record

	    ticket["status"] = (e_errors.OK, None)
	    self.reply_to_caller(ticket)
	    return

    def swap_package(self, ticket) :
	    #
	    #  ticket must contain bfid of old package file and bfid of new package file
	    # 'bfid', 'new_bfid'
	    #
	    #  this function is needed to migration
	    #
	    bfid     = self.extract_value_from_ticket('bfid', ticket, fail_None = True)
	    new_bfid = self.extract_value_from_ticket('new_bfid', ticket, fail_None = True)
	    if not bfid or not new_bfid:
		    return
	    #
	    # swap_package handles all other errors on DB backend
	    #
	    q = "select swap_package('%s','%s')"%(bfid,new_bfid,)
	    try:
		    res = self.filedb_dict.update(q)
	    except e_errors.EnstoreError as e:
		    ticket["status"] = e.ticket["status"]
		    self.reply_to_caller(ticket)
		    return
	    except:
		    exc_type, exc_value = sys.exc_info()[:2]
		    message = 'swap_parent(): '+str(exc_type)+' '+str(exc_value)+' query: '+q
		    Trace.log(e_errors.ERROR, message)
		    ticket["status"] = (e_errors.ERROR, message)
		    self.reply_to_caller(ticket)
		    return
	    ticket["status"] = (e_errors.OK, None)
	    self.reply_to_caller(ticket)
	    return


    def set_cache_status(self,ticket):
	    list_of_arguments = ticket.get("bfids",None)
	    if not list_of_arguments:
		    ticket["status"] = (e_errors.ERROR,
					"Failed to extract list of bfids from ticket %s"%(str(ticket)))

		    self.reply_to_caller(ticket)
		    return
	    del(ticket["bfids"])
	    ticket["status"] = (e_errors.OK, None)
	    self.reply_to_caller(ticket)
	    for item in list_of_arguments:
		    bfid = item.get("bfid",None)
		    if not bfid:
			    continue
		    q = "update file set "
		    haveAny = False
		    for key in ("cache_status","archive_status","cache_location"):
			    value = item.get(key,None)
			    if not value:
				    continue
			    haveAny = True
			    if value.lower() == "null" :
				    q += key+"=NULL,"
			    else:
				    q += key+"='"+value+"',"

		    if not haveAny:
			    continue

		    q = q[:-1] + " where bfid='%s'"%(bfid,)

		    self.filedb_dict.update(q)

    #### DONE
    def set_crcs(self, ticket):

        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.

        complete_crc = self.extract_value_from_ticket('complete_crc', ticket)
        if not complete_crc:
            return #extract_value_from_ticket handles its own errors.

        sanity_cookie = self.extract_value_from_ticket('sanity_cookie', ticket)
        if not sanity_cookie:
            return #extract_value_from_ticket handles its own errors.


        record["complete_crc"]=complete_crc
        record["sanity_cookie"]=sanity_cookie
        #record our changes to the database
        self.filedb_dict[bfid] = record
        ticket["status"]=(e_errors.OK, None)
        #reply to caller with updated database values
        record=self.filedb_dict[bfid]
        ticket["complete_crc"]=record["complete_crc"]
        ticket["sanity_cookie"]=record["sanity_cookie"]
        self.reply_to_caller(ticket)

    #### DONE
    # change the delete state element in the dictionary
    def set_deleted(self, ticket):

        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.

        deleted = self.extract_value_from_ticket('deleted', ticket)
        if not deleted:
            return #extract_value_from_ticket handles its own errors.
        deleted = string.lower(deleted);

	if deleted not in enstore_constants.FILE_DELETED_FLAGS:
		message="Unsupported delete flag \"%s\", supported flags are "%(deleted,)
		for f in enstore_constants.FILE_DELETED_FLAGS:
			message=message+"\""+f+"\","
		message = message[:-1]  #remove trailing comma
		ticket["status"] = (e_errors.FILE_CLERK_ERROR, message)
		self.reply_to_caller(ticket)
		return

	if record.get("package_id",None) == bfid and \
	   record.get("active_package_files_count") > 0 :
		ticket["status"] = (e_errors.FILE_CLERK_ERROR, "cannot set deleted non-empty package file")
		self.reply_to_caller(ticket)
		return

        if record["deleted"] != deleted:
            record["deleted"] = deleted
            self.filedb_dict[bfid] = record

        # take care of the copies
        copies = self._find_copies(bfid)
        for i in copies:
            record = self.filedb_dict[i]
            # skip non existing copies
            if record:
                if record["deleted"] != deleted:
                    record["deleted"] = deleted
                    self.filedb_dict[i] = record

        ticket["status"] = (e_errors.OK, None)
        # look up in our dictionary the request bit field id
        self.reply_to_caller(ticket)
        Trace.log(e_errors.INFO, 'set_deleted %s'%(ticket,))
        return

    #### DONE
    # change the delete state element in the dictionary
    def del_bfid(self, ticket):

        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.

        # This is a restricted service
        status = self.restricted_access()
        if status:
            msg = "attempt to delete file %s from %s"%(bfid, self.reply_address[0])
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = status
            self.reply_to_caller(ticket)
            return

        # now just delete the bfid
        del self.filedb_dict[bfid]
        Trace.log(e_errors.INFO, "bfid %s has been removed from DB"%(bfid,))

        # and return to the caller
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(12,'del_bfid %s'%(ticket,))
        return

    #### DONE
    # __erase_volume(self, vol) -- delete all files belonging to vol
    #
    # This is only the file clerk portion of erasing a volume.
    # A complete erasing needs to erase volume information too.
    #
    # This involves removing volmap directory in /pnfs.
    # If it fails, nothing would be done further.

    def __erase_volume(self, vol):
        Trace.log(e_errors.INFO, 'erasing files of volume %s'%(vol))
        try:
            bfids = self.get_all_bfids(vol)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = "__erase_volume(): can not get bfids for '%s' %s %s"%(vol, str(exc_type), str(exc_value))
            Trace.log(e_errors.ERROR, msg)
            return e_errors.ERROR, msg

        # remove file record
        for bfid in bfids:
            try:
                del self.filedb_dict[bfid]
            except:
                exc_type, exc_value = sys.exc_info()[:2]
                msg = "__erase_volume(): failed to remove record '%s' %s %s"%(bfid, str(exc_type), str(exc_value))
                Trace.log(e_errors.ERROR, msg)
                return e_errors.ERROR, msg

        Trace.log(e_errors.INFO, 'files of volume %s are erased'%(vol))
        return e_errors.OK, None

    # erase_volume -- server service
    #### DONE
    def erase_volume(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label or external_label == (None, None):
            return #extract_external_lable_from_ticket handles its own errors.

        ticket["status"] = (e_errors.OK, None)
        # catch any failure
        try:
            ticket['status'] = self.__erase_volume(external_label)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = 'erase failed due to: '+str(exc_type)+' '+str(exc_value)
            Trace.log(e_errors.ERROR, msg)
            ticket["status"] = (e_errors.FILE_CLERK_ERROR, msg)
        # and return to the caller
        self.reply_to_caller(ticket)
        return

    # __delete_volume(self, vol) -- mark all files belonging to vol as deleted
    #
    # Note: this is NOT the counter part of __delete_volume() in
    #       volume clerk, which is simply a renaming to *.deleted
    #### DONE
    def __delete_volume(self, vol):
        Trace.log(e_errors.INFO, 'marking files of volume %s as deleted'%(vol))
        bfids = self.get_all_bfids(vol)
        for bfid in bfids:
            record = self.filedb_dict[bfid]
            if record['deleted'] != 'yes':
                record['deleted'] = 'yes'
                self.filedb_dict[bfid] = record
        Trace.log(e_errors.INFO, 'all files of volume %s are marked deleted'%(vol))
        return

    #### DONE
    # delete_volume -- server service

    def delete_volume(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label or external_label == (None, None):
            return #extract_external_lable_from_ticket handles its own errors.

        ticket["status"] = (e_errors.OK, None)
        # catch any failure
        try:
            self.__delete_volume(external_label)
        except:
            ticket["status"] = (e_errors.FILE_CLERK_ERROR, "delete failed")
        # and return to the caller
        self.reply_to_caller(ticket)
        return

    def start_backup(self, ticket):
        try:
            Trace.log(e_errors.INFO, "start_backup")
            self.filedb_dict.start_backup()
            ticket['status'] = (e_errors.OK, None)
            ticket['start_backup'] = "yes"
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg = sys.exc_info()[:2]
            ticket['status'] = str(exc), str(msg)
            Trace.log(e_errors.ERROR, "start_backup %s" % (ticket['status'],))
            ticket['start_backup'] = "no"

        self.reply_to_caller(ticket)


    def stop_backup(self, ticket):
        try:
            Trace.log(e_errors.INFO,"stop_backup")
            self.filedb_dict.stop_backup()
            ticket['status'] = (e_errors.OK, None)
            ticket['stop_backup'] = "yes"
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg = sys.exc_info()[:2]
            ticket['status'] = str(exc), str(msg)
            Trace.log(e_errors.ERROR, "stop_backup %s" % (ticket['status'],))
            ticket['stop_backup'] = "no"

        self.reply_to_caller(ticket)

    def backup(self, ticket):
        try:
            Trace.log(e_errors.INFO,"backup")
            self.filedb_dict.backup()
            ticket['status'] = (e_errors.OK, None)
            ticket['backup'] = "yes"
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg = sys.exc_info()[:2]
            ticket['status'] = str(exc), str(msg)
            Trace.log(e_errors.ERROR, "backup %s" % (ticket['status'],))
            ticket['backup'] = "no"

        self.reply_to_caller(ticket)

    def mark_bad(self, ticket):

        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.

        path = self.extract_value_from_ticket('path', ticket)
        if not path:
            return #extract_value_from_ticket handles its own errors.

        # check if this file has already been marked bad
        q = "select * from bad_file where bfid = '%s';"%(bfid)
        res = self.filedb_dict.query_dictresult(q)
        if res:
            msg = "file %s has already been marked bad"%(bfid)
            ticket["status"] = (e_errors.FILE_CLERK_ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # insert into database
        q = "insert into bad_file (bfid, path) values('%s', '%s');"%(
            bfid, path)
        try:
            res = self.filedb_dict.insert(q)
            ticket['status'] = (e_errors.OK, None)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = "failed to mark %s bad due to "%(bfid)+str(exc_type)+' '+str(exc_value)
            ticket["status"] = (e_errors.FILE_CLERK_ERROR, msg)
            Trace.log(e_errors.KEYERROR, msg)

        self.reply_to_caller(ticket)
        return

    def unmark_bad(self, ticket):

        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.

        q = "delete from bad_file where bfid = '%s';" %(bfid)
        try:
            res = self.filedb_dict.remove(q)
            ticket['status'] = (e_errors.OK, None)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = "failed to unmark %s bad due to "%(bfid)+str(exc_type)+' '+str(exc_value)
            ticket["status"] = (e_errors.FILE_CLERK_ERROR, msg)
            Trace.log(e_errors.KEYERROR, msg)

        self.reply_to_caller(ticket)
        return

    # made_copy(ticket) -- decrease copies count
    #                    if the count becomes zero, delete the record
    def made_copy(self, ticket):
        bfid, record = self.extract_bfid_from_ticket(ticket)
        if not bfid:
            return #extract_bfid_from_ticket handles its own errors.

	ticket['status'] = (e_errors.OK, "")
	rc = self._made_copy(ticket['bfid'])
	if rc != 0:
	    ticket['status'] = (e_errors.ERROR, "")
        self.reply_to_caller(ticket)
        return

    # __migration(): Underlying function that exectutes the SQL commands
    # for the migration state [un]setting functions.
    #
    # query - An SQL statement to execute; one that does not return any values.
    # ticket - The original ticket received; the status field is set.
    def __migration(self, query, ticket):
        try:
            self.filedb_dict.insert(query)

            ticket['status'] = (e_errors.OK, None)
	    #
	    # edb module raises underlying DB errors as EnstoreError.
	    #
        except e_errors.EnstoreError, msg:
            if str(msg).find("duplicate key violates unique constraint") != -1:
                #The old unique constraint was on each src & dst
                # column.  For modern migration capable of migrating
                # to multiple copies this constraint needs to be on
                # the pair of src & dst columns.
                ticket['status'] = (msg.type,
                                    "The database has an obsolete unique key constraint.")
            else:
                ticket['status'] = (msg.type, str(msg))
        except:
            ticket['status'] = (e_errors.FILE_CLERK_ERROR, str(msg))

        return ticket

    # __set_migration() & __unset_migration(): Intermidiate functions used
    # to execute SQL commands for the migration state [un]setting functions.
    #
    # q_base - The string with the SQL command; the python %s string insert
    #          substrings are allowed.  This function fills them in.
    #          For set functions:  The first one must be for setting the
    #                              timestamp, the second for giving the
    #                              source bfid, the third is the destination
    #                              bfid.
    #          For unset_functions:  The first one is the source bfid,
    #                                the second one is the destination bfid.
    # ticket - The original ticket received; the status field is set.

    def __set_migration(self, q_base, ticket):
        # extract the additional information if source and/or destination
        # information is requested.
        src_bfid, src_record = self.extract_bfid_from_ticket(ticket,
                                                             key = 'src_bfid')
        if not src_bfid:
            return #extract_bfid_from_ticket handles its own errors.
        dst_bfid, dst_record = self.extract_bfid_from_ticket(ticket,
                                                             key = 'dst_bfid')
        if not dst_bfid:
            return #extract_bfid_from_ticket handles its own errors.

        q = q_base % (time2timestamp(time.time()), src_bfid, dst_bfid)

        return self.__migration(q, ticket)

    def __unset_migration(self, q_base, ticket):
        # extract the additional information if source and/or destination
        # information is requested.
        src_bfid, src_record = self.extract_bfid_from_ticket(ticket,
                                                             key = 'src_bfid')
        if not src_bfid:
            return #extract_bfid_from_ticket handles its own errors.
        dst_bfid, dst_record = self.extract_bfid_from_ticket(ticket,
                                                             key = 'dst_bfid')
        if not dst_bfid:
            return #extract_bfid_from_ticket handles its own errors.

        q = q_base % (src_bfid, dst_bfid)

        return self.__migration(q, ticket)

    #set_copied(): Insert into the migration table a new record showing
    #   that the file has been copied to a new tape.
    def set_copied(self, ticket):
        q_base = "insert into migration (copied, src_bfid, dst_bfid) \
                  values ('%s', '%s', '%s');"

        ticket = self.__set_migration(q_base, ticket)

        self.reply_to_caller(ticket)
        return

    #unset_copied():  First, remove the copied status from the record,
    #   then if no errors occured remove the entire row.
    def unset_copied(self, ticket):
        q_base = "update migration set copied = NULL where \
                  src_bfid = '%s' and dst_bfid = '%s';"

        ticket = self.__unset_migration(q_base, ticket)
        if not e_errors.is_ok(ticket):
            self.reply_to_caller(ticket)

        q_base = "delete from migration where \
                  src_bfid = '%s' and dst_bfid = '%s';"

        ticket = self.__unset_migration(q_base, ticket)

        self.reply_to_caller(ticket)
        return

    #set_swapped():  Updated record to indicate the source and destination
    #   files have been swapped.
    def set_swapped(self, ticket):
        q_base = "update migration set swapped = '%s' where \
                  src_bfid = '%s' and dst_bfid = '%s';"

        ticket = self.__set_migration(q_base, ticket)

        self.reply_to_caller(ticket)
        return

    #unset_swapped():  Remove the swapped status from the migration record.
    def unset_swapped(self, ticket):
        q_base = "update migration set swapped = NULL where \
                  src_bfid = '%s' and dst_bfid = '%s';"

        ticket = self.__unset_migration(q_base, ticket)

        self.reply_to_caller(ticket)
        return

    #set_checked():  Updated record to indicate the migration destination
    #   file has been scanned/checked.
    def set_checked(self, ticket):
        q_base = "update migration set checked = '%s' where \
                  src_bfid = '%s' and dst_bfid = '%s';"

        ticket = self.__set_migration(q_base, ticket)

        self.reply_to_caller(ticket)
        return

    #unset_checked():  Remove the checked status from the migration record.
    def unset_checked(self, ticket):
        q_base = "update migration set checked = NULL where \
                  src_bfid = '%s' and dst_bfid = '%s';"

        ticket = self.__set_migration(q_base, ticket)

        self.reply_to_caller(ticket)
        return

    #set_closed():  Update record to indicate the migration has been completed.
    def set_closed(self, ticket):
        q_base = "update migration set closed = '%s' where \
                  src_bfid = '%s' and dst_bfid = '%s';"

        ticket = self.__set_migration(q_base, ticket)

        self.reply_to_caller(ticket)
        return

    #unset_closed():  Remove the closed status from the migration record.
    def unset_closed(self, ticket):
        q_base = "update migration set closed = NULL where \
                  src_bfid = '%s' and dst_bfid = '%s';"

        ticket = self.__set_migration(q_base, ticket)

        self.reply_to_caller(ticket)
        return


class FileClerk(FileClerkMethods, generic_server.GenericServer):

    def __init__(self, csc):
        generic_server.GenericServer.__init__(self, csc, MY_NAME,
                                              function = self.handle_er_msg)

        Trace.init(self.log_name)

        FileClerkMethods.__init__(self, csc)

        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        self.alive_interval = monitored_server.get_alive_interval(self.csc,
                                                                  MY_NAME,
                                                                  self.keys)

        #Setup the error handling to reconnect to the database if the
        # connection is broken.
        self.set_error_handler(self.file_error_handler)
        self.connection_failure = 0
        # setup the communications with the event relay task
        self.erc.start([event_relay_messages.NEWCONFIGFILE])
        # start our heartbeat to the event relay process
        self.erc.start_heartbeat(enstore_constants.FILE_CLERK,
                                 self.alive_interval)

	threading.Thread(target=self.replay_cache_written_events).start()
	self.file_check_thread = threading.Thread(target=self.check_files_in_transition)
	self.file_check_thread.start()

    def get_files_in_transition(self, all=None):
        if all:
            q=SELECT_ALL_FILES_IN_TRANSITION
        else:
	    q=SELECT_FILES_IN_TRANSITION
        res = self.filedb_dict.query_getresult(q)
        return res

    def replay_cache_written_events(self, all=None):
	    res=self.get_files_in_transition(all)
	    for row in res:
		    bfid = row[0]
		    self.replay_cache_written_event(bfid)

    def replay_cache_written_event(self,bfid):
	    record=self.filedb_dict[bfid]
	    if record.get("original_library",None) :
		    event = pe_client.evt_cache_written_fc({ "fc" : { "original_library"  : record.get("original_library",None),
								      "file_family_width" : record.get("file_family_width",None)
								      }
							     }, record)
		    try:
			    self.en_qpid_client.send(event)
			    Trace.log(e_errors.INFO,"Succesfully replayed CACHE_WRITTEN event for %s"%(bfid,))
		    except:
			    Trace.log(e_errors.INFO,"Failed replay CACHE_WRITTEN event for %s"%(bfid,))
			    pass

    def replay(self,ticket):
        if self.en_qpid_client :
            func_name="self."+ticket.get("func")
            func=eval(func_name)
            arg = ticket.get("args")
            ticket["status"] = (e_errors.OK, None)
            try:
                func(arg)
            except:
                ticket["status"] = (str(sys.exc_info()[0]),str(sys.exc_info()[1]))
        else:
            ticket["status"] = (e_errors.ERROR, "No qpid client defined, check configuration")
        self.reply_to_caller(ticket)


    def check_files_in_transition(self) :
	    q=SELECT_FILES_IN_TRANSITION
	    while True:
		    res = self.filedb_dict.query_getresult(q)
		    if len(res)>0:
			    txt=""
			    f=open("/tmp/FILES_IN_TRANSITION","w")
			    for row in res :
				    txt += "%s\n"%(row[0])
			    f.write("%s"%(txt,))
			    f.close();
			    inq_d = self.csc.get(enstore_constants.INQUISITOR, {})
			    html_dir=inq_d.get("html_file",enstore_files.default_dir)
			    html_host=inq_d.get("host","localhost")
			    cmd="$ENSTORE_DIR/sbin/enrcp %s %s:%s"%(f.name,html_host,html_dir)
			    rc = enstore_functions2.shell_command2(cmd)
			    failed=False
			    if rc :
				    if rc[0] !=0 :
					    failed=True
					    txt = "Failed to execute command %s\n. Output was %s\n. List of files: %s\n"%(cmd, rc[2], txt,)

			    else :
				    failed=True
				    txt = "Failed to execute command %s\n. List of files: %s\n"%(cmd, txt,)
			    if not failed :
				    txt = 'See <A HREF="FILES_IN_TRANSITION">FILES_IN_TRANSITION</A>'
			    Trace.alarm(e_errors.WARNING, " %d files stuck in files_in_transition table"%len(res), txt )
		    time.sleep(FILES_IN_TRANSITION_CHECK_INTERVAL)



    def file_error_handler(self, exc, msg, tb):
        __pychecker__ = "unusednames=tb"
        # is it PostgreSQL connection error?
        #
        # This is indeed a OR condition implemented in if-elif-elif-...
        # so that each one can be specified individually
        if exc == edb.pg.ProgrammingError and str(msg)[:13] == 'server closed':
            self.reconnect(msg)
        elif exc == ValueError and str(msg)[:13] == 'server closed':
            self.reconnect(msg)
        elif exc == TypeError and str(msg)[:10] == 'Connection':
            self.reconnect(msg)
        elif exc == ValueError and str(msg)[:13] == 'no connection':
            self.reconnect(msg)
        self.reply_to_caller({'status':(str(exc),str(msg), 'error'),
            'exc_type':str(exc), 'exc_value':str(msg)} )

    # reconnect() -- re-establish connection to database
    def reconnect(self, msg="unknown reason"):
        try:
            self.filedb_dict.reconnect()
            Trace.alarm(e_errors.WARNING, "RECONNECT", "reconnect to database due to "+str(msg))
            self.connection_failure = 0
        except:
            Trace.alarm(e_errors.ERROR, "RECONNECTION FAILURE",
                "Is database server running on %s:%d?"%(self.filedb_dict.host,
                self.filedb_dict.port))
            self.connection_failure = self.connection_failure + 1
            if self.connection_failure > MAX_CONNECTION_FAILURE:
                pass	# place holder for future RED BALL


    def quit(self, ticket):
	self.filedb_dict.close()
	# set sentinel
	self.sequentialThreadQueue.put(None)
	for process in self.parallelThreads:
		self.parallelThreadQueue.put(None)
	self.sequentialWorker.join(10.)
	for process in self.parallelThreads:
		process.join(10.)
	dispatching_worker.DispatchingWorker.quit(self, ticket)


class FileClerkInterface(generic_server.GenericServerInterface):
    pass

if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME))

    # get the interface
    intf = FileClerkInterface()

    # get a file clerk
    fc = FileClerk((intf.config_host, intf.config_port))
    fc.handle_generic_commands(intf)

    Trace.log(e_errors.INFO, '%s' % (sys.argv,))

    while 1:
        try:
            Trace.log(e_errors.INFO, "File Clerk (re)starting")
            fc.serve_forever()
        except edb.pg.Error, exp:
            fc.reconnect(exp)
            continue
        except SystemExit, exit_code:
            # fc.dict.close()
            sys.exit(exit_code)
        except:
            fc.serve_forever_error(fc.log_name)
            fc.reconnect("paranoid")
            continue
    Trace.trace(e_errors.ERROR,"File Clerk finished (impossible)")
