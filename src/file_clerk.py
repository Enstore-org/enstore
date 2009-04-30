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
import string
import socket
import select

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

MY_NAME = enstore_constants.FILE_CLERK   #"file_clerk"
MAX_CONNECTION_FAILURE = 5

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
        db_host = dbInfo['db_host']
        db_port = dbInfo['db_port']

        #Open conection to the Enstore DB.
        Trace.log(e_errors.INFO, "opening file database using edb.FileDB")
        try:
            self.filedb_dict = edb.FileDB(host=db_host, port=db_port, jou=jouHome)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            message = str(exc_type)+' '+str(exc_value)+' IS POSTMASTER RUNNING?'
            Trace.log(e_errors.ERROR, message)
            Trace.alarm(e_errors.ERROR, message, {})
            Trace.log(e_errors.ERROR, "CAN NOT ESTABLISH DATABASE CONNECTION ... QUIT!")
            sys.exit(1)

    ####################################################################

    # These extract value functions are used to get a value from the ticket
    # and perform validity checks in a consistant fashion.  These functions
    # duplicated in volume_clerk.py; they should be made more generic to
    # eliminate maintaining two sets of identical code.
            
    def extract_value_from_ticket(self, key, ticket, fail_None = False):
        try:
            value = ticket[key]
        except KeyError, detail:
            message =  "%s: key %s is missing" % (MY_NAME, detail,)
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

    #This functions uses an acitve protocol.  This function uses UDP and TCP.
    def reply_to_caller_with_long_answer_part1(self, ticket, long_items = []):

        if not e_errors.is_ok(ticket):
            #If we have an error, then we only need to reply and skip the rest.
            self.reply_to_caller(ticket)
            return None

        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)

        ticket['callback_addr'] = (host, port)

        #The initial over UDP message needs to be small.
        reply = ticket.copy()
        for name in long_items:
            try:
                del reply[name]
            except KeyError:
                pass
        #Tell the client to wait for a connection.
        self.reply_to_caller(reply)

        #Wait for the client to connect over TCP.
        r, w, x = select.select([listen_socket], [], [], 60)
        if not r:
            listen_socket.close()
            message = "connection timedout from %s" % (ticket['r_a'],)
            Trace.log(e_errors.ERROR, message)
            return None

        #Accept the servers connection.
        control_socket, address = listen_socket.accept()
        
        #Veify that this connection is made from an acceptable
        # IP address.
        if not hostaddr.allow(address):
            control_socket.close()
            listen_socket.close()
            message = "address %s not allowed" % (address,)
            Trace.log(e_errors.ERROR, message)
            return None

        #Socket cleanup.
        listen_socket.close()

        return control_socket
        
    #Generalize the code to have a really large ticket be returned.
    #This functions uses an acitve protocol.  This function uses UDP and TCP.
    def reply_to_caller_with_long_answer_part2(self, control_socket, ticket):
        try:
            #Write reply on control socket.
            callback.write_tcp_obj_new(control_socket, ticket)
        except (socket.error), msg:
            message = "failed to use control socket: %s" % (str(msg),)
            Trace.log(e_errors.NET_ERROR, message)

        #Socket cleanup.
        control_socket.close()

    #Generalize the code to have a really large ticket be returned.
    #This functions uses an acitve protocol.  This function uses UDP and TCP.
    #
    # The 'ticket' is sent over the network.
    # 'long_items' is a list of elements that should be supressed in the
    # initial UDP response.
    def reply_to_caller_with_long_answer(self, ticket, long_items = []):
        control_socket = self.reply_to_caller_with_long_answer_part1(ticket, long_items)
        if not control_socket:
            return

        self.reply_to_caller_with_long_answer_part2(control_socket, ticket)
        
    ####################################################################

    #### DONE
    # get_all_bfids(external_label) -- get all bfids of a particular volume

    def get_all_bfids(self, external_label):
        q = "select bfid, location_cookie from file, volume\
             where volume.label = '%s' and \
                   file.volume = volume.id \
                   order by location_cookie;"%(external_label)
        res = self.filedb_dict.db.query(q).getresult()
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
            for i in self.filedb_dict.db.query(q).getresult():
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
        except (edb.pg.ProgrammingError, edb.pg.InternalError), msg:
            ticket["copies"] = []
            ticket["status"] = (e_errors.DATABASE_ERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    # _find_original(bfid) -- find its original
    # there should eb at most one original!
    def _find_original(self, bfid):
        q = "select bfid from file_copies_map where alt_bfid = '%s';"%(bfid)
        try:
            res = self.filedb_dict.db.query(q).getresult()
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
        except (edb.pg.ProgrammingError,  edb.pg.InternalError), msg:
            ticket["original"] = None
            ticket["status"] = (e_errors.DATABASE_ERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    # find any information if this file has been involved in migration
    # or duplication
    def __find_migrated(self, bfid):
        src_list = []
        dst_list = []
        
        q = "select src_bfid,dst_bfid from migration where (dst_bfid = '%s' or src_bfid = '%s') ;" % (bfid, bfid)
        
        res = self.filedb_dict.db.query(q).getresult()
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
        except (edb.pg.ProgrammingError,  edb.pg.InternalError), msg:
            ticket["src_bfid"] = None
            ticket["dst_bfid"] = None
            ticket["status"] = (e_errors.DATABASE_ERROR, str(msg))
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
        res = self.filedb_dict.db.query(q)
        return res.ntuples()

    #### DONE
    # has_undeleted_file -- server service

    def has_undeleted_file(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label:
            return #extract_external_lable_from_ticket handles its own errors.

        # catch any failure
        try:
            result = self.__has_undeleted_file(external_label)
            ticket["status"] = (e_errors.OK, result)
        except (edb.pg.ProgrammingError,  edb.pg.InternalError), msg:
            ticket["status"] = (e_errors.DATABASE_ERROR, str(msg))
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
        if not external_label:
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
    # reply_to_caller_with_long_answer().
    def get_bfids2(self, ticket):
        
        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label:
            return #extract_external_lable_from_ticket handles its own errors.

        # get bfids
        bfid_list = self.get_all_bfids(external_label)

        # send the reply
        ticket['bfids'] = bfid_list
        ticket["status"] = (e_errors.OK, None)
        try:
            self.reply_to_caller_with_long_answer(ticket, ["bfids"])
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "get_bfids2: %s" % (str(msg),))
            return

        return

    #If export_format is False, the default, then the raw names from the
    # db query are returned in the dictionary.  If export_format is True,
    # then the edb.py file DB export_format() function is called to
    # rename the keys from the query.
    def __tape_list(self, external_label, export_format = False):
        q = "select bfid, crc, deleted, drive, volume.label, \
                    location_cookie, pnfs_path, pnfs_id, \
                    sanity_size, sanity_crc, size \
             from file, volume \
             where \
                 file.volume = volume.id and volume.label = '%s' \
             order by location_cookie;" % (external_label,)

        res = self.filedb_dict.db.query(q).dictresult()

        # convert to external format
        file_list = []
        for file_info in res:
            if export_format:
                # used for tape_list3()
                value = self.filedb_dict.export_format(file_info)
            else:
                # used for tape_list2()
                value = file_info
            if not value.has_key('pnfs_name0'):
                value['pnfs_name0'] = "unknown"
            file_list.append(value)

        return file_list

    #### DONE
    def tape_list(self, ticket):
        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label:
            return #extract_external_lable_from_ticket handles its own errors.

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket,ticket)

        # log the activity
        Trace.log(e_errors.INFO, "start listing " + external_label)
        
        vol = self.__tape_list(external_label)

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
        if not external_label:
            return #extract_external_lable_from_ticket handles its own errors.

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket, ticket)

        # log the activity
        Trace.log(e_errors.INFO, "start listing " + external_label + " (2)")

        vol = self.__tape_list(external_label)
        

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
    # reply_to_caller_with_long_answer().
    def tape_list3(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label:
            return #extract_external_lable_from_ticket handles its own errors.

        # log the activity
        Trace.log(e_errors.INFO, "start listing " + external_label + " (3)")

        # start communication
        ticket["status"] = (e_errors.OK, None)
        try:
            control_socket = self.reply_to_caller_with_long_answer_part1(ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "tape_list3(): %s" % (str(msg),))
            return

        #Make sure the socket exists.
        if not control_socket:
            return

        # get reply
        file_info = self.__tape_list(external_label, export_format = True)
        ticket['tape_list'] = file_info

        # send the reply
        try:
            self.reply_to_caller_with_long_answer_part2(control_socket, ticket)
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
        if not external_label:
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

        res = self.filedb_dict.db.query(q).dictresult()

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

    	return self.filedb_dict.db.query(q).getresult()


    # list_active2(self, ticket) -- list the active files on a volume
    #	 only the /pnfs path is listed
    #	 the purpose is to generate a list for deletion before the
    #	 deletion of a volume

    def list_active2(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label:
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
    # reply_to_caller_with_long_answer().
    def list_active3(self, ticket):

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label:
            return #extract_external_lable_from_ticket handles its own errors.

        # start communication
        ticket["status"] = (e_errors.OK, None)
        try:
            control_socket = self.reply_to_caller_with_long_answer_part1(ticket)
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
            self.reply_to_caller_with_long_answer_part2(control_socket, ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "list_active3(): %s" % (str(msg),))
            return

    def __show_bad(self):
        q = "select label, bad_file.bfid, size, path \
             from bad_file, file, volume \
             where \
                 bad_file.bfid = file.bfid and \
                 file.volume = volume.id;"
        return self.filedb_dict.db.query(q).dictresult()

        
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
    # reply_to_caller_with_long_answer().
    def show_bad2(self, ticket):

        # get bad files
        bad_files = self.__show_bad()

        # send the reply
        ticket['bad_files'] = bad_files
        ticket["status"] = (e_errors.OK, None)
        try:
            self.reply_to_caller_with_long_answer(ticket, ["bad_files"])
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "show_bad2: %s" % (str(msg),))
            return

        return


class FileClerkMethods(FileClerkInfoMethods):

    def __init__(self, csc):
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

        #Set the brand.
        self.set_brand(brand)

    ####################################################################

    ###
    ### These functions are internal file_clerk functions.
    ###

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
        bfid = time.time()
        bfid = long(bfid)*100000
        while self.filedb_dict.has_key(self.brand+str(bfid)):
            bfid = bfid + 1
        return self.brand+str(bfid)

    # register_copy(original, copy) -- register copy of original
    def register_copy(self, original, copy):
        Trace.log(e_errors.INFO,
                  'register copy %s of original %s' % (copy, original))
	q = "insert into file_copies_map (bfid, alt_bfid) values ('%s', '%s');"%(original, copy)
        try:
            res = self.filedb_dict.db.query(q)
        except:
            return 1
        return

    # log_copies(bfid, n) -- log number of copies to make
    def log_copies(self, bfid, n):
        Trace.log(e_errors.INFO, "log_copies: %s, %d"%(bfid, n))
        q = "insert into active_file_copying (bfid, remaining) \
                values ('%s', %d);"%(bfid, n)
        try:
            res = self.filedb_dict.db.query(q)
        except:
            return 1
        return

    # made_copy(bfid) -- decrease copies count
    #                    if the count becomes zero, delete the record
    def made_copy(self, bfid):
        q = "select * from active_file_copying where bfid = '%s';"%(bfid)
        res = self.filedb_dict.db.query(q).dictresult()
        if not res:
            Trace.log(e_errors.ERROR, "made_copy(): %s does not have copies"%(bfid))
            return
        if res[0]['remaining'] <= 1:
            # all done, delete this entry
            q = "delete from active_file_copying where bfid = '%s';"%(bfid)
            try:
                res = self.filedb_dict.db.query(q)
            except:
                return 1
        else: # decrease the number
            q = "update active_file_copying set remaining = %d where bfid = '%s';"%(res[0]['remaining'] - 1, bfid)
            try:
                res = self.filedb_dict.db.query(q)
            except:
                return 1
        return

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

        # take care of the copy count
        original = self._find_original(bfid)
        if original:
            self.made_copy(original)

        # record our changes
        self.filedb_dict[bfid] = record 
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(12,'set_pnfsid %s'%(ticket,))
        return
    
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
        if not external_label:
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
        if not external_label:
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
        res = self.filedb_dict.db.query(q).dictresult()
        if res:
            msg = "file %s has already been marked bad"%(bfid)
            ticket["status"] = (e_errors.FILE_CLERK_ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # insert into database
        q = "insert into bad_file (bfid, path) values('%s', '%s');"%(
            bfid, path)
        try:
            res = self.filedb_dict.db.query(q)
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
            res = self.filedb_dict.db.query(q)
            ticket['status'] = (e_errors.OK, None)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = "failed to unmark %s bad due to "%(bfid)+str(exc_type)+' '+str(exc_value)
            ticket["status"] = (e_errors.FILE_CLERK_ERROR, msg)
            Trace.log(e_errors.KEYERROR, msg)

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

    """
    # find the brand
    Trace.log(e_errors.INFO,"find the brand")
    try:
        brand = configuration_client.ConfigurationClient(
                (intf.config_host, intf.config_port)).get('file_clerk')['brand']
        Trace.log(e_errors.INFO,"The brand is %s"%(brand))
    except:
        brand = string.upper(string.split(os.uname()[1], ".")[0][:2])+'MS'
        Trace.log(e_errors.INFO,"No brand is found, using '%s'"%(brand))

    fc.set_brand(brand)

    Trace.log(e_errors.INFO,"determine dbHome and jouHome")
    try:
        dbInfo = configuration_client.ConfigurationClient(
                (intf.config_host, intf.config_port)).get('database')
        dbHome = dbInfo['db_dir']
        try:  # backward compatible
            jouHome = dbInfo['jou_dir']
        except:
            jouHome = dbHome
    except:
        dbHome = os.environ['ENSTORE_DIR']
        jouHome = dbHome

    db_host = dbInfo['db_host']
    db_port = dbInfo['db_port']

    Trace.log(e_errors.INFO,"opening file database using edb.FileDB")
    try:
        fc.dict = edb.FileDB(host=db_host, port=db_port, jou=jouHome)
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        msg = str(exc_type)+' '+str(exc_value)+' IS POSTMASTER RUNNING?'
        Trace.log(e_errors.ERROR,msg)
        Trace.alarm(e_errors.ERROR,msg, {})
        Trace.log(e_errors.ERROR, "CAN NOT ESTABLISH DATABASE CONNECTION ... QUIT!")
        sys.exit(1) 

    """
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
