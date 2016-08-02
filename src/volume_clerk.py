#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import Queue
import sys
import os
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
import enstore_constants
import monitored_server
import inquisitor_client
import cPickle
import event_relay_messages
import udp_common
import enstore_functions2
import enstore_functions3

# conditional comparison
def mycmp(cond, a, b):
    # condition may be None or some other
    if not cond: return a==b        # if cond is not None use ==
    else: return a!=b               # else use !=

KB=enstore_constants.KB
MB=enstore_constants.MB
GB=enstore_constants.GB

# make pychecker happy

if GB:
    pass

# require 5% more space on a tape than the file size,
#    this accounts for the wrapper overhead and "some" tape rewrites

SAFETY_FACTOR=enstore_constants.SAFETY_FACTOR
MIN_LEFT=enstore_constants.MIN_LEFT

MY_NAME = enstore_constants.VOLUME_CLERK   #"volume_clerk"
MAX_CONNECTION_FAILURE = enstore_constants.MAX_CONNECTION_FAILURE

PARALLEL_QUEUE_SIZE=enstore_constants.PARALLEL_QUEUE_SIZE
MAX_THREADS =  enstore_constants.MAX_THREADS
# we have run into issues with volume_clerk locking up
# when number of max threads was more than number of max
# connections. Set it to max number of threads + 1 (counting
# main thread)
MAX_CONNECTIONS=MAX_THREADS+1

class VolumeClerkInfoMethods(dispatching_worker.DispatchingWorker):
    ### This class of Volume Clerk methods should only be readonly operations.
    ### This class is inherited by Info Server (to increase code reuse)
    ### and we don't want the Info Server to have the ability to modify
    ### anything.  Also, any privledged/admin inquiries should not go
    ### here either.


    def __init__(self, csc):
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


    def extract_external_label_from_ticket(self, ticket,
                                           key = "external_label",
                                           check_exists = True):

        external_label = self.extract_value_from_ticket(key, ticket,
                                                        fail_None = True)
        if not external_label:
            if check_exists:
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
            return None, None

        record = None
        if check_exists :
            #Make sure the volume exists.
            record =  self.volumedb_dict[external_label]
            if not record:
                message = "%s: no such external_label %s" \
                          % (MY_NAME, external_label,)
                ticket["status"] = (e_errors.NO_VOLUME, message)
                Trace.log(e_errors.ERROR, message)
                self.reply_to_caller(ticket)
                return None, None

        if check_exists:
            return external_label, record
        else:
            return external_label

    ####################################################################

    #### DONE
    # get a port for the data transfer
    # tell the user I'm your volume clerk and here's your ticket
    def get_user_sockets(self, ticket):
        try:
            addr = ticket['callback_addr']
            if not hostaddr.allow(addr):
                return 0
            volume_clerk_host, volume_clerk_port, listen_socket = callback.get_callback()
            listen_socket.listen(4)
            ticket["volume_clerk_callback_addr"] = (volume_clerk_host, volume_clerk_port)
	    address_family = socket.getaddrinfo(volume_clerk_host, None)[0][0]
            self.control_socket = socket.socket(address_family, socket.SOCK_STREAM)
            self.control_socket.connect(addr)
            callback.write_tcp_obj(self.control_socket, ticket)

            r,w,x = select.select([listen_socket], [], [], 15)
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
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.handle_error(exc, msg)
            return 0
        return 1

    ####################################################################

    # has_undeleted_file(vol) -- check if vol has undeleted file

    def has_undeleted_file(self, vol):
        q = "select unknown_files,active_files from volume where label = '%s'"%(vol)
        res = self.volumedb_dict.query_getresult(q)
        if len(res)>0:
            return (res[0][0]>0 or res[0][1]>0)
        else:
            return False

    # check if quota is enabled in the configuration #### DONE
    def quota_enabled2(self):
        q_dict = self.csc.get('quotas')
        if q_dict['status'][0] == e_errors.KEYERROR:
            # no quota defined in the configuration
            return None
        enabled = q_dict.get('enabled',None)
        if not enabled:
            # enabled key does not exist. Wrong cofig.
            return None
        if 'y' not in string.lower(enabled):
            # do not use quota
            return None
        else:
            return q_dict

    # it is backward compatible with old quota_enabled()
    def quota_enabled(self):
        q = "select value from option where key = 'quota'"
        state = self.volumedb_dict.query_getresult(q)[0][0]
        if state != "enabled":
            return None
        q = "select library, storage_group, quota, significance from quota;"
        res = self.volumedb_dict.query_dictresult(q)
        libraries = {}
        order = {'bottom':[], 'top':[]}
        for i in res:
            if not libraries.has_key(i['library']):
                libraries[i['library']] = {}
            libraries[i['library']][i['storage_group']] = i['quota']
            if i['significance'] == 'y':
                order['top'].append((i['library'], i['storage_group']))
            else:
                order['bottom'].append((i['library'], i['storage_group']))
        q_dict = {
            'enabled': 'yes',
            'libraries': libraries,
            'order': order
        }

        return q_dict

    # check quota #### DONE
    def check_quota(self, quotas, library, storage_group):
        if not quotas.has_key('libraries'):
            Trace.log(e_errors.ERROR, "Wrong quota config")
            return 0

        if quotas['libraries'].has_key(library):
            if not quotas['libraries'][library].has_key(storage_group):
                return 1
            vol_count=self.get_sg_counter(library, storage_group)
            quota = quotas['libraries'][library].get(storage_group, 0)
            Trace.log(e_errors.INFO, "storage group %s, vol counter %s, quota %s" % (storage_group, vol_count, quota))
            if quota == 0 or (vol_count >= quota):
                return 0
            else:
                # advanced alarm
                # if the head room is less than 3 volumes or the count
                # exceeds 95%
                dq = quota - vol_count
                # at quota?
                if dq == 1:
                    message = "(%s, %s) has reached its quota limit (%d/%d)" \
                              % (library, storage_group, vol_count + 1, quota)
                    Trace.alarm(e_errors.WARNING, 'QUOTA LIMIT REACHED', message)
                elif dq < 3 or dq < (quota * 0.05):
                    message = "(%s, %s) is approaching its quota limit (%d/%d)"\
                              % (library, storage_group, vol_count+1, quota)
                    Trace.alarm(e_errors.WARNING, 'APPROACHING QUOTA LIMIT', message)
                return 1
        else:
            message = "no library %s defined in the quota configuration" \
                      % (library,)
            Trace.log(e_errors.INFO, message)
            return 1
        return 0

    ####################################################################

    # The following functions are run by dispatching worker in response to
    # a ticket request ariving.  Helper __name() functions are left next to
    # their non-doubleunderscore counterpart.

    # show_state -- show internal configuration values
    def show_state(self, ticket):
        ticket['state'] = {}
        for i in self.__dict__.keys():
            ticket['state'][i] = `self.__dict__[i]`
        ticket['status'] = (e_errors.OK, None)

        self.reply_to_caller(ticket)
        return

    # __history(vol) -- show state change history of vol
    def __history(self, vol):
        q = "select to_char(time,'YYYY-MM-DD HH24:MI:SS') as time, \
        label, state_type.name as type, state.value \
             from state, state_type, volume \
             where \
                label like '%s%%' and \
                state.volume = volume.id and \
                state.type = state_type.id \
             order by time desc;"%(vol)
        try:
            res = self.volumedb_dict.query_dictresult(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            message = '__history(): '+str(exc_type)+' '+str(exc_value)+' query: '+q
            Trace.log(e_errors.ERROR, message)
            res = []
        return res

    # history(ticket) -- server version of __history()
    def history(self, ticket):

        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        ticket["status"] = (e_errors.OK, None)

        self.reply_to_caller(ticket)

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket,ticket)
        res = self.__history(external_label)
        callback.write_tcp_obj_new(self.data_socket, res)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
        return

    # history(ticket) -- server version of __history()
    #
    # This is even newer and better implementation that replaces
    # history().  Now the network communications are done using
    # send_reply_with_long_answer().
    def history2(self, ticket):
        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        # get reply
        reply = self.__history(external_label)

        # send the reply
        ticket['history'] = reply
        ticket["status"] = (e_errors.OK, None)
        try:
            self.send_reply_with_long_answer(ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "history2: %s" % (str(msg),))
            return

        return

    def get_sg_counter(self,library, storage_group):
        """
        Returns number of volumes belonging to a given library and storage group.
        Throws exception if no counter is found or DB access issue.

        :type library: :obj:`str`
        :arg library: Library name, e.g. ``CD-10KCF1``.

        :type storage_group: :obj:`str`
        :arg storage_group: Storage group name, e.g. ``lqcd``.
        :rtype: :obj:`int` number of volumes for a given storage group, library

        """
        q = """
        SELECT count from sg_count
        WHERE library = '{}'
        AND   storage_group = '{}'
        """
        try:
            res = self.volumedb_dict.query(q.format(library,storage_group))
            return int(res[0][0]) if len(res) > 0 else 0
        except Exception as e:
            raise e_errors.EnstoreError(None,
                                        "Failed to get volume count for library={}, storage_group={} : {}\n".format(library,storage_group,str(e)),
                                        e_errors.VOLUME_CLERK_ERROR)

    def list_storage_group_count(self):
        """
        Returns list of library.storage_group counts.
        Throws exception in case of DB access issue.
        :rtype: :obj:`dict` dictionary of volume counts keyed on sg.library
        """
        q = """
        select library || '.' || storage_group, count from sg_count
        """
        result = {}
        try:
            res = self.volumedb_dict.query(q)
            for i in res:
                result[i[0]] = i[1]
            return result
        except Exception as e:
            raise e_errors.EnstoreError(None,
                                        "Failed to list storage group counts : {}\n".format(str(e)),
                                        e_errors.VOLUME_CLERK_ERROR)

    # write_protect_status(self, ticket):
    def write_protect_status(self, ticket):

        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        write_protected = self.extract_value_from_ticket('write_protected', record)
        if not write_protected:
            return #extract_value_from_ticket handles its own errors.

        if write_protected == 'y':
            status = "ON"
        elif write_protected == 'n':
            status = "OFF"
        else:
            status = "UNKNOWN"
        ticket['status'] = (e_errors.OK, status)
        self.reply_to_caller(ticket)
        return

    # show_quota() -- return quota information #### DONE

    def show_quota(self, ticket):
	ticket['quota'] = self.quota_enabled()
	ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    # get the remaining bytes value for this volume #### DONE
    def get_remaining_bytes(self, ticket):
        saved_reply_address = ticket.get('r_a')
        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        #This isn't the original use of extract_value_from_ticket(), but
        # it works for getting things from the database record (as a
        # dictionary) too.
        remaining_bytes = self.extract_value_from_ticket('remaining_bytes', record)
        if not remaining_bytes:
            return #extract_value_from_ticket handles its own errors.

        ticket['remaining_bytes'] = remaining_bytes
        ticket['r_a'] = saved_reply_address
        self.reply_to_caller(ticket)
        return

    # get the current database volume about a specific entry #### DONE
    def inquire_vol(self, ticket):
        saved_reply_address = ticket.get('r_a', None)
        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        record["status"] = (e_errors.OK, None)
        record['r_a'] = saved_reply_address
        self.reply_to_caller(record)

    #### DONE, probably not completely
    # return all the volumes in our dictionary.  Not so useful!
    def get_vols(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

	# log it
        Trace.log(e_errors.INFO, "start listing all volumes")

        if not self.get_user_sockets(ticket):
            return
        try:
            callback.write_tcp_obj(self.data_socket, ticket)
        except:
            Trace.log(e_errors.ERROR, "get_vols(): client bailed out 1")
            return

        msg = {}
        q = "select * from volume "
        if ticket.has_key('in_state'):
            state = ticket['in_state']
        else:
            state = None
        if ticket.has_key('not'):
            cond = ticket['not']
        else:
            cond = None
        if ticket.has_key('key'):
            key = ticket['key']
        else:
            key = None

        if key and state:
            if key == 'volume_family':
                sg, ff, wp = string.split(state, '.')
                if cond == None:
                    q = q + "where storage_group = '%s' and file_family = '%s' and wrapper = '%s'"%(sg, ff, wp)
                else:
                    q = q + "where not (storage_group = '%s' and file_family = '%s' and wrapper = '%s')"%(sg, ff, wp)

            else:
                if key in ['blocksize', 'capacity_bytes',
                    'non_del_files', 'remaining_bytes', 'sum_mounts',
                    'sum_rd_access', 'sum_rd_err', 'sum_wr_access',
                    'sum_wr_err']:
                    val = "%d"%(state)
                elif key in ['eod_cookie', 'external_label', 'library',
                    'media_type', 'volume_family', 'wrapper',
                    'storage_group', 'file_family', 'wrapper']:
                    val = "'%s'"%(state)
                elif key in ['first_access', 'last_access', 'declared',
                    'si_time_0', 'si_time_1', 'system_inhibit_0',
                    'system_inhibit_1', 'user_inhibit_0',
                    'user_inhibit_1','modification_time']:
                    val = "'%s'"%(edb.time2timestamp(state))
                else:
                    val = state

                if key == 'external_label':
                    key = 'label'

                if cond == None:
                    q = q + "where %s = %s"%(key, val)
                else:
                    q = q + "where %s %s %s"%(key, cond, val)
        elif state:
            if enstore_functions2.is_readonly_state(state):
                #readonly states are the only ones in system_inhibit_1?
                q = q + "where system_inhibit_1 = '%s'"%(state)
            else:
                q = q + "where system_inhibit_0 = '%s'"%(state)
        else:
            msg['header'] = 'FULL'

        q = q + ' order by label;'

        try:
            res = self.volumedb_dict.query_dictresult(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            mesg = 'get_vols(): '+str(exc_type)+' '+str(exc_value)+' query: '+q
            Trace.log(e_errors.ERROR, mesg)
            res = []

        msg['volumes'] = []
        for v2 in res:
            vol2 = {'volume': v2['label']}
            for k in ["capacity_bytes","remaining_bytes", "library",
                "non_del_files"]:
                vol2[k] = v2[k]
            vol2['volume_family'] = v2['storage_group']+'.'+v2['file_family']+'.'+v2['wrapper']
            vol2['system_inhibit'] = (v2['system_inhibit_0'], v2['system_inhibit_1'])
            vol2['user_inhibit'] = (v2['user_inhibit_0'], v2['user_inhibit_1'])
            vol2['si_time'] = (edb.timestamp2time(v2['si_time_0']),
                edb.timestamp2time(v2['si_time_1']))
            if len(v2['comment']):
                vol2['comment'] = v2['comment']
            msg['volumes'].append(vol2)


        try:
            callback.write_tcp_obj_new(self.data_socket, msg)
        except:
            Trace.log(e_errors.ERROR, "get_vols(): client bailed out 2")
            # clean up
            self.data_socket.close()
            return
        self.data_socket.close()
        try:
            callback.write_tcp_obj(self.control_socket, ticket)
        except:
            Trace.log(e_errors.ERROR, "get_vols(): client bailed out 3")
            # clean up
            self.control_socket.close()
            return
        self.control_socket.close()

        Trace.log(e_errors.INFO, "stop listing all volumes")
        return

    # return all the volumes in our dictionary.  Not so useful!
    def __get_vols2(self, ticket):

        reply = {}
        # q = "select * from volume "
        q = "select label, capacity_bytes, remaining_bytes, library, system_inhibit_0, system_inhibit_1, si_time_0, si_time_1, storage_group, file_family, wrapper, comment from volume "
        if ticket.has_key('in_state'):
            state = ticket['in_state']
        else:
            state = None
        if ticket.has_key('not'):
            cond = ticket['not']
        else:
            cond = None
        if ticket.has_key('key'):
            key = ticket['key']
        else:
            key = None

        if key and state:
            if key == 'volume_family':
                sg, ff, wp = string.split(state, '.')
                if cond == None:
                    q = q + "where storage_group = '%s' and file_family = '%s' and wrapper = '%s'"%(sg, ff, wp)
                else:
                    q = q + "where not (storage_group = '%s' and file_family = '%s' and wrapper = '%s')"%(sg, ff, wp)

            else:
                if key in ['blocksize', 'capacity_bytes',
                    'non_del_files', 'remaining_bytes', 'sum_mounts',
                    'sum_rd_access', 'sum_rd_err', 'sum_wr_access',
                    'sum_wr_err']:
                    val = "%d"%(state)
                elif key in ['eod_cookie', 'external_label', 'library',
                    'media_type', 'volume_family', 'wrapper',
                    'storage_group', 'file_family', 'wrapper',
                    'system_inhibit_0', 'system_inhibit_1',
                    'user_inhibit_0', 'user_inhibit_1']:
                    val = "'%s'"%(state)
                elif key in ['first_access', 'last_access', 'declared',
                    'si_time_0', 'si_time_1','modification_time']:
                    val = "'%s'"%(edb.time2timestamp(state))
                else:
                    val = state

                if key == 'external_label':
                    key = 'label'

                if cond == None:
                    q = q + "where %s = %s"%(key, val)
                else:
                    q = q + "where %s %s %s"%(key, cond, val)
            q = q + " and not label like '%%.deleted'"
        elif state:
            if enstore_functions2.is_readonly_state(state):
                #readonly states are the only ones in system_inhibit_1?
                q = q + "where system_inhibit_1 = '%s'"%(state)
            else:
                q = q + "where system_inhibit_0 = '%s'"%(string.upper(state))
            q = q + " and not label like '%%.deleted'"

        reply['header'] = 'FULL'

        q = q + ' order by label;'

        try:
            res = self.volumedb_dict.query_dictresult(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            mesg = 'get_vols(): '+str(exc_type)+' '+str(exc_value)+' query: '+q
            Trace.log(e_errors.ERROR, mesg)
            res = []
        reply['volumes'] = edb.sanitize_datetime_values(res)

        return reply

    # return all the volumes in our dictionary.  Not so useful!
    #
    # This is the newer and better implementation that replaces
    # get_vols(). Now the data formatting, which takes 90% of CPU
    # load in this request, is done in the client, freeing up
    # server for other requests
    def get_vols2(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # log it
        Trace.log(e_errors.INFO, "start listing all volumes (2)")

        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket, ticket)

        reply = self.__get_vols2(ticket)

        callback.write_tcp_obj_new(self.data_socket, reply)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket, ticket)
        self.control_socket.close()

        Trace.log(e_errors.INFO, "stop listing all volumes (2)")
        return

    # return all the volumes in our dictionary.  Not so useful!
    #
    # This is the newer and better implementation that replaces
    # get_vols(). Now the data formatting, which takes 90% of CPU
    # load in this request, is done in the client, freeing up
    # server for other requests
    #
    # This is even newer and better implementation that replaces
    # get_vols2().  Now the network communications are done using
    # send_reply_with_long_answer().
    def get_vols3(self, ticket):
        # log it
        Trace.log(e_errors.INFO, "start listing all volumes (3)")

        # start communication
        ticket["status"] = (e_errors.OK, None)
        try:
            control_socket = self.send_reply_with_long_answer_part1(ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "get_vols3(): %s" % (str(msg),))
            return

        #Make sure the socket exists.
        if not control_socket:
            return

        # get reply
        reply = self.__get_vols2(ticket)
        reply['status'] = (e_errors.OK, None)

        # send the reply
        try:
            self.send_reply_with_long_answer_part2(control_socket, reply)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "get_vols3(): %s" % (str(msg),))
            return

        # log it
        Trace.log(e_errors.INFO, "stop listing all volumes (3)")
        return

    # return the volumes that have set system_inhibits
    def __get_pvols(self):
        reply = {}
        q = "select * from volume where \
            not label like '%.deleted' and \
            (system_inhibit_0 != 'none' or \
            system_inhibit_1 != 'none') \
            order by label;"

        try:
            res = self.volumedb_dict.query_dictresult(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            mesg = '__get_pvols(): '+str(exc_type)+' '+str(exc_value)+' query: '+q
            Trace.log(e_errors.ERROR, mesg)
            res = []
        reply['volumes'] = edb.sanitize_datetime_values(res)

        return reply

    # return the volumes that have set system_inhibits
    def get_pvols(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # log it
        Trace.log(e_errors.INFO, "start listing all problematic volumes")

        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket, ticket)

        reply = self.__get_pvols()

        callback.write_tcp_obj_new(self.data_socket, reply)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket, ticket)
        self.control_socket.close()

        Trace.log(e_errors.INFO, "stop listing all problematic volumes")
        return

    # return the volumes that have set system_inhibits
    #
    # This is even newer and better implementation that replaces
    # get_pvols().  Now the network communications are done using
    # send_reply_with_long_answer().
    def get_pvols2(self, ticket):
        # log it
        Trace.log(e_errors.INFO, "start listing all problematic volumes (2)")

        # start communication
        ticket["status"] = (e_errors.OK, None)
        try:
            control_socket = self.send_reply_with_long_answer_part1(ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "get_pvols2(): %s" % (str(msg),))
            return

        #Make sure the socket exists.
        if not control_socket:
            return

        # get reply
        reply = self.__get_pvols()
        reply['status'] = (e_errors.OK, None)

        # send the reply
        try:
            self.send_reply_with_long_answer_part2(control_socket, reply)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "get_pvols2(): %s" % (str(msg),))
            return

        # log it
        Trace.log(e_errors.INFO, "stop listing all problematic volumes (2)")

    #### DONE
    def get_sg_count(self, ticket):

        lib = self.extract_value_from_ticket('library', ticket)
        if not lib:
            return #extract_value_from_ticket handles its own errors.

        sg = self.extract_value_from_ticket('storage_group', ticket)
        if not sg:
            return #extract_value_from_ticket handles its own errors.

        try:
            ticket['count'] = self.get_sg_counter(lib, sg)
            ticket['status'] = (e_errors.OK, None)
        except Exception as e:
            Trace.log(e_errors.INFO, "get_sg_count: %s" % (str(e)))
            ticket['status'] = (e_errors.VOLUME_CLERK_ERROR, "Failed to get sg count, see server log for details")
        finally:
            self.reply_to_caller(ticket)

    #### DONE
    def list_sg_count(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        sgcnt = self.list_storage_group_count()
        try:
            if not self.get_user_sockets(ticket):
                return
            ticket["status"] = (e_errors.OK, None)
            callback.write_tcp_obj(self.data_socket, ticket)
            callback.write_tcp_obj_new(self.data_socket, sgcnt)
            self.data_socket.close()
            callback.write_tcp_obj(self.control_socket, ticket)
            self.control_socket.close()
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.handle_error(exc, msg)
        return

    #### DONE
    # This is even newer and better implementation that replaces
    # list_sg_count().  Now the network communications are done using
    # send_reply_with_long_answer().

    def list_sg_count2(self, ticket):
        try:
            ticket['sgcnt'] = self.list_storage_group_count()
            ticket["status"] = (e_errors.OK, None)
        except Exception as e:
            Trace.log(e_errors.INFO, "list_sg_count2(): %s" % (str(e)))
            ticket['status'] = (e_errors.VOLUME_CLERK_ERROR, "Failed to list storage group counts, see server log for details")
        finally:
            self.send_reply_with_long_answer(ticket)
        return


    #### DONE
    def __get_vol_list(self):
        q = "select label from volume order by label;"
        try:
            res2 = self.volumedb_dict.query_getresult(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            message = '__get_vol_list(): '+str(exc_type)+' '+str(exc_value)+' query: '+q
            Trace.log(e_errors.ERROR, message)
            return []
        res = []
        for i in res2:
            res.append(i[0])
        return res

    #### DONE
    # return a list of all the volumes
    def get_vol_list(self, ticket):
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        try:
            if not self.get_user_sockets(ticket):
                return
            ticket['status'] = (e_errors.OK, None)
            callback.write_tcp_obj(self.data_socket, ticket)
            vols = self.__get_vol_list()
            callback.write_tcp_obj_new(self.data_socket, vols)
            self.data_socket.close()
            callback.write_tcp_obj(self.control_socket, ticket)
            self.control_socket.close()
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.handle_error(exc, msg)
        return

    #### DONE
    # return a list of all the volumes
    #
    # This is even newer and better implementation that replaces
    # get_vol_list().  Now the network communications are done using
    # send_reply_with_long_answer().
    def get_vol_list2(self, ticket):
        ticket['status'] = (e_errors.OK, None)

        # start communication
        try:
            control_socket = self.send_reply_with_long_answer_part1(ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "get_vol_list2(): %s" % (str(msg),))
            return

        #Make sure the socket exists.
        if not control_socket:
            return

        # get reply
        vols = self.__get_vol_list()

        # send the reply
        ticket['volumes'] = vols
        try:
            self.send_reply_with_long_answer_part2(control_socket, ticket)
        except (socket.error, select.error), msg:
            Trace.log(e_errors.INFO, "get_vol_list2(): %s" % (str(msg),))
            return

    #### DONE
    def list_ignored_sg(self, ticket):
        ticket['status'] = (e_errors.OK, self.ignored_sg)
        self.reply_to_caller(ticket)

    def list_migrated_files(self, ticket):
        src_vol, src_record = self.extract_external_label_from_ticket(ticket,
                                                           key = 'src_vol')
        if not src_vol:
            return #extract_external_label_from_ticket handles its own errors.
        dst_vol, dst_record = self.extract_external_label_from_ticket(ticket,
                                                           key = 'dst_vol')
        if not dst_vol:
            return #extract_external_label_from_ticket handles its own errors.

        q = "select migration.src_bfid, src_bfid, copied, swapped, checked, closed " \
            "from migration,file f1, volume v1, file f2, volume v2 " \
            "where v1.label = '%s' and f1.volume = v1.id " \
            "  and v2.label = '%s' and f2.volume = v2.id " \
            "  and f1.bfid = migration.src_bfid " \
            "  and f2.bfid = migration.dst_bfid;" % (src_vol, dst_vol)

        try:
            ticket['migrated_files'] = edb.sanitize_datetime_values(self.volumedb_dict.query_dictresult(q))
            ticket['status'] = (e_errors.OK, None)
        except e_errors.EnstoreError as msg:
            ticket['status'] = (msg.type, str(msg))
        except:
            ticket['status'] = (e_errors.VOLUME_CLERK_ERROR, str(sys.exc_info()[1]))
        self.send_reply(ticket)
        return


    def list_duplicated_files(self, ticket):
        src_vol, src_record = self.extract_external_label_from_ticket(ticket,
                                                              key = 'src_vol')
        if not src_vol:
            return #extract_external_label_from_ticket handles its own errors.
        dst_vol, dst_record = self.extract_external_label_from_ticket(ticket,
                                                              key = 'dst_vol')
        if not dst_vol:
            return #extract_external_label_from_ticket handles its own errors.

        q = "select file_copies_map.bfid, alt_bfid " \
            "from file_copies_map,file f1, volume v1, file f2, volume v2 " \
            "where v1.label = '%s' and f1.volume = v1.id " \
            "  and v2.label = '%s' and f2.volume = v2.id " \
            "  and f1.bfid = file_copies_map.bfid " \
            "  and f2.bfid = file_copies_map.alt_bfid;" % (src_vol, dst_vol)

        try:
            ticket['duplicated_files'] = edb.sanitize_datetime_values(self.volumedb_dict.query_dictresult(q))
            ticket['status'] = (e_errors.OK, None)
        except e_errors.EnstoreError as msg:
            ticket['status'] = (msg.type, str(msg))
        except:
            ticket['status'] = (e_errors.VOLUME_CLERK_ERROR, str(sys.exc_info()[1]))
        self.send_reply(ticket)
        return

    #_migration_history():  Underlying function to obtain the internal
    #   database IDs for the source and destination migration volumes.
    def _migration_history(self, ticket):
        # extract the additional information if source and/or destination
        # information is requested.
        src_vol, src_record = self.extract_external_label_from_ticket(ticket,
                                                               key = 'src_vol')
        if not src_vol:
            return #extract_external_label_from_ticket handles its own errors.
        dst_vol, dst_record = self.extract_external_label_from_ticket(ticket,
                                                              key = 'dst_vol')
        if not dst_vol:
            return #extract_external_label_from_ticket handles its own errors.

        vol_id_list = []
        for vol in (src_vol, dst_vol):
            q = "select id from volume where label = '%s'" % (vol,)

            try:
                res = self.volumedb_dict.query_getresult(q)

                if len(res) == 0:
                    ticket['status'] = (e_errors.NOVOLUME,
                                        "volume %s not found in DB" % (vol,))
                    return ticket
                else:
                    vol_id_list.append(res[0][0]) #We got the volume DB ID.
            except e_errors.EnstoreError as msg:
                ticket['status'] = (msg.type, str(msg))
            except:
                exc_type, exc_value = sys.exc_info()[:2]
                message = "failed to find %s volume id due to: %s" \
                          % (vol, (str(exc_type), str(exc_value)))
                ticket["status"] = (e_errors.VOLUME_CLERK_ERROR, message)
                Trace.log(e_errors.ERROR, message)

                return ticket

        #If we get this far, we successfully obtained two volume IDs.
        ticket['src_vol_id'] = vol_id_list[0]
        ticket['dst_vol_id'] = vol_id_list[1]
        ticket['status'] = (e_errors.OK, None)

        return ticket

    def get_migration_history(self, ticket):
        ticket = self._migration_history(ticket)

        # Insert this volume combintation into the migration_history table.
        q = "select * from  migration_history where src_vol_id = '%s' \
             and dst_vol_id = '%s';" % (ticket['src_vol_id'],
                                        ticket['dst_vol_id'])

        try:
            res = self.volumedb_dict.query_dictresult(q)

            ticket['status'] = (e_errors.OK, None)
            ticket['migration_history'] = edb.sanitize_datetime_values(res)


        except e_errors.EnstoreError as msg:
            ticket['status'] = (msg.type,
                                "Failed to update migration_history for %s due to: %s" \
                                % ((ticket['src_vol'], ticket['dst_vol']), str(msg)))
        except:
            ticket['status'] = (e_errors.VOLUME_CLERK_ERROR,
                                "Failed to update migration_history for %s due to: %s" \
                                % ((ticket['src_vol'], ticket['dst_vol']), str(sys.exc_info()[1])))
        self.send_reply(ticket)
        return

class VolumeClerkMethods(VolumeClerkInfoMethods):

    def __init__(self, csc):
        VolumeClerkInfoMethods.__init__(self, csc)

        self.noaccess_cnt = 0
        self.max_noaccess_cnt = self.keys.get('max_noaccess_cnt', 2)
        self.noaccess_to = self.keys.get('noaccess_to', 300.)
        self.paused_lms = {}
        self.noaccess_time = time.time()

        self.paused_lms = {}
        self.common_blank_low = {'warning':100, 'alarm':10}

        #Retrieve database information from the configuration.
        Trace.log(e_errors.INFO, "determine dbHome and jouHome")
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

        self.parallelQueueSize       = self.keys.get('parallel_queue_size',PARALLEL_QUEUE_SIZE)
        self.numberOfParallelWorkers = self.keys.get('max_threads',MAX_THREADS)
        self.max_connections         = self.numberOfParallelWorkers+1

        self.volumedb_dict = edb.VolumeDB(host=dbInfo.get('db_host',None),
                                          port=dbInfo.get('db_port',None),
                                          user=dbInfo.get('dbuser',None),
                                          database=dbInfo.get('dbname',None),
                                          jou=jouHome,
                                          max_connections=self.max_connections,
                                          max_idle=int(self.max_connections*0.9+0.5))

        self.volumedb_dict.dbaccess.set_retries(MAX_CONNECTION_FAILURE)

	self.parallelThreadQueue = Queue.Queue(self.parallelQueueSize)
	self.parallelThreads = []
	for i in range(self.numberOfParallelWorkers):
		worker = dispatching_worker.ThreadExecutor(self.parallelThreadQueue,self)
		self.parallelThreads.append(worker)
		worker.start()

        # load ignored sg
        self.ignored_sg_file = os.path.join(dbHome, 'IGNORED_SG')
        try:
            f = open(self.ignored_sg_file)
            self.ignored_sg = cPickle.load(f)
            f.close()
        except:
            self.ignored_sg = []

        # get common pool low water mark
	# default to be 10
        res = self.csc.get('common_blank_low')
        if res['status'][0] == e_errors.OK:
            self.common_blank_low = res


    def invoke_function(self, function, args=()):
        if  function.__name__ == "quit":
            apply(function,args)
        else:
		Trace.trace(5, "Putting on parallel thread queue %d %s"%(self.parallelThreadQueue.qsize(),function.__name__))
		self.parallelThreadQueue.put([function.__name__, args])

    ####################################################################

    ###
    ### These functions are internal volume_clerk functions.
    ###

    # change_state(type, value) -- change a state
    def change_state(self, volume, type, value):
        q = "insert into state (volume, type, value) values (\
             lookup_vol('%s'), lookup_stype('%s'), '%s');" % \
             (volume, type, value)
        try:
	    self.volumedb_dict.insert(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            message = "change_state(): "+str(exc_type)+' '+str(exc_value)+' query: '+q
            Trace.log(e_errors.ERROR, message)

    #### DONE
    # set pause flag for the all Library Managers corresponding to
    # certain Media Changer
    def pause_lm(self, external_label):
        # get the current entry for the volume
        record = self.volumedb_dict[external_label]
        if not record:
            message = "Volume Clerk: no such volume %s" % (external_label,)
            Trace.log(e_errors.ERROR, message)
            return
        # find the media changer for this volume
        # m_changer = self.csc.get_media_changer(record['library'] + ".library_manager")
        lib = record['library']
        if len(lib) < 16 or lib[-16:] != '.library_manager':
            lib = lib + '.library_manager'
        m_changer = self.csc.get_media_changer(lib)
        if m_changer:
            if not self.paused_lms.has_key(m_changer):
                self.paused_lms[m_changer] = {'paused':0,
                                              'noaccess_cnt': 0,
                                              'noaccess_time':time.time(),
                                              }
            now = time.time()
            if self.paused_lms[m_changer]['noaccess_cnt'] == 0:
                self.paused_lms[m_changer]['noaccess_time'] = now
            if now - self.paused_lms[m_changer]['noaccess_time'] <= self.noaccess_to:
                self.paused_lms[m_changer]['noaccess_cnt'] = self.paused_lms[m_changer]['noaccess_cnt'] + 1
            else:
                self.paused_lms[m_changer]['noaccess_cnt'] = 1
            if ((self.paused_lms[m_changer]['noaccess_cnt'] >= self.max_noaccess_cnt) and
                self.paused_lms[m_changer]['paused'] == 0):
                self.paused_lms[m_changer]['paused'] = 1
                Trace.log(e_errors.INFO,'pause library_managers for %s media_changerare paused due to too many volumes set to NOACCESS' % (m_changer,))

    # check if Library Manager is paused #### DONE
    def lm_is_paused(self, library):
        # m_changer = self.csc.get_media_changer(library + ".library_manager")
        if len(library) < 16 or library[-16:] != '.library_manager':
            library = library + '.library_manager'
        m_changer = self.csc.get_media_changer(library)
        # guard against configuration server timeout
        # Here, we rely on csc to return a string.
        # Hoever, if the request timed out, csc will return a dict
        # with error code ...
        if type(m_changer) != type(''):
            # log this locally
            print time.ctime(time.time()), 'm_changer =', `m_changer`
            return 0
        if m_changer:
            if (self.paused_lms.has_key(m_changer) and
                self.paused_lms[m_changer]['paused'] != 0):
                ret_code = 1
                Trace.log(e_errors.ERROR,'library_managers for %s media_changerare paused due to too many volumes set to NOACCESS' % (m_changer,))
            else:
                ret_code = 0
        else:
            ret_code = 0
        return ret_code

    ####################################################################
    ### The following group of functions initially look like they don't
    ### modify anything.  However, is_volume_full() calls change_state()
    ### and the find_matching_volume() calls is_volume_full().

    # check if volume is full #### DONE
    def is_volume_full(self, v, min_remaining_bytes):
        external_label = v['external_label']
        ret = ""
        left = v["remaining_bytes"]
        if left < long(min_remaining_bytes*SAFETY_FACTOR) or left < MIN_LEFT:
            # if it __ever__ happens that we can't write a file on a
            # volume, then mark volume as full.  This prevents us from
            # putting 1 byte files on old "golden" volumes and potentially
            # losing the entire tape. One could argue that a very large
            # file write could prematurely flag a volume as full, but lets
            # worry about if it is really a problem - I propose that an
            # administrator reset the system_inhibit back to none in these
            # special, and hopefully rare cases.

            if v["system_inhibit"][1] != "full":
                # detect a transition
                ret = e_errors.VOL_SET_TO_FULL
                v["system_inhibit"][1] = "full"
                v["si_time"][1] = time.time()
                left = v["remaining_bytes"]/1.
                totb = v["capacity_bytes"]/1.
                if totb != 0:
                    waste = left/totb*100.
                else:
                    waste = 0.
                Trace.log(e_errors.INFO,
                          "%s is now full, bytes remaining = %d, %.2f %%" %
                          (external_label,
                           v["remaining_bytes"],waste))
                # update it
                self.volumedb_dict[external_label] = v
                self.change_state(external_label, 'system_inhibit_1', "full")
                Trace.log(e_errors.INFO, 'volume %s is set to "full" by is_volume_full()'%(external_label))
            else: ret = e_errors.NOSPACE
        return ret


    # find volume that matches given volume family #### DONE
    def find_matching_volume(self, library, vol_fam, pool,
                             wrapper, vol_veto_list, first_found,
                             min_remaining_bytes, exact_match=1,
                             mover={}):

        # to make pychecker happy
        if first_found:
            pass

        # decomposit storage_group, file_family and wrapper
        storage_group, file_family, wrapper = string.split(pool, '.')

        # figure out minimal space needed
        required_bytes = max(long(min_remaining_bytes*SAFETY_FACTOR), MIN_LEFT)

        # build vito list into where clause
        veto_q = ""
        for i in vol_veto_list:
            veto_q = veto_q+" and label != '%s'"%(i)

        type_of_mover = mover.get('mover_type','Mover')

        # To be backward comparible
        if type_of_mover == 'DiskMover':
            exact_match = 1

        Trace.trace(20,  "volume family %s pool %s wrapper %s veto %s exact %s" %
                    (vol_fam, pool,wrapper, vol_veto_list, exact_match))

        # special treatment for Disk Mover
        if type_of_mover == 'DiskMover':
            mover_ip_map = mover.get('ip_map', '')

            q = """
            SELECT block_size,
                   capacity_bytes,
                   declared,
                   eod_cookie,
                   label,
                   first_access,
                   last_access,
                   library,
                   media_type,
                   non_del_files,
                   remaining_bytes,
                   sum_mounts,
                   sum_rd_access,
                   sum_rd_err,
                   sum_wr_access,
                   sum_wr_err,
                   system_inhibit_0,
                   system_inhibit_1,
                   si_time_0,
                   si_time_1,
                   user_inhibit_0,
                   user_inhibit_1,
                   storage_group,
                   file_family,
                   wrapper,
                   comment,
                   write_protected,
                   modification_time,
                   COALESCE(active_files,0) AS active_files,
                   COALESCE(deleted_files,0) AS deleted_files,
                   COALESCE(unknown_files,0) AS unknown_files,
                   COALESCE(active_bytes,0) AS active_bytes,
                   COALESCE(deleted_bytes,0) AS deleted_bytes,
                   COALESCE(unknown_bytes,0) AS unknown_bytes
            FROM volume
            WHERE label like '{}:%'
              AND library = '{}'
              AND storage_group = '{}'
              AND file_family = '{}'
              AND wrapper = '{}'
              AND system_inhibit_0 = 'none'
              AND system_inhibit_1 = 'none'
              AND user_inhibit_0 = 'none'
              AND user_inhibit_1 = 'none'
              AND write_protected = 'n'
              {}
            ORDER BY declared,label limit 10
            """.format(mover_ip_map, library,
                       storage_group, file_family, wrapper, veto_q)
        else: # normal case
            q = """
            SELECT block_size,
                   capacity_bytes,
                   declared,
                   eod_cookie,
                   label,
                   first_access,
                   last_access,
                   library,
                   media_type,
                   non_del_files,
                   remaining_bytes,
                   sum_mounts,
                   sum_rd_access,
                   sum_rd_err,
                   sum_wr_access,
                   sum_wr_err,
                   system_inhibit_0,
                   system_inhibit_1,
                   si_time_0,
                   si_time_1,
                   user_inhibit_0,
                   user_inhibit_1,
                   storage_group,
                   file_family,
                   wrapper,
                   comment,
                   write_protected,
                   modification_time,
                   COALESCE(active_files,0) AS active_files,
                   COALESCE(deleted_files,0) AS deleted_files,
                   COALESCE(unknown_files,0) AS unknown_files,
                   COALESCE(active_bytes,0) AS active_bytes,
                   COALESCE(deleted_bytes,0) AS deleted_bytes,
                   COALESCE(unknown_bytes,0) AS unknown_bytes
             FROM volume
             WHERE library = '{}'
               AND storage_group = '{}'
               AND file_family = '{}'
               AND wrapper = '{}'
               AND system_inhibit_0 = 'none'
               AND system_inhibit_1 = 'none'
               AND user_inhibit_0 = 'none'
               AND user_inhibit_1 = 'none'
               AND write_protected = 'n'
               {}
             ORDER BY declared, label limit 10
             """.format(library, storage_group,
                        file_family, wrapper, veto_q)
        Trace.trace(20, "start query: %s"%(q))
        try:
            res = self.volumedb_dict.query_dictresult(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            message = 'find_matching_volume(): '+str(exc_type)+' '+str(exc_value)+' query: '+q
            Trace.log(e_errors.ERROR, message)
            res = []
        Trace.trace(20, "finish query: found %d exact_match=%d"%(len(res), exact_match))
        if len(res):
            if exact_match:
                for v in res:
                    v1 = self.volumedb_dict.export_format(v)
                    if self.is_volume_full(v1,min_remaining_bytes):
                        Trace.trace(20, "set %s to full"%(v1['external_label']))
                    else:
                        return v1
                return {}
            else:
                return self.volumedb_dict.export_format(res[0])
        else:
            return {}

    # get the actual state of the volume from the media changer #### DONE
    #
    ### Note: While this function only gets information, there is no reason
    ### that any other server (aka InfoServer) could use this function.
    ### Thus, it is stayiny in VolumeClerkMethods and not going into
    ### VolumeClerkInfoMethods.
    def get_media_changer_state(self, lib, volume, m_type):
        # m_changer = self.csc.get_media_changer(lib + ".library_manager")

        # a short cut for non-existing library, such as blank
        if not string.split(lib, '.')[0] in self.csc.get_library_managers().keys():
            return "no_lib"  #Not a fatal error.

        if len(lib) < 16 or lib[-16:] != '.library_manager':
            lib = lib + '.library_manager'
        m_changer = self.csc.get_media_changer(lib)
        if not m_changer:
            Trace.log(e_errors.ERROR,
                      "vc.get_media_changer_state: ERROR: no media changer found (lib = %s) %s" % (lib, volume))
            return "no_mc"  #Not a fatal error.

        import media_changer_client
        mcc = media_changer_client.MediaChangerClient(self.csc, m_changer )
        reply_ticket = mcc.viewvol(volume, m_type, rcv_timeout = 5,
                                   rcv_tries = 5)
        if not e_errors.is_ok(reply_ticket):
            Trace.log(e_errors.ERROR,
                      "unable to get volume state from %s: %s: %s" % \
                      (m_changer, reply_ticket['status'][0],
                       reply_ticket['status'][1]))
            return "unknown"  #Fatal error.

        #Most common values are:
        # "O" for occupied in its home slot
        # "M" for mounted in drive
        # "E" for ejected
        # "U" for undefined/unknown
        stat = reply_ticket['status'][3]

        return stat

    ####################################################################

    ###
    ### These functions are called via dispatching worker.
    ###

    # set_write_protect
    def write_protect_on(self, ticket):

        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        try:
            self.change_state(external_label, 'write_protect', 'ON')
            # for journaling
            record = self.volumedb_dict[external_label]
            record['write_protected'] = 'y'
            self.volumedb_dict[external_label] = record
            ticket['status'] = (e_errors.OK, None)
        except e_errors.EnstoreError as msg:
            message = "unable to turn write protect on: %s" % (str(msg),)
            ticket['status'] = (msg.type, message)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            message = str(exc_type)+' '+str(exc_value)
            Trace.log(e_errors.ERROR, message)
            ticket["status"] = (e_errors.VOLUME_CLERK_ERROR, message)
        self.reply_to_caller(ticket)
        return

    # set_write_protect
    def write_protect_off(self, ticket):

        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        try:
            self.change_state(external_label, 'write_protect', 'OFF')
            # for journaling
            record = self.volumedb_dict[external_label]
            record['write_protected'] = 'n'
            self.volumedb_dict[external_label] = record
            ticket['status'] = (e_errors.OK, None)
        except e_errors.EnstoreError as msg:
            message = "unable to turn write protect off: %s" % (str(msg),)
            ticket['status'] = (msg.type, message)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            message = str(exc_type)+' '+str(exc_value)
            Trace.log(e_errors.ERROR, message)
            ticket["status"] = (e_errors.VOLUME_CLERK_ERROR, message)
        self.reply_to_caller(ticket)
        return


    #### DONE
    # __rename_volume(old, new): rename a volume from old to new
    #
    # renaming a volume involves:
    # [1] renaming the records of the files in it, done by file clerk
    #     [a] in each file record, 'external_label' and 'pnfs_mapname'
    #         are changed according
    # [2] physically renaming the volmap path in /pnfs, done by file clerk
    # [3] renaming volume record by changing its 'external_label'
    #
    # after renaming, the original volume does not exist any more

    def __rename_volume(self, old, new):
        record = self.volumedb_dict[old]
        if not record:
            return 'EACCESS', "volume %s does not exist"%(old)

        if self.volumedb_dict.has_key(new):
            return 'EEXIST', "volume %s already exists"%(new)

        try:
            record['external_label'] = new
            self.volumedb_dict[old] = record
        except:
            Trace.log(e_errors.ERROR, "failed to rename %s to %s"%(old, new))
            return e_errors.ERROR, None

        Trace.log(e_errors.INFO, "volume renamed %s->%s"%(old, new))
        return e_errors.OK, None

    # rename_volume() -- server version of __rename_volume()

    def rename_volume(self, ticket):
        saved_reply_address = ticket.get('r_a', None)
        old = self.extract_external_label_from_ticket(
            ticket, key = "old", check_exists = False)
        if not old:
            return #extract_external_label_from_ticket handles its own errors.

        new = self.extract_external_label_from_ticket(
            ticket, key = "new", check_exists = False)
        if not new:
            return #extract_external_label_from_ticket handles its own errors.

        # This is a restricted service
        status = self.restricted_access(ticket)
        if status:
            message = "attempt to rename volume %s to %s from %s" \
                      % (old, new, self.reply_address[0])
            Trace.log(e_errors.ERROR, message)
            ticket['status'] = status
        else:
            ticket['status'] = self.__rename_volume(old, new)
        ticket['r_a'] = saved_reply_address
        self.reply_to_caller(ticket)
        return

    # __erase_volume(vol) -- erase vol forever #### DONE
    # This one is very dangerous
    #
    # erasing a volume wipe out the meta information about it as if
    # it never exists.
    #
    # * only deleted volume can be erased.
    #
    # erasing a volume involves:
    # [1] erasing all file records associated with this volume -- done
    #     by file clerk
    # [2] erasing volume record

    def __erase_volume(self, vol):

        # only allow deleted volume to be erased
        if vol[-8:] != ".deleted":
            error_msg = "trying to erase a undeleted volume %s" % (vol)
            Trace.log(e_errors.ERROR, error_msg)
            return e_errors.ERROR, error_msg

        q = "select id from volume where label = '%s';" % (vol)
        res = self.volumedb_dict.query_getresult(q)
        if not res:
            message = 'volume "%s" does not exist' % (vol)
            Trace.log(e_errors.ERROR, message)
            return e_errors.ERROR, message

        vid = res[0][0]

        q = "delete from file where volume = %d;"%(vid)
        try:
            self.volumedb_dict.delete(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            message = '__erase_volume(): '+str(exc_type)+' '+str(exc_value)+' '+ q
            Trace.log(e_errors.ERROR, message)
            return e_errors.ERROR, message

        # fcc = file_clerk_client.FileClient(self.csc)
        #
        # # erase file record
        # status = fcc.erase_volume(vol)['status']
        # del fcc
        # if status[0] != e_errors.OK:
        #    Trace.log(e_errors.ERROR, 'erasing volume "%s" failed'%(vol))
        #    return status
        # erase volume record
        del self.volumedb_dict[vol]
        Trace.log(e_errors.INFO, 'volume "%s" has been erased' % (vol,))
        return e_errors.OK, None

    # erase_volume(vol) -- server version of __erase_volume() #### DONE

    def erase_volume(self, ticket):
        saved_reply_address = ticket.get('r_a')
        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        # This is a restricted service
        status = self.restricted_access(ticket)
        if status:
            message = "attempt to erase volume %s from %s" \
                      % (external_label, self.reply_address[0])
            Trace.log(e_errors.ERROR, message)
            ticket['status'] = status
        else:
            ticket['status'] = self.__erase_volume(external_label)
        ticket['r_a'] = saved_reply_address
        self.reply_to_caller(ticket)
        return

    # __delete_volume(vol) -- delete a volume #### DONE
    #
    # * only a volume that contains no active files can be deleted
    #
    # deleting a volume, vol, is simply renaming it to vol.deleted
    #
    # if recycle flag is set, vol will be redeclared as a new volume

    def __delete_volume(self, vol, recycle = 0, check_state = 1,
                        clear_sg = False, reset_declared = True,
                        record = None):
        status = e_errors.OK, None
        if record == None:
            # check existence of the volume
            record = self.volumedb_dict[vol]
            if not record:
                msg = "%s: no such volume %s" % (MY_NAME, vol)
                Trace.log(e_errors.ERROR, msg)
                return e_errors.ERROR, msg

        # check if it has been deleted
        if vol[-8:] == '.deleted' or record['external_label'][-8:] == '.deleted':
            return e_errors.OK, 'volume %s has been deleted already'%(vol)

        # check if all files are deleted
        try:
            if self.has_undeleted_file(vol):
                msg = 'can not delete non-empty volume %s'%(vol)
                Trace.log(e_errors.ERROR, msg)
                return e_errors.ERROR, msg
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = 'has_undeleted_file(): '+str(exc_type)+' '+str(exc_value)
            Trace.log(e_errors.ERROR, msg)
            return e_errors.ERROR, msg

        if check_state and record["media_type"] not in ("null", "disk"):
            # check the volume's state with the media_changer
            ret = self.get_media_changer_state(record["library"],
                                           record["external_label"],
                                           record["media_type"])

            # (ret == "no_lib") when no matching library is found in the
            #             current configuration.
            # (ret == "unknown) when the query to the media_changer times out.
            # (ret = "no_mc") when their is no media changer defined for
            #                 the volume's library.

            if ret != "no_mc" and ret != "no_lib" \
                   and ret != 'O' and ret != "E" and ret != "U":
                message = "volume state must be unmounted ('O') or " \
                          "ejected ('E') or undefined ('U').  (state is %s)" \
                          % (ret,)
                return e_errors.CONFLICT, message

        # delete the volume
        # check if <vol>.deleted exists, if so, erase it.

        if self.volumedb_dict.has_key(vol+'.deleted'):
            # erase it
            status = self.__erase_volume(vol+'.deleted')
            if status[0] != e_errors.OK:
                return status

        renamed = None
        # check if it is never written, if so, erase it
        if record['sum_wr_access']:
	    status = self.__rename_volume(vol, vol+'.deleted')
            if status[0] == e_errors.OK:
                record = self.volumedb_dict[vol+'.deleted']
                record['system_inhibit'][0] = e_errors.DELETED
                self.volumedb_dict[vol+'.deleted'] = record
		self.change_state(vol+'.deleted', 'system_inhibit_0', e_errors.DELETED)
                Trace.log(e_errors.INFO, 'volume "%s" has been deleted'%(vol))
                renamed = vol+'.deleted'
            else: # don't do anything further
                return status
        else:    # never written
            if not recycle:
                #
                # we only delete the volume if we are not recycling it, otherwise
                # the history is lost
                #
                del self.volumedb_dict[vol]
                status = e_errors.OK, None
                Trace.log(e_errors.INFO, 'Empty volume "%s" has been deleted'%(vol))

        # recycling it?

        if recycle:
            record['external_label'] = vol
            record['remaining_bytes'] = record['capacity_bytes']
            if reset_declared:
                record['declared'] = time.time()
            if record['eod_cookie']  != "none":
                record['eod_cookie'] = '0000_000000000_0000001'
            record['last_access'] = -1
            record['first_access'] = -1
            record['modification_time'] = -1
            record['system_inhibit'] = ["none", "none"]
            record['user_inhibit'] = ["none", "none"]
            record['sum_rd_access'] = 0
            record['sum_wr_access'] = 0
            record['sum_wr_err'] = 0
            record['sum_rd_err'] = 0
            record['non_del_files'] = 0
            record['comment']=""
            for key in ("deleted_files","deleted_bytes"):
                record[key]=0
            # reseting volume family
            sg = string.split(record['volume_family'], '.')[0]
            if clear_sg:
                record['volume_family'] = 'none.none.none'
            else:
                record['volume_family'] = sg+'.none.none'
            # check for obsolete fields
            for ek in ['at_mover', 'file_family', 'status']:
                if record.has_key(ek):
                    del record[ek]
            self.volumedb_dict[vol] = record
            self.change_state(vol, 'other', "RECYCLED");
            Trace.log(e_errors.INFO, 'volume "%s" has been recycled'%(vol))
            if renamed:
                # take care of write_protect state
                # take it from its previous life
                q = "select time, value from state, state_type, volume \
                     where \
                        state.type = state_type.id and \
                        state_type.name = 'write_protect' and \
                        state.volume = volume.id and \
                        volume.label = '%s' \
                        order by time desc limit 1;"%(renamed)
                try:
                    res = self.volumedb_dict.query_dictresult(q)
                    if res:
                        self.change_state(vol, 'write_protect', res[0]['value'])
                        message = 'volume "%s" has been recycled' % (vol,)
                        Trace.log(e_errors.INFO, message)
                except:
                    pass
                # take care of history by re-assigning state record from <volume>.deleted to <volume>
                q = "update state set volume=(select id from volume where label='%s') \
                     where volume=(select id from volume where label='%s')"%(vol,renamed,)
                try:
                    self.volumedb_dict.update(q)
                except:
                    return e_errors.ERROR, "Failed to preserve volume history"

        else:

            # get storage group and take care of quota

            library = record['library']
            sg = volume_family.extract_storage_group(record['volume_family'])
            if sg == 'none':
                sgc = self.get_sg_counter(library, 'none')
                if sgc < self.common_blank_low['alarm']:
                    message = "(%s, %s) has only %d tapes left, less than %d" \
                              % (library, 'none', sgc,
                                 self.common_blank_low['alarm'])
                    Trace.alarm(e_errors.ERROR, 'COMMON BLANK POOL LOW',
                                message)
                elif sgc < self.common_blank_low['warning']:
                    message = "(%s, %s) has only %d tapes left, less than %d" \
                              % (library, 'none', sgc,
                                 self.common_blank_low['warning'])
                    Trace.alarm(e_errors.WARNING, 'COMMON BLANK POOL LOW',
                                message)
        return status

    # delete_volume(vol) -- server version of __delete_volume() #### DONE

    def delete_volume(self, ticket):
        saved_reply_address = ticket.get('r_a')
        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        # This is a restricted service
        status = self.restricted_access(ticket)
        if status:
            message = "attempt to delete volume %s from %s" \
                      % (external_label, self.reply_address[0])
            Trace.log(e_errors.ERROR, message)
            ticket['status'] = status
            ticket['r_a'] = saved_reply_address
            self.reply_to_caller(ticket)
            return

        if ticket.has_key('check_state'):
            ticket['status'] = self.__delete_volume(external_label, check_state = ticket['check_state'], record = record)
        else:
            ticket['status'] = self.__delete_volume(external_label, record = record)
        ticket['r_a'] = saved_reply_address
        self.reply_to_caller(ticket)
        return

    #### DONE
    # recycle_volume(vol) -- server version of __delete_volume(vol, 1)

    def recycle_volume(self, ticket):
        saved_reply_address = ticket.get('r_a')
        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        # This is a restricted service
        status = self.restricted_access(ticket)
        if status:
            message = "attempt to recycle volume %s from %s" \
                      % (external_label, self.reply_address[0])
            Trace.log(e_errors.ERROR, message)
            ticket['r_a'] = saved_reply_address
            ticket['status'] = status
            self.reply_to_caller(ticket)
            return

        if ticket.has_key('check_state'):
            check_state = ticket['check_state']
        else:
            check_state = 1

        if ticket.has_key('clear_sg'):
            clear_sg = True
        else:
            clear_sg = False
        if ticket.has_key('reset_declared'):
            ticket['status'] = self.__delete_volume(
                external_label, 1, check_state = check_state,
                clear_sg = clear_sg, reset_declared = ticket['reset_declared'],
                record = record)
        else:
            ticket['status'] = self.__delete_volume(
                external_label, 1, check_state = check_state,
                clear_sg = clear_sg, record = record)
        ticket['r_a'] = saved_reply_address
        self.reply_to_caller(ticket)
        return

    #### DONE
    # __restore_volume(vol) -- restore a deleted volume
    #
    # Only a deleted volume can be restored, i.e., vol must be of the
    # form <vol>.deleted
    #
    # if <vol> exists:
    #     if <vol> has not been written:
    #         erase <vol>
    #     if <vol> has benn written:
    #         signal this as an error
    #
    # a volume is restored to the state when it was deleted, i.e.,
    # containing only deleted files.
    #
    # restoring a volume is, if all critera are satisfied, simply
    # renaming a volume from <vol>.deleted to <vol>

    def __restore_volume(self, vol):

        # only allow deleted volume to be restored
        if vol[-8:] != '.deleted':
            error_msg = 'trying to restore a undeleted volume %s'%(vol)
            Trace.log(e_errors.ERROR, error_msg)
            return e_errors.ERROR, error_msg

        # check if another vol exists
        vol = vol[:-8]
        if self.volumedb_dict.has_key(vol):
            # there is vol, check if it has been written
            record = self.volumedb_dict[vol]
            if record['sum_wr_access']:
                error_msg = 'volume %s already exists, can not restore %s.deleted'%(vol, vol)
                Trace.log(e_errors.ERROR, error_msg)
                return e_errors.ERROR, error_msg
            else:    # never written, just erase it
                del self.volumedb_dict[vol]
                Trace.log(e_errors.INFO, 'Empty volume "%s" is erased due to restoration of a previous version'%(vol))

        status = self.__rename_volume(vol+'.deleted', vol)
        if status[0] == e_errors.OK:
            # take care of system inhibit[0]
            record = self.volumedb_dict[vol]
            record['system_inhibit'][0] = 'none'
            self.volumedb_dict[vol] = record
            self.change_state(vol, 'system_inhibit_0', 'none')
            Trace.log(e_errors.INFO, 'volume "%s" has been restored'%(vol))
        return status

    # restore_volume(vol) -- server version of __restore_volume()

    def restore_volume(self, ticket):
        saved_reply_address = ticket.get('r_a')
        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.
        # This is a restricted service
        status = self.restricted_access(ticket)
        if status:
            message = "attempt to restore volume %s from %s" \
                      % (external_label, self.reply_address[0])
            Trace.log(e_errors.ERROR, message)
            ticket['status'] = status
            ticket['r_a'] = saved_reply_address
            self.reply_to_caller(ticket)
            return

        ticket['status'] = self.__restore_volume(external_label)
        ticket['r_a'] = saved_reply_address
        self.reply_to_caller(ticket)
        return

    #### DONE
    # reassign_sg(self, ticket) -- reassign storage group
    #    only the volumes with initial storage 'none' can be reassigned

    def reassign_sg(self, ticket):

        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        storage_group = self.extract_value_from_ticket("storage_group", ticket)
        if not storage_group:
            return None, None #extract_value_from_ticket handles its own errors.

        if storage_group == 'none':
            message = "Can not assign to storage group 'none'"
            Trace.log(e_errors.ERROR, message)
            #Shouldn't ticket['status'] be set to an error here?
            self.reply_to_caller(ticket)
            return

        sg, ff, wp = string.split(record['volume_family'], '.')
        if sg != 'none': # can not do it
            message = "can not reassign from existing storage group %s" % (sg,)
            Trace.log(e_errors.ERROR, message)
            ticket['status'] = (e_errors.VOLUME_CLERK_ERROR, message)
            self.reply_to_caller(ticket)
            return

        # deal with quota

        library = record['library']
        q_dict = self.quota_enabled()
        if q_dict:
            if not self.check_quota(q_dict, library, storage_group):
                message = "(%s, %s) quota exceeded when reassiging blank " \
                          "volume to it. Contact enstore admin." \
                          % (library, storage_group)
                Trace.log(e_errors.ERROR, message)
                ticket["status"] = (e_errors.QUOTAEXCEEDED, message)
                self.reply_to_caller(ticket)
                return

        record['volume_family'] = string.join((storage_group, ff, wp), '.')

        self.volumedb_dict[external_label] = record
        sgc = self.get_sg_counter(library, 'none')
        if sgc < self.common_blank_low['alarm']:
            message = "(%s, %s) has only %d tapes left, less than %d" % \
                      (library, 'none', sgc, self.common_blank_low['alarm'])
            Trace.alarm(e_errors.ERROR, 'COMMON BLANK POOL LOW', message)
        elif sgc < self.common_blank_low['warning']:
            message = "(%s, %s) has only %d tapes left, less than %d" % \
                      (library, 'none', sgc, self.common_blank_low['warning'])
            Trace.alarm(e_errors.WARNING, 'COMMON BLANK POOL LOW', message)

        ticket['status'] = (e_errors.OK, None)
        message = "volume %s is assigned to storage group %s" % \
                  (external_label, storage_group)
        Trace.log(e_errors.INFO, message)
        self.reply_to_caller(ticket)
        return

    # set_comment() -- set comment to a volume record #### DONE

    def set_comment(self, ticket):

        external_label, record = self.extract_external_label_from_ticket(
            ticket, key = "vol")
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        comment = self.extract_value_from_ticket("comment", ticket)
        if not comment:
            return #extract_value_from_ticket handles its own errors.

        record['comment'] = comment
        self.volumedb_dict[external_label] = record
        self.change_state(external_label, 'set_comment', comment)
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### DONE
    # add: some sort of hook to keep old versions of the s/w out
    # since we should like to have some control over format of the records.
    def addvol(self, ticket):
        saved_reply_address = ticket.get('r_a')
        # create empty record and control what goes into database
        # do not pass ticket, for example to the database!
        record={}

        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        # This is a restricted service
        # but not for a disk media type
        media = ticket.get('media_type', None)
        if media and media == 'disk':
            status = None
        else:
            status = self.restricted_access(ticket)
        if status:
            message = "attempt to add volume %s from %s" \
                      % (external_label, self.reply_address[0])
            Trace.log(e_errors.ERROR, message)
            ticket['status'] = status
            ticket['r_a'] = saved_reply_address
            self.reply_to_caller(ticket)
            return

        # can't have 2 with same external_label
        if self.volumedb_dict.has_key(external_label):
            message = "%s: volume %s already exists" \
                      % (MY_NAME, external_label,)
            ticket["status"] = (e_errors.VOLUME_EXISTS, message)
            Trace.log(e_errors.ERROR, message)
            ticket['r_a'] = saved_reply_address
            self.reply_to_caller(ticket)
            return

        # first check quota
        if ticket.has_key('library') and ticket.has_key('storage_group'):
            library = ticket['library']
            sg = ticket['storage_group']
            if sg != 'none':
                # check if quota is enabled
                q_dict = self.quota_enabled()
                if q_dict:
                    if not self.check_quota(q_dict, library, sg):
                        message = "%s: (%s, %s) quota exceeded " \
                                  "while adding %s. Contact enstore admin." \
                                  % (MY_NAME, library, sg, external_label)
                        ticket["status"] = (e_errors.QUOTAEXCEEDED, message)
                        Trace.log(e_errors.ERROR, message)
                        ticket['r_a'] = saved_reply_address
                        self.reply_to_caller(ticket)
                        return
        else:
            message = "%s: key %s or %s is missing" \
                      % (MY_NAME, 'library', 'storage_group')
            ticket["status"] = (e_errors.KEYERROR, message)
            Trace.log(e_errors.ERROR, message)
            ticket['r_a'] = saved_reply_address
            self.reply_to_caller(ticket)
            return

        # check if library key is valid library manager name
        llm = self.csc.get_library_managers()
        # "shelf" library is a special case
        if ticket['library']!='shelf' and not llm.has_key(ticket['library']):
            Trace.log(e_errors.INFO,
                      " vc.addvol: Library Manager does not exist: %s "
                      % (ticket['library'],))

        # mandatory keys
        for key in  ['external_label','media_type', 'library',
                     'eod_cookie', 'capacity_bytes']:
            value = self.extract_value_from_ticket(key, ticket)
            if value == None:
                return #extract_value_from_ticket handles its own errors.
            record[key] = value

        # form a volume family
        storage_group = self.extract_value_from_ticket("storage_group", ticket)
        if not storage_group:
            return #extract_value_from_ticket handles its own errors.
        file_family = self.extract_value_from_ticket("file_family", ticket)
        if not file_family:
            return #extract_value_from_ticket handles its own errors.
        wrapper = self.extract_value_from_ticket("wrapper", ticket)
        if not wrapper:
            return #extract_value_from_ticket handles its own errors.

        record['volume_family'] = volume_family.make_volume_family(
            storage_group, file_family, wrapper)

        # set remaining bytes
        record['remaining_bytes'] = ticket.get('remaining_bytes', record['capacity_bytes'])

        # optional keys - use default values if not specified
        record['last_access'] = ticket.get('last_access', -1)
        record['first_access'] = ticket.get('first_access', -1)
        record['modification_time'] = ticket.get('modification_time', -1)
        record['declared'] = ticket.get('declared',-1)
        if record['declared'] == -1:
            record["declared"] = time.time()
        record['system_inhibit'] = ticket.get('system_inhibit', ["none", "none"])
        record['user_inhibit'] = ticket.get('user_inhibit', ["none", "none"])
        record['sum_wr_err'] = ticket.get('sum_wr_err', 0)
        record['sum_rd_err'] = ticket.get('sum_rd_err', 0)
        record['sum_wr_access'] = ticket.get('sum_wr_access', 0)
        record['sum_rd_access'] = ticket.get('sum_rd_access', 0)
        record['sum_mounts'] = ticket.get('sum_mounts', 0)
        record['non_del_files'] = ticket.get('non_del_files', 0)
        record['wrapper'] = ticket.get('wrapper', None)
        record['blocksize'] = ticket.get('blocksize', -1)
	record['si_time'] = [0.0, 0.0]
	record['comment'] = ""
        record['write_protected'] = 'n'
        if record['blocksize'] == -1:
            sizes = self.csc.get("blocksizes")
            try:
                msize = sizes[ticket['media_type']]
            except:
                message = "%s:  unknown media type = unknown blocksize" \
                          % (MY_NAME,)
                ticket['status'] = (e_errors.UNKNOWNMEDIA, message)
                Trace.log(e_errors.ERROR, message)
                ticket['r_a'] = saved_reply_address
                self.reply_to_caller(ticket)
                return
            record['blocksize'] = msize


        # write the ticket out to the database
        self.volumedb_dict[external_label] = record
        ticket["status"] = (e_errors.OK, None)
        ticket['r_a'] = saved_reply_address
        self.reply_to_caller(ticket)
        return

    #### DONE
    # modify:
    def modifyvol(self, ticket):

        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        # put the new values into our copy of the volume record
        mdr = {}
        for key in record.keys():
            if ticket.has_key(key):
                record[key]=ticket[key]
                mdr[key]=ticket[key] # keep a record

        # verify blocksize for this media type
        sizes = self.csc.get("blocksizes")
        try:
            msize = sizes[record['media_type']]
        except:
            message = "%s:  unknown media type = unknown blocksize" \
                      % (MY_NAME,)
            ticket['status'] = (e_errors.UNKNOWNMEDIA, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            return
        record['blocksize'] = msize
        mdr['blocksize'] = msize

        #If the media_type is null, make sure these additional values
        # are set correctly.
        if record['media_type']=='null':
            record['wrapper']='null'
            mdr['wrapper'] = 'null'

        # write the ticket out to the database
        self.volumedb_dict[external_label] = record
        Trace.log(e_errors.INFO, "volume has been modifyed %s" % (record,))
        # to make SQL happy
        mdr2 = string.replace(`mdr`, "'", '"')
        self.change_state(external_label, 'modified', mdr2)
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### DONE
    # delete a volume entry from the database
    # This is meant to be used only by trained professional ...
    # It removes the volume entry in the database ...
    # However, it does NOT coordinate with file_clerk nor pnfs ...
    # Its purpose is simply to clean up some portion of the database ...
    # Once an entry is removed, it is gone forever!

    def rmvolent(self, ticket):
        saved_reply_address = ticket.get('r_a')
        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.
        # This is a restricted service
        status = self.restricted_access(ticket)
        if status:
            message = "attempt to remove volume entry %s from %s" % \
                      (external_label, self.reply_address[0])
            Trace.log(e_errors.ERROR, message)
            ticket['status'] = status
            ticket['r_a'] = saved_reply_address
            self.reply_to_caller(ticket)
            return

        message = "removing volume %s from database. %s" % \
                  (external_label, `record`)
        Trace.log(e_errors.INFO, message)
        del self.volumedb_dict[external_label]
        ticket["status"] = (e_errors.OK, None)
        ticket['r_a'] = saved_reply_address
        self.reply_to_caller(ticket)
        return


    # Get the next volume that satisfy criteria #### DONE
    def next_write_volume (self, ticket):
        Trace.trace(20, "next_write_volume %s" % (ticket,))
        saved_reply_address = ticket.get('r_a', None)

        vol_veto = ticket["vol_veto_list"]
        vol_veto_list = udp_common.r_eval(vol_veto)

        # get the criteria for the volume from the user's ticket
        min_remaining_bytes = ticket["min_remaining_bytes"]
        library = ticket["library"]
        if self.lm_is_paused(library):
            ticket['status'] = (e_errors.BROKEN,'Too many volumes set to NOACCESS')
            self.reply_to_caller(ticket)
            return

        vol_fam = ticket['volume_family']
        first_found = ticket["first_found"]
        wrapper_type = volume_family.extract_wrapper(vol_fam)
        use_exact_match = ticket['use_exact_match']

        # go through the volumes and find one we can use for this request
        # first use exact match
        sg = volume_family.extract_storage_group(vol_fam)
        ff = volume_family.extract_file_family(vol_fam)
        Trace.trace(20, "next_write_volume %s %s" % (vol_fam, vol_fam))

        pool = vol_fam
        # To be backward compatible
        if not ticket.has_key('mover'):
            ticket['mover'] = {}
        mover_type = ticket['mover'].get('mover_type','Mover')
        if mover_type == 'DiskMover':
           use_exact_match = 1
           first_found = 1
        vol = self.find_matching_volume(library, vol_fam, pool,
                                        wrapper_type, vol_veto_list,
                                        first_found, min_remaining_bytes,exact_match=1,
                                        mover=ticket['mover'])
        Trace.trace(20, "1 find matching volume returned %s" % (vol,))

        if use_exact_match:
            if not vol or len(vol) == 0:
                # nothing was available at all
                if mover_type == 'DiskMover':
                    vol['external_label'] = None
                    vol['volume_family'] = vol_fam
                    vol['wrapper'] = wrapper_type
                    Trace.log(e_errors.INFO, "Assigning fake volume %s from storage group %s to library %s, volume family %s"
                              % (vol['external_label'], pool, library, vol_fam))
                    vol["status"] = (e_errors.OK, None)
                else:
                    message = "%s: no new volumes available [%s, %s]" \
                              % (MY_NAME, library, vol_fam)
                    ticket["status"] = (e_errors.NOVOLUME, message)
                    Trace.alarm(e_errors.ERROR, 'NO VOLUME', message)
                vol['r_a'] = saved_reply_address
                self.reply_to_caller(vol)
                return

        if not vol or len(vol) == 0:
            # nothing was available - see if we can assign a blank from a
            # given storage group and file family.
            pool = volume_family.make_volume_family(sg, ff, 'none')

            Trace.trace(20, "next_write_volume %s %s" % (vol_fam, pool))
            vol = self.find_matching_volume(library, vol_fam, pool, wrapper_type,
                                            vol_veto_list, first_found,
                                            min_remaining_bytes,exact_match=0)

            Trace.trace(20, "2 find matching volume returned %s" % (vol,))

        if not vol or len(vol) == 0:
            # nothing was available - see if we can assign a blank from a
            # given storage group
            pool = volume_family.make_volume_family(sg, 'none', 'none')

            Trace.trace(20, "next_write_volume %s %s" % (vol_fam, pool))
            vol = self.find_matching_volume(library, vol_fam, pool, wrapper_type,
                                            vol_veto_list, first_found,
                                            min_remaining_bytes,exact_match=0)

            Trace.trace(20, "3 find matching volume returned %s" % (vol,))

        inc_counter = 0
        if not vol or len(vol) == 0:
            # nothing was available - see if we can assign a blank from a
            # common pool
            pool = 'none.none.none'
            Trace.trace(20, "next_write_volume %s %s" % (vol_fam, pool))
            vol = self.find_matching_volume(library, vol_fam, pool, wrapper_type,
                                            vol_veto_list, first_found,
                                            min_remaining_bytes, exact_match=0)
            Trace.trace(20, "4 find matching volume returned %s" % (vol,))

            if vol and len(vol) != 0:
                # check if quota is enabled
                q_dict = self.quota_enabled()
                inc_counter = 1
                if q_dict:
                    if not self.check_quota(q_dict, library, sg):
                        message = "%s: (%s, %s) quota exceeded " \
                                  "while drawing from common pool. " \
                                  "Contact enstore admin." \
                                  % (MY_NAME, library, sg)
                        ticket["status"] = (e_errors.QUOTAEXCEEDED, message)
                        Trace.alarm(e_errors.ERROR, e_errors.QUOTAEXCEEDED,
                                    message)
                        if not library+'.'+sg in self.ignored_sg:
                            ic = inquisitor_client.Inquisitor(self.csc)
                            ic.override(enstore_constants.ENSTORE,
                                        enstore_constants.RED, reason=message,
                                        rcv_timeout=10, tries=1)
                            # release ic
                            del ic
                            self.reply_to_caller(ticket)
                            return

        # return blank volume we found
        if vol and len(vol) != 0:
            label = vol['external_label']
            if ff == "ephemeral":
                vol_fam = volume_family.make_volume_family(sg, label, wrapper_type)
            osg = volume_family.extract_storage_group(vol['volume_family'])
            vol['volume_family'] = vol_fam
            vol['wrapper'] = wrapper_type
            if vol['sum_wr_access'] != 0:
                filled_state = ""
            else:
                filled_state = "blank"
            message = "Assigning %s volume %s from storage group %s to " \
                      "library %s, volume family %s" \
                      % (filled_state, label, pool, library, vol_fam)
            Trace.log(e_errors.INFO, message)
            if inc_counter:
                if osg == 'none':
                    sgc = self.get_sg_counter(library, osg)
                    if sgc < self.common_blank_low['alarm']:
                        message = "(%s, %s) has only %d tapes left, less than %d" \
                                  % (library, 'none', sgc, self.common_blank_low['alarm'])
                        Trace.alarm(e_errors.ERROR, "COMMON BLANK POOL LOW",
                                    message)
                    elif sgc < self.common_blank_low['warning']:
                        message = "(%s, %s) has only %d tapes left, less than %d" \
                                  % (library, 'none', sgc, self.common_blank_low['warning'])
                        Trace.alarm(e_errors.WARNING, "COMMON BLANK POOL LOW",
                                    message)
            # update database
            self.volumedb_dict[label] = { 'volume_family' : vol['volume_family'], 'wrapper' : vol['wrapper'] }
            vol['status'] = (e_errors.OK, None)
            vol['r_a'] = saved_reply_address
            self.reply_to_caller(vol)
            return

        # nothing was available at all
        message = "%s: no new volumes available [%s, %s]" \
                  % (MY_NAME, library, vol_fam)
        ticket['status'] = (e_errors.NOVOLUME, message)
        # ignore NULL
        if volume_family.extract_wrapper(vol_fam) != 'null' and \
               library[:4] != 'null' and library[-4:] != 'null':
            Trace.alarm(e_errors.ERROR, 'NO VOLUME', message)
            # this is important so turn the enstore ball red
            if not library+'.'+sg in self.ignored_sg:
                ic = inquisitor_client.Inquisitor(self.csc)
                ic.override(enstore_constants.ENSTORE, enstore_constants.RED,
                            reason=message, rcv_timeout=10, tries=1)
                # release ic
                del ic
        self.reply_to_caller(ticket)
        return


    # check if specific volume can be used for write #### DONE
    def can_write_volume (self, ticket):

        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.
        min_remaining_bytes = self.extract_value_from_ticket("min_remaining_bytes", ticket)
        if not min_remaining_bytes:
            return #extract_value_from_ticket handles its own errors.
        library = self.extract_value_from_ticket("library", ticket)
        if not library:
            return #extract_value_from_ticket handles its own errors.
        vol_fam = self.extract_value_from_ticket("volume_family", ticket)
        if not vol_fam:
            return #extract_value_from_ticket handles its own errors.

        ticket["status"] = (e_errors.OK, None)
        if (record["library"] == library and
            (record["volume_family"] == vol_fam) and
            record["user_inhibit"][0] == "none" and
            record["user_inhibit"][1] == "none" and
            record["system_inhibit"][0] == "none" and
            record["system_inhibit"][1] == "none"):
            ##
            ##ret_st = self.is_volume_full(record, min_remaining_bytes)
            ##if ret_st:
            ##    ticket["status"] = (ret_st, None)
            ##

            if record["remaining_bytes"] < long(min_remaining_bytes*SAFETY_FACTOR):
                ticket["status"] = (e_errors.WRITE_EOT, "file too big")
            self.reply_to_caller(ticket)
            return
        else:
            ticket["status"] = (e_errors.NOACCESS, None)
            self.reply_to_caller(ticket)
            return

    #### DONE
    ##This should really be renamed, it does more than set_remaining_bytes
    # update the database entry for this volume
    def set_remaining_bytes(self, ticket):
        saved_reply_address = ticket.get('r_a', None)
        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        #
        # disk movers are accessed concurrently and model
        # "get record, change it, update in DB" does not work
        #
        if record["media_type"] == "disk" :
            record['r_a'] = saved_reply_address
            record["status"] = (e_errors.OK, None)
            self.reply_to_caller(record)

        eod_cookie = None
        eod_cookie_on_record = None

        try:
            eod_cookie_on_record = enstore_functions3.extract_file_number(record["eod_cookie"])
            eod_cookie           = enstore_functions3.extract_file_number(ticket["eod_cookie"])
        except KeyError,detail:
            ticket["status"] = (e_errors.KEYERROR, str(detail))
            Trace.log(e_errors.ERROR, str(detail))
            self.reply_to_caller(ticket)
            return
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.handle_error(exc, msg)
            ticket["status"] = (e_errors.VOLUME_CLERK_ERROR, str(msg))
            self.reply_to_caller(ticket)
            return

        if not eod_cookie :
            message = "eod_cookie is malformed : %s"%(ticket["eod_cookie"],)
            ticket["status"] = (e_errors.VOLUME_CLERK_ERROR, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            return

        if eod_cookie_on_record:
            if eod_cookie < eod_cookie_on_record:
                if record["media_type"] == "disk" :
                    #
                    # in case of disk media type we can have asyncronous set_remaining
                    # calls for a given volume. As these numbers do not matter for disk
                    # volume we ignore them
                    #
                    message = "disk media_type, refuse to set eod_cookie from %s, to %s"%(record["eod_cookie"],
                                                                                          ticket["eod_cookie"],)
                    ticket["status"] = (e_errors.OK, message)
                    Trace.log(e_errors.INFO, message)
                else:
                    #
                    # in case of non-disk media type we error out if there
                    # is an attempt to set eod_cookie wich is less than one on record
                    #
                    message = "Refuse to set eod_cookie from %s, to %s"%(record["eod_cookie"],
                                                                         ticket["eod_cookie"],)
                    ticket["status"] = (e_errors.VOLUME_CLERK_ERROR, message)
                    Trace.log(e_errors.ERROR, message)
                self.reply_to_caller(ticket)
                return

        # update the fields that have changed

        try:
            for key in ["remaining_bytes","eod_cookie"]:
                record[key] = ticket[key]

        except KeyError, detail:
            message = "%s: key %s is missing" % (MY_NAME, detail)
            ticket["status"] = (e_errors.KEYERROR, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            return

        if record["remaining_bytes"] == 0 and \
            record["system_inhibit"][1] == "none":
            record["system_inhibit"][1] = "full"
            self.change_state(external_label, 'system_inhibit_1', "full")
            record["si_time"][1] = time.time()

        record["last_access"] = time.time()
        if record["first_access"] == -1:
            record["first_access"] = record["last_access"]
        if record["modification_time"] == -1:
            record["modification_time"] = record["last_access"]

        # update the non-deleted file count if we wrote a new file to the tape
        bfid = ticket.get("bfid") #will be present when a new file is added
        if bfid:
            record['non_del_files'] = record['non_del_files'] + 1

        # record our changes
        self.volumedb_dict[external_label] = record
        record['r_a'] = saved_reply_address
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        return

    # decrement the file count on the volume #### DONE
    def decr_file_count(self, ticket):
        saved_reply_address = ticket.get('r_a', None)
        external_label = self.extract_external_label_from_ticket(ticket,check_exists=False)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        # assume the count is 1 unless specified
        count = ticket.get("count", 1)
        q="""
        update volume set non_del_files = non_del_files - {} where label='{}'
        """
        try:
            res = self.volumedb_dict.update(q.format(count,external_label))
            record = self.volumedb_dict[external_label]
            # need to sync w/ journal
            self.volumedb_dict.jou[external_label] = record
            record["status"] = (e_errors.OK, None)
            record['r_a'] = saved_reply_address
            self.reply_to_caller(record)
        except e_errors.EnstoreError as msg:
            ticket['status'] = (msg.type, str(msg))
            self.reply_to_caller(ticket)
        except:
            ticket['status'] = (e_errors.VOLUME_CLERK_ERROR, str(sys.exc_info()[1]))
            self.reply_to_caller(ticket)
        return

    # update the database entry for this volume #### DONE
    def update_counts(self, ticket):
        saved_reply_address = ticket.get('r_a', None)
        external_label, record = self.extract_external_label_from_ticket(ticket)

        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        #
        # disk movers are accessed concurrently and model
        # "get record, change it, update in DB" does not work
        #
        if record["media_type"] == "disk" :
            record["status"] = (e_errors.OK, None)
            record['r_a'] = saved_reply_address
            self.reply_to_caller(record)
            return

        # update the fields that have changed
        if ticket['wr_access'] == 1:
            record["modification_time"] = time.time()
        record["last_access"] = time.time()
        if record["first_access"] == -1:
            record["first_access"] = record["last_access"]

        for key in ['wr_err','rd_err','wr_access','rd_access','mounts']:
            try:
                record['sum_'+key] = record['sum_'+key] + ticket[key]
            except KeyError, detail:
                if key == 'mounts':
                    # FIX ME LATER!!!
                    if ticket.has_key('mounts'):
                        # make a new dictionary entry for the old tape records
                        record['sum_mounts'] = ticket[key]
                else:
                    message = "%s: key %s is missing" % (MY_NAME, detail)
                    ticket["status"] = (e_errors.KEYERROR, message)
                    Trace.log(e_errors.ERROR, message)
                    self.reply_to_caller(ticket)
                    return

        # record our changes
        self.volumedb_dict[external_label] = record
        record["status"] = (e_errors.OK, None)
        record['r_a'] = saved_reply_address
        self.reply_to_caller(record)
        return

    # touch(self, ticket) -- update last_access time #### DONE
    def touch(self, ticket):

        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        record['last_access'] = time.time()
        if record.has_key('modification_time'):
            record['modification_time'] = record['last_access']
        if record['first_access'] == -1:
            record['first_access'] = record['last_access']
        self.volumedb_dict[external_label] = record
        ticket["last_access"] = record['last_access']
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### Should have nothing to trim!!!
    # check_record(self, ticket) -- trim obsolete fileds #### DONE
    def check_record(self, ticket):

        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        changed = 0
        for i in ['at_mover', 'status', 'mounts']:
            if record.has_key(i):
                del record[i]
                changed = 1
        if changed:
            self.volumedb_dict[external_label] = record
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    # flag the database that we are now writing the system #### DONE
    def clr_system_inhibit(self, ticket):
        saved_reply_address = ticket.get('r_a', None)
        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            print "external_label:", external_label
            return #extract_external_label_from_ticket handles its own errors.

        inhibit = self.extract_value_from_ticket("inhibit", ticket)
        if not inhibit:
            inhibit = "system_inhibit" # set default field

        position = self.extract_value_from_ticket("position", ticket)
        if position == None:
            return #extract_value_from_ticket handles its own errors.

        # check the range of position
        if position != 0 and position != 1:
            message = "%s: clr_system_inhibit(%s, %d), no such position %d" \
                      % (MY_NAME, inhibit, position, position)
            ticket["status"] = (e_errors.VOLUME_CLERK_ERROR, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            return

        if (inhibit == "system_inhibit" and position == 0):
            if record [inhibit][position] == e_errors.DELETED:
                # if volume is deleted no data can be changed
                record["status"] = (e_errors.DELETED,
                                    "Cannot perform action on deleted volume")
            else:
                # update the fields that have changed
                record[inhibit][position] = "none"
                # set time stamp
                record['si_time'][position] = time.time()
                self.volumedb_dict[external_label] = record   # THIS WILL JOURNAL IT
                record["status"] = (e_errors.OK, None)
        else:
            # if it is not record["system_inhibit"][0] just set it to none
            record[inhibit][position] = "none"
            if inhibit == "system_inhibit":
                # set time stamp
                record['si_time'][position] = time.time()
            self.volumedb_dict[external_label] = record   # THIS WILL JOURNAL IT
            record["status"] = (e_errors.OK, None)
        if record["status"][0] == e_errors.OK:
            itype = inhibit+'_'+`position`
            self.change_state(external_label, itype, "none")
            message = "system inhibit %d cleared for %s" \
                      % (position, external_label)
            Trace.log(e_errors.INFO, message)
        record['r_a'] = saved_reply_address
        self.reply_to_caller(record)
        return

    # move a volume to a new library #### DONE
    def new_library(self, ticket):
        saved_reply_address = ticket.get('r_a', None)
        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        new_library = self.extract_value_from_ticket("new_library", ticket)
        if new_library == None:
            return #extract_value_from_ticket handles its own errors.

        # update the library field with the new library
        old_library = record ["library"]
        record ["library"] = new_library
        self.volumedb_dict[external_label] = record   # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        Trace.log(e_errors.INFO, 'volume %s is assigned from library %s to library %s'%(external_label, old_library, new_library))
        # log to its history
        self.change_state(external_label, 'new_library', '%s -> %s'%(old_library, new_library))
        record['r_a'] = saved_reply_address
        self.reply_to_caller(record)
        return

    # set system_inhibit flag #### DONE
    def set_system_inhibit(self, ticket, flag, index=0):
        saved_reply_address = ticket.get('r_a', None)
        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return ticket["status"] #extract_external_label_from_ticket handles its own errors.

        # update the fields that have changed

        # ??? why does it matter? ???
        # if flag is "readonly":
        #     # check if volume is blank
        #     if record['non_del_files'] == 0:
        #         record['status'] = (e_errors.CONFLICT, "volume is blank")
        #         self.reply_to_caller(record)
        #         return record["status"]
        record["system_inhibit"][index] = flag
        # record time
        record["si_time"][index] = time.time()
        self.volumedb_dict[external_label] = record   # THIS WILL JOURNAL IT
        self.change_state(external_label, 'system_inhibit_'+`index`, flag)
        record["status"] = (e_errors.OK, None)
        Trace.log(e_errors.INFO,external_label+" system inhibit set to "+flag)
        record['r_a'] = saved_reply_address
        self.reply_to_caller(record)
        return record["status"]

    #### DONE
    # set system_inhibit flag, flag the database that we are now writing the system
    def set_writing(self, ticket):
        return self.set_system_inhibit(ticket, "writing")

    # set system_inhibit flag to none #### DONE
    def set_system_none(self, ticket):
        return self.set_system_inhibit(ticket, "none")

    # flag that the current volume is readonly #### DONE
    def set_system_readonly(self, ticket):
        return self.set_system_inhibit(ticket, "readonly", 1)

    # flag that the current volume is migrated #### DONE
    def set_system_migrated(self, ticket):
        return self.set_system_inhibit(ticket, "migrated", 1)

    # flag that the current volume is being migrated #### DONE
    def set_system_migrating(self, ticket):
        return self.set_system_inhibit(ticket, "migrating", 1)

    # flag that the current volume is duplicated #### DONE
    def set_system_duplicated(self, ticket):
        return self.set_system_inhibit(ticket, "duplicated", 1)

    # flag that the current volume is being duplicated #### DONE
    def set_system_duplicating(self, ticket):
        return self.set_system_inhibit(ticket, "duplicating", 1)

    # flag that the current volume is cloned #### DONE
    def set_system_cloned(self, ticket):
        return self.set_system_inhibit(ticket, "cloned", 1)

    # flag that the current volume is being cloned #### DONE
    def set_system_cloning(self, ticket):
        return self.set_system_inhibit(ticket, "cloning", 1)

    # flag that the current volume is full #### DONE
    def set_system_full(self, ticket):
        return self.set_system_inhibit(ticket, "full", 1)


    # flag that the current volume is marked as noaccess #### DONE
    def set_system_noaccess(self, ticket):
        external_label = self.extract_external_label_from_ticket(
            ticket, check_exists = False)
        if not external_label:
            rc = (e_errors.OK, None)
            return rc #extract_external_label_from_ticket handles its own errors.

        message = "volume %s is set to %s" % \
                  (ticket["external_label"], e_errors.NOACCESS)
        Trace.alarm(e_errors.WARNING, message,
                    {"label":ticket["external_label"]})
        rc = self.set_system_inhibit(ticket, e_errors.NOACCESS)
        if rc[0] == e_errors.OK:
            self.pause_lm(ticket["external_label"])

        return rc

    # flag that the current volume is marked as not allowed #### DONE
    def set_system_notallowed(self, ticket):
        # Trace.alarm(e_errors.WARNING, e_errors.NOTALLOWED,
        #              {"label":ticket["external_label"]})
        message = "volume %s is set to NOTALLOWED" % (ticket['external_label'],)
        Trace.log(e_errors.INFO, message)
        return self.set_system_inhibit(ticket, e_errors.NOTALLOWED)


    def rebuild_sg_count(self, ticket):
        ticket['status'] = (e_errors.ERROR, "This function is deprecated")
        self.reply_to_caller(ticket)

    #### DONE
    def set_sg_count(self, ticket):
        ticket['status'] = (e_errors.ERROR, "This function is deprecated")
        self.reply_to_caller(ticket)

    # The following are for backups

    #### DONE
    def start_backup(self, ticket):
        try:
            self.volumedb_dict.start_backup()
            ticket["status"] = (e_errors.OK, None)
            ticket["start_backup"] = "yes"
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.handle_error(exc, msg)
            status = str(exc), str(msg)
            ticket["status"] = status
            ticket["start_backup"] = "no"

        self.reply_to_caller(ticket)


    #### DONE
    def stop_backup(self, ticket):
        try:
            Trace.log(e_errors.INFO,"stop_backup")
            self.volumedb_dict.stop_backup()
            ticket["status"] = (e_errors.OK, None)
            ticket["stop_backup"] = "yes"
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg=sys.exc_info()[:2]
            Trace.handle_error(exc, msg)
            status = str(exc), str(msg)
            ticket["status"] = status
            ticket["stop_backup"] = "no"

        self.reply_to_caller(ticket)

    #### DONE
    def backup(self, ticket):
        try:
            Trace.log(e_errors.INFO,"backup")
            self.volumedb_dict.backup()
            ticket["status"] = (e_errors.OK, None)
            ticket["backup"] = "yes"
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.handle_error(exc, msg)
            status = str(exc), str(msg)
            ticket["status"] = status
            ticket["backup"] = "no"

        self.reply_to_caller(ticket)

    #### DONE
    def clear_lm_pause(self, ticket):
        # m_changer = self.csc.get_media_changer(ticket['library'] + ".library_manager")
        #lib = ticket['library']

        lib = self.extract_value_from_ticket("library", ticket)
        if lib == None:
            return #extract_value_from_ticket handles its own errors.


        if len(lib) < 16 or lib[-16:] != '.library_manager':
            lib = lib + '.library_manager'
        m_changer = self.csc.get_media_changer(lib)
        if m_changer:
            if self.paused_lms.has_key(m_changer):
                message = "Cleared BROKEN flag for all LMs related to " \
                          "media changer %s" % (m_changer,)
                Trace.log(e_errors.INFO, message)
                self.paused_lms[m_changer] = {'paused':0,
                                              'noaccess_cnt': 0,
                                              'noaccess_time':time.time(),
                                              }
        self.max_noaccess_cnt = self.keys.get('max_noaccess_cnt', 2)
        self.noaccess_to = self.keys.get('noaccess_to', 300.)
        self.reply_to_caller({"status" : (e_errors.OK, None)})

    #### DONE
    # The following is for temporarily surpress raising the red ball
    # when new tape is drawn from the common pool. The operator may
    # use the following methods to set or clear a library.storage_group
    # in an ignored group list. This list is presistent across the
    # sessions

    #### DONE
    def set_ignored_sg(self, ticket):

        sg = self.extract_value_from_ticket("sg", ticket)
        if sg == None:
            return #extract_value_from_ticket handles its own errors.

        # check syntax

        if len(string.split(sg, '.')) != 2:
            message = 'wrong format. It has to be "library.storage_group"'
            ticket["status"] = (e_errors.WRONG_FORMAT, message)
            Trace.log(e_errors.ERROR, message)
            self.reply_to_caller(ticket)
            return

        if not sg in self.ignored_sg:
            self.ignored_sg.append(sg)
            # dump it to file
            try:
                f = open(self.ignored_sg_file, 'w')
                cPickle.dump(self.ignored_sg, f)
                f.close()

                message = 'storage group "%s" has been ignored' % (sg,)
                ticket['status'] = (e_errors.OK, self.ignored_sg)
                Trace.log(e_errors.INFO, message)
            except:
                message = '%s: failed to ignore storage group "%s"' \
                          % (MY_NAME, sg)
                ticket['status'] = (e_errors.VOLUME_CLERK_ERROR, message)
                Trace.log(e_errors.ERROR, message)
                #self.reply_to_caller(ticket)
                #return

        #ticket['status'] = (e_errors.OK, self.ignored_sg)
        self.reply_to_caller(ticket)
        return

    #### DONE
    def clear_ignored_sg(self, ticket):

        sg = self.extract_value_from_ticket("sg", ticket)
        if sg == None:
            return #extract_value_from_ticket handles its own errors.

        if sg in self.ignored_sg:
            self.ignored_sg.remove(sg)
            # dump it to file
            try:
                f = open(self.ignored_sg_file, 'w')
                cPickle.dump(self.ignored_sg, f)
                f.close()

                message = 'ignored storage group "%s" has been cleared' % (sg,)
                ticket['status'] = (e_errors.OK, self.ignored_sg)
                Trace.log(e_errors.INFO, message)
            except:
                message = '%s: failed to clear ignored storage ' \
                          'group "%s"' % (MY_NAME, sg,)
                ticket['status'] = (e_errors.VOLUME_CLERK_ERROR, message)
                Trace.log(e_errors.ERROR, message)
                #self.reply_to_caller(ticket)
                #return
        else:
            message = '"%s" is not in ignored storage group list' % (sg,)
            ticket['status'] = (e_errors.VOLUME_CLERK_ERROR, message)
            #self.reply_to_caller(ticket)
            #return

        #ticket['status'] = (e_errors.OK, self.ignored_sg)
        self.reply_to_caller(ticket)
        return

    #### DONE
    def clear_all_ignored_sg(self, ticket):
        try:
            self.ignored_sg = []
            f = open(self.ignored_sg_file, 'w')
            cPickle.dump(self.ignored_sg, f)
            f.close()

            message = "all ignored storage groups has been cleared"
            ticket['status'] = (e_errors.OK, self.ignored_sg)
            Trace.log(e_errors.INFO, message)
        except:
            message = "failed to clear all ignored storage groups"
            ticket['status'] = (e_errors.VOLUME_CLERK_ERROR, message)
            Trace.log(e_errors.ERROR, message)
            #self.reply_to_caller(ticket)
            #return

        #ticket['status'] = (e_errors.OK, self.ignored_sg)
        self.reply_to_caller(ticket)
        return


    #### DONE
    # Check if volume is available
    def is_vol_available(self, ticket):

        external_label, record = self.extract_external_label_from_ticket(ticket)
        if not external_label:
            return #extract_external_label_from_ticket handles its own errors.

        work = self.extract_value_from_ticket("action", ticket)
        if work == None:
            return #extract_value_from_ticket handles its own errors.

        # if to many volumes are NOACCESS, return an error condition here
        # even if our particular volume is not marked NOACCESS
        if self.lm_is_paused(record['library']):
            message = "Too many volumes set to NOACCESS"
            ticket['status'] = (e_errors.BROKEN, message)
            self.reply_to_caller(ticket)
            return


        ret_stat = (e_errors.OK,None)
        message = "is_vol_available system_inhibit = %s user_inhibit = %s " \
                  "ticket = %s" % \
                  (record['system_inhibit'], record['user_inhibit'], ticket)
        Trace.trace(35, message)
        if record["system_inhibit"][0] == e_errors.DELETED:
            ret_stat = (record["system_inhibit"][0], None)
        else:
            if work == 'read_from_hsm':
                Trace.trace(35, "is_vol_available: reading")
                # if system_inhibit is NOT in one of the following
                # states it is NOT available for reading
                if record['system_inhibit'][0] != 'none':
                    ret_stat = (record['system_inhibit'][0], None)
                elif not enstore_functions2.is_readable_state(
                    record['system_inhibit'][1]):
                    ret_stat = (record['system_inhibit'][1], None)
                # if user_inhibit is NOT in one of the following
                # states it is NOT available for reading
                elif record['user_inhibit'][0] != 'none':
                    ret_stat = (record['user_inhibit'][0], None)
                elif not enstore_functions2.is_readable_state(
                    record['user_inhibit'][1]):
                    ret_stat = (record['user_inhibit'][1], None)
                else:
                    ret_stat = (e_errors.OK,None)
            elif work == 'write_to_hsm':
                Trace.trace(35, "is_vol_available: writing")
                if record['system_inhibit'][0] != 'none':
                    ret_stat = (record['system_inhibit'][0], None)
                elif enstore_functions2.is_migration_state(record['system_inhibit'][1]):
                    # treated as readonly
                    ret_stat = ('readonly', None)
                elif (record['system_inhibit'][1] == 'readonly' or
                      record['system_inhibit'][1] == 'full'):
                    ret_stat = (record['system_inhibit'][1], None)
                elif record['user_inhibit'][0] != 'none':
                    ret_stat = (record['user_inhibit'], None)
                elif (record['user_inhibit'][1] == 'readonly' or
                      record['user_inhibit'][1] == 'full'):
                    ret_stat = (record['user_inhibit'][1], None)
                else:
                    ff = volume_family.extract_file_family(ticket['volume_family'])
                    Trace.trace(35, "is_vol_available: ticket %s, record %s" %
                                (ticket['volume_family'],record['volume_family']))

                    #XXX deal with 2-tuple vs 3-tuple...
                    if (volume_family.match_volume_families(ticket['volume_family'],record['volume_family']) or
                        ff == 'ephemeral'):
                        ret = self.is_volume_full(record,ticket['file_size'])
                        if not ret:
                            ret_stat = (e_errors.OK,None)
                        else:
                            ret_stat = (ret, None)
                    else: ret_stat = (e_errors.NOACCESS,None)
            else:
                ret_stat = (e_errors.UNKNOWN,None)
        ticket['status'] = ret_stat
        Trace.trace(35, "is_volume_available: returning %s " %(ret_stat,))
        self.reply_to_caller(ticket)


    #set_migration_history():  Update the migration_history table for
    # the source and destination volumes.
    def set_migration_history(self, ticket):
        #Get the source and destination volumes' DB ID.
        ticket = self._migration_history(ticket)

        # Insert this volume combintation into the migration_history table.
        q = "insert into migration_history (src, src_vol_id, dst, dst_vol_id) \
             values ('%s', '%s', '%s', '%s');" \
             % (ticket['src_vol'], ticket['src_vol_id'],
                ticket['dst_vol'], ticket['dst_vol_id'])

        try:
            self.volumedb_dict.insert(q)

            ticket['status'] = (e_errors.OK, None)
        except e_errors.EnstoreError as msg:
            ticket['status'] = (msg.type,
                                "Failed to update migration_history for %s due to: %s" \
                                % ((ticket['src_vol'], ticket['dst_vol']), str(msg)))
        except:
            ticket['status'] = (e_errors.VOLUME_CLERK_ERROR,
                                "Failed to update migration_history for %s due to: %s" \
                                % ((ticket['src_vol'], ticket['dst_vol']), str(sys.exc_info()[1])))
        self.reply_to_caller(ticket)
        return

    #set_migration_history_closed():  Update the migration_history table for
    # the source and destination volumes.
    def set_migration_history_closed(self, ticket):
        #Get the source and destination volumes' DB ID.
        ticket = self._migration_history(ticket)

        # Update the closed field for this volume combintation in the
        # migration_history table.
        q = "update migration_history set closed_time = current_timestamp " \
            "where migration_history.src_vol_id = '%s' " \
            "      and migration_history.dst_vol_id = '%s';" % \
            (ticket['src_vol_id'], ticket['dst_vol_id'])

        try:
            self.volumedb_dict.insert(q)

            ticket['status'] = (e_errors.OK, None)
        except e_errors.EnstoreError as msg:
            ticket['status'] = (msg.type,
                      "Failed to update migration_history for %s due to: %s" \
                      % ((ticket['src_vol'], ticket['dst_vol']), str(msg)))
        except:
            ticket['status'] = (e_errors.VOLUME_CLERK_ERROR,
                      "Failed to update migration_history for %s due to: %s" \
                      % ((ticket['src_vol'], ticket['dst_vol']),  str(sys.exc_info()[1])))

        self.reply_to_caller(ticket)
        return


class VolumeClerk(VolumeClerkMethods, generic_server.GenericServer):
    def __init__(self, csc):
        # basically, to make pychecker happy
        generic_server.GenericServer.__init__(self, csc, MY_NAME,
                                              function = self.handle_er_msg)
        Trace.init(self.log_name,"yes")

        VolumeClerkMethods.__init__(self, csc)
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
	self.alive_interval = monitored_server.get_alive_interval(self.csc,
								  MY_NAME,
								  self.keys)

        self.set_error_handler(self.vol_error_handler)
        self.connection_failure = 0

        # setup the communications with the event relay task
        self.resubscribe_rate = 300
        self.erc.start([event_relay_messages.NEWCONFIGFILE],
                       self.resubscribe_rate)

        # start our heartbeat to the event relay process
	self.erc.start_heartbeat(enstore_constants.VOLUME_CLERK,
				 self.alive_interval)

    def vol_error_handler(self, exc, msg, tb):
        __pychecker__ = "unusednames=tb"
        self.reply_to_caller({'status':(str(exc),str(msg), 'error'),
            'exc_type':str(exc), 'exc_value':str(msg), 'traceback':str(tb)} )


    #### DONE
    def quit(self, ticket):
	self.volumedb_dict.close()
	for t in self.parallelThreads:
            self.parallelThreadQueue.put(None)
	for t in self.parallelThreads:
		t.join(10.)
	dispatching_worker.DispatchingWorker.quit(self, ticket)

class VolumeClerkInterface(generic_server.GenericServerInterface):
        pass

if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME),"yes")

    # get the interface
    intf = VolumeClerkInterface()

    # get a volume clerk
    vc = VolumeClerk((intf.config_host, intf.config_port))
    vc.handle_generic_commands(intf)

    Trace.log(e_errors.INFO, '%s' % (sys.argv,))

    while 1:
        try:
            Trace.log(e_errors.INFO,'Volume Clerk (re)starting')
            vc.serve_forever()
        except e_errors.EnstoreError:
            continue
        except SystemExit, exit_code:
            vc.volumedb_dict.close()
            sys.exit(exit_code)
        except:
            vc.serve_forever_error(vc.log_name)
            continue
    Trace.log(e_errors.ERROR,"Volume Clerk finished (impossible)")
