#!/usr/bin/env python

"""
Library manager receives client requests (encp)
and directs them to assigned movers.
It manages encp request queue and selects a better appropriate request
based on different criteria, such as priority, location on tape, etc.
"""

# system imports
import os
import time
import traceback
import sys
import string
import socket
import select
import errno
import fcntl
import re
import copy
import threading

# enstore imports


import volume_clerk_client
import file_clerk_client
import callback
import hostaddr
import dispatching_worker
import generic_server
import event_relay_messages
import event_relay_client
import monitored_server
import enstore_constants
import option
import Trace
import udp_client
import enstore_functions2

import manage_queue
import e_errors
import lm_list
import volume_family
import priority_selector
import mover_constants
import charset
import discipline
import encp_ticket
import udp_server
import cleanUDP
import udp_common
import checksum
import file_cache_status

KB=enstore_constants.KB
MB=enstore_constants.MB
GB=enstore_constants.GB
SAFETY_FACTOR=enstore_constants.SAFETY_FACTOR
MIN_LEFT=enstore_constants.MIN_LEFT
INQUIRE_VOL_TO = 15
INQUIRE_VOL_RETRY = 2

DEBUG_LOG=9 # make entries in DEBUGLOG file at this level


# Trace levels for different classes and methods
# All timing - 100
# Requests: 300 - 309
# SG_VF: 310 - 319
# AtMovers: 320 - 329
# PostponedRequests: 330 - 339
# LibraryManagerMethods 200 - 239 no internal loops
#                       240 internal loops
# Library manager 11 - 99

def get_storage_group(dict):
    sg = dict.get('storage_group', None)
    if not sg:
	vf = dict.get('volume_family', None)
	if vf:
	    sg = volume_family.extract_storage_group(vf)
    return sg

def log_q_message(dict, q_txt):
    sg = get_storage_group(dict)
    if sg:
	# this message is used to create a plot. do not change the format of the message.
	Trace.log(e_errors.INFO,
		  "%s request added to %s queue for storage group : %s"%(Trace.MSG_ADD_TO_LMQ,
									 q_txt, sg))

def log_add_to_pending_queue(dict):
    log_q_message(dict, enstore_constants.PENDING)

def log_add_to_wam_queue(dict):
    log_q_message(dict, enstore_constants.WAM)


#########################

def thread_is_running(thread_name):
    """
    check if named thread is running

    :type thread_name: :obj:`str`
    :arg thread_name: thread name
    """

    threads = threading.enumerate()
    for thread in threads:
        if ((thread.getName() == thread_name) and thread.isAlive()):
            return True
    else:
        return False


##############################################################
class Requests:
    """
    Defines the queue of request coming from all kind of clients.
    Currently there are 2 types of clients: encp and volume assert.

    """
    def __init__(self, worker=None, lm_server = None, processing_function=None, *args):
        """

        :type worker: :class:`dispatching_worker.DispatchingWorker`
        :arg worker: dispatching worker instance
        :type lm_server: :class:`LibraryManager`
        :arg lm_server: library manager instance
        :type processing_function: :obj:`callable`
        :arg processing_function: function, which will process request
        :type args: :obj:`tuple`
        :arg args: arguments of request processing function
        """
        if worker == None:
           raise e_errors.EnstoreError(None, "Worker is not defined", e_errors.WRONGPARAMETER)

        self.trace_level = 300 # trace level for class Requests
        self.worker = worker
        if lm_server:
           self.lm_server = lm_server
        else:
           self.lm_server = worker

        if processing_function:
            self.processing_function = processing_function
            self.args = args
        else:
            self.processing_function = self.do_one_request
            self.args = ()

    def get(self):
        """
        Get request, coming from the client.

        :rtype: :obj:`tuple` (:obj:`str` - message, :obj:`tuple` (:obj:`str`- IP address, :obj:`int` - port) - client address)
        """
        Trace.trace(self.trace_level," Requests: get")
        ret = self.worker.get_request()
        Trace.trace(self.trace_level," Requests: get %s"%(ret,))
        return ret

    def do_one_request(self):
        """Receive and process one request, possibly blocking."""
        # request is a "(idn,number,ticket)"
        request, client_address = self.get()
        Trace.trace(self.trace_level, "Requests: do_one_request:get returned %s %s"%(request, client_address))
        Trace.trace(self.trace_level, "Requests: do_one_request:interval functions %s"%(self.worker.interval_funcs.items()))
        if request is None: #Invalid request sent in
            return

        if request == '':
            # nothing returned, must be timeout
            self.worker.handle_timeout()
            return
        try:
            self.process_request(request, client_address)
        except KeyboardInterrupt:
            Trace.trace(self.trace_level, "do_one_request: KeyboardInterrupt")
            traceback.print_exc()
        except SystemExit, code:
            Trace.trace(self.trace_level, "do_one_request: SystemExit %s"%(code,))
            # processing may fork (forked process will call exit)
            sys.exit( code )
        except:
            Trace.trace(self.trace_level, "do_one_request: other exception")
            self.worker.handle_error(request, client_address)


    def serve_forever(self):
        """
        Cloned from :class:`dispatching_worker.serve_forever` to run on a separate port.
        This needs to run in a thread as it an infinite loop!
        """
        Trace.trace(self.trace_level, "Requests starting %s"%(self,))
        count = 0
        if self.worker.use_raw:
            self.worker.set_out_file()
            if self.worker.allow_callback:
                Trace.trace(self.trace_level, "Request:spawning get_fd_message")
                # spawn callback processing thread (event relay messages)
                dispatching_worker.run_in_thread("call_back_proc", self.worker.serve_callback)
            # start receiver thread or process
            self.worker.raw_requests.receiver()

        while not self.worker.is_child:
            Trace.trace(self.trace_level, "Requests: serve_forever: calling do_one_request")
            self.do_one_request()
            Trace.trace(self.trace_level, "Requests: serve_forever: done do_one_request")

            now=time.time()
            for func, time_data in self.worker.interval_funcs.items():
                interval, last_called, one_shot = time_data
                Trace.trace(self.trace_level, "Requests: do_one_request: func %s interval %s now-last_called %s one_shot %s"%(func, interval, now-last_called, one_shot))
                if now - last_called > interval:
                    if one_shot:
                        del self.worker.interval_funcs[func]
                    else: #record last call time
                        self.worker.interval_funcs[func][1] =  now
                    Trace.trace(self.trace_level, "do_one_request: calling interval function %s"%(func,))
                    func()

            self.worker.collect_children()
            count = count + 1
            #if count > 100:
            if count > 20:
                self.worker.purge_stale_entries()
                count = 0

        if self.worker.is_child:
            Trace.trace(self.trace_level,"serve_forever, child process exiting")
            os._exit(0) ## in case the child process doesn't explicitly exit
        else:
            Trace.trace(self.trace_level,"serve_forever, shouldn't get here")

    def process_request(self, request, client_address):
        """
        Process incoming request.
        This method defines what method to run based on work contained in request and executes this method.

        :type request: :obj:`str`
        :arg  request: message
        :type client_address: :obj:`tuple`
        :arg client_address: (:obj:`str`- IP address, :obj:`int` - port)
        """
        t1=time.time()
        Trace.trace(self.trace_level, "RequestQeue:process_request %s"%(request,))
        ticket = udp_server.UDPServer.process_request(self.worker, request,
                                                      client_address)
        Trace.trace(self.trace_level, "RequestQeue:process_request: ticket %s"%(ticket,))
        t11=time.time()

        if not ticket:
            Trace.trace(self.trace_level, "RequestQeue:process_request. No ticket!!!")
            return

        # look in the ticket and figure out what work user wants
        try:
            function_name = ticket["work"]
        except (KeyError, AttributeError, TypeError), detail:
            ticket = {'status' : (e_errors.KEYERROR,
                                  "cannot find any named function")}
            msg = "%s process_request %s from %s" % \
                  (detail, ticket, client_address)
            Trace.log(e_errors.ERROR, msg)
            self.worker.reply_to_caller(ticket)
            self.worker.done_cleanup(ticket)
            return

        Trace.trace(self.trace_level, "GETTING %s of %s"%(function_name, self.lm_server))
        try:
            function = getattr(self.lm_server, function_name)
        except (KeyError, AttributeError, TypeError), detail:
            ticket = {'status' : (e_errors.KEYERROR,
                                  "cannot find requested function `%s'"
                                  % (function_name,))}
            msg = "%s process_raw_request %s %s from %s" % \
                  (detail, ticket, function_name, client_address)
            Trace.log(e_errors.ERROR, msg)
            self.worker.reply_to_caller(ticket)
            self.worker.done_cleanup(ticket)
            return

        # call the user function
        t = time.time()
        Trace.trace(self.trace_level,"Requests:process_request: function %s"%(function_name, ))

        if function_name in ('mover_idle', 'mover_busy', 'mover_bound_volume', 'mover_error'):
            if self.lm_server.use_threads:
                thread_name = ticket['mover']
                Trace.trace(self.trace_level, "Requests:process_request:thread starting %s"%(thread_name,))
                dispatching_worker.run_in_thread(thread_name, self.lm_server.request_thread, (function, ticket,))
                self.worker.done_cleanup(ticket)
            else:
                Trace.trace(self.trace_level, "Requests:process_request: calling %s(%s)"%(function, ticket))
                try:
                    function(ticket)
                except:
                    exc, msg, tb = sys.exc_info()
                    Trace.trace(self.trace_level, "Requests:process_request: exception %s %s"%(exc, msg))
                    Trace.handle_error(exc, msg, tb)
                t2 = time.time()
                Trace.trace(self.trace_level, "Requests:process_request: finished %s(%s)"%(function, ticket))
                self.worker.done_cleanup(ticket)
                if t2 - t >= 3.:
                    # leave this for debugging purposes
                    """
                    Trace.trace(5,"process_mover_request: changing logging") # leave this on dispatching worker level
                    try:
                        if not self.lch:
                            self.lm_server._do_print({'levels':range(500)})
                    except AttributeError:
                        self.lm_server._do_print({'levels':range(500)})
                        self.lch = True
                    """
                    pass
        else:
            Trace.trace(self.trace_level,"Requests:process_request: other function called")
            function(ticket)
            t2 = time.time()
            self.worker.done_cleanup(ticket)
            Trace.trace(self.trace_level,"Request:process_request: function %s time %s %s %s"%(function_name,t2-t, t2-t1, t11-t1))


##############################################################
class SG_VF:
    """
    Active movers per storage group / volume family.
    This class is used in AtMovers class.
    """
    def __init__(self):
        """
        Internally this class has two dictionaries.
        Active requests are stored into dictionary based on storage group and
        into another dictionary based on volume family
        """
        self.sg = {}
        self.vf = {}
        self.trace_level = 310

    def delete(self, mover, volume, sg, vf):
        """
        Delete active request.

        :type mover: :obj:`str`
        :arg mover: active mover name
        :type volume: :obj:`str`
        :arg volume: volume associated with active mover
        :type sg: :obj:`str`
        :arg sg: storage group name associated with active mover
        :type vf: :obj:`str`
        :arg vf: volume family associated with active mover
        :rtype: :obj:`int` 0 - success, 1- failure
        """
        rc = 0
        #if not (mover and volume and sg and vf): return
        Trace.trace(self.trace_level, "SG:delete mover %s, volume %s, sg %s, vf %s" % (mover, volume, sg, vf))
        if self.sg.has_key(sg) and (mover, volume) in self.sg[sg]:
            self.sg[sg].remove((mover, volume))
            if len(self.sg[sg]) == 0:
                del(self.sg[sg])
        else:
            Trace.log(DEBUG_LOG,'can not remove from sg %s %s' % (mover, volume))
            Trace.log(DEBUG_LOG, 'SG: %s' % (self.sg,))
            rc = -1
        if self.vf.has_key(vf) and (mover, volume) in self.vf[vf]:
            self.vf[vf].remove((mover, volume))
            if len(self.vf[vf]) == 0:
                del(self.vf[vf])
        else:
            rc = -1
        return rc

    def delete_mover(self, mover):
        """
        Find and delete active request for specified mover.

        :type mover: :obj:`str`
        :arg mover: active mover name
        :rtype: :obj:`int` 0 - success, 1- failure
        """
        rc = 0
        m,v = None, None
        # delete from sg
        for key in self.sg.keys():
            for tpl in self.sg[key]:
                if tpl[0] == mover:
                    m,v = tpl[0], tpl[1]
                    break
            if m and v:
                Trace.trace(self.trace_level,"delete_mover %s from %s"%(mover,self.sg[key]))
                break
        if m and v:
            self.sg[key].remove((m,v))
            if len(self.sg[key]) == 0:
                del(self.sg[key])
        else:
            Trace.log(DEBUG_LOG,'delete_mover can not remove from sg %s %s' % (m, v))
            Trace.log(DEBUG_LOG, 'delete_mover SG: %s' % (self.sg,))

        # now delete from vf
        m,v = None, None
        for key in self.vf.keys():
            for tpl in self.vf[key]:
                if tpl[0] == mover:
                    m,v = tpl[0], tpl[1]
                    break
            if m and v:
                Trace.trace(self.trace_level,"delete_mover %s from %s"%(mover,self.vf[key]))
                break
        if m and v:
            self.vf[key].remove((m,v))
            if len(self.vf[key]) == 0:
                del(self.vf[key])
        else:
            rc = -1
            Trace.log(DEBUG_LOG,'delete_mover can not remove from vf %s %s' % (m, v))
            Trace.log(DEBUG_LOG,'delete_mover VF: %s' % (self.vf,))
        return rc

    def put(self, mover, volume, sg, vf):
        """
        Add active request.

        :type mover: :obj:`str`
        :arg mover: active mover name
        :type volume: :obj:`str`
        :arg volume: volume associated with active mover
        :type sg: :obj:`str`
        :arg sg: storage group name associated with active mover
        :type vf: :obj:`str`
        :arg vf: volume family associated with active mover
        """
        self.delete(mover, volume, sg, vf) # delete entry to update content
        if not self.sg.has_key(sg):
            self.sg[sg] = []
        if not self.vf.has_key(vf):
            self.vf[vf] = []
        if not ((mover, volume) in self.sg[sg]):
            self.sg[sg].append((mover,volume))
        if not ((mover, volume) in self.vf[vf]):
            self.vf[vf].append((mover,volume))
        Trace.log(DEBUG_LOG, "%s"%(self,))

    def __repr__(self):
        return "<storage groups %s volume_families %s >" % (self.sg, self.vf)

##############################################################
class AtMovers:
    """
    Active movers

    List of movers with assigned work or bound tapes.

    Library manager uses this list to keep the information about
    movers, that are in the following states:

    ``MOUNT_WAIT`` - waiting for tape to be mounted

    ``SETUP`` - mover sets up connection with client

    ``HAVE_BOUND`` - mover has a mounted tape

    ``ACTIVE`` - data transfer

    ``DISMOUNT_WAIT`` - wiating for tape to be dismounted
    """

    def __init__(self):
        self.at_movers = {}
        self.sg_vf = SG_VF()
        self.max_time_in_active = 7200
        self.max_time_in_other = 1200
        self.dont_update = {}
        self._lock = threading.Lock()
        self.alarm_sent = []
        self.trace_level = 320

    def put(self, mover_info):
        """
        Add active request.

        :type mover_info: :obj:`dict`
        :arg mover_info: dictionary containing the following information:

           mover :obj:`str` - mover name

           extrenal_label :obj:`str` - volume name

           volume_family :obj:`str` - volume faimily

           work :obj:`str` - read_from_hsm, write_to_hsm, volume_assert

           current location :obj:`str` - location cookie
        """

        state = mover_info.get('state')
        if state == 'IDLE':
            return
        Trace.trace(self.trace_level,"AtMovers:put: %s" % (mover_info,))
        Trace.trace(self.trace_level,"AtMovers put before: at_movers: %s" % (self.at_movers,))
        Trace.trace(self.trace_level+1,"AtMovers put before: sg_vf: %s" % (self.sg_vf,))
        Trace.trace(self.trace_level,"dont_update: %s" % (self.dont_update,))
        if not mover_info['external_label']: return
        if not mover_info['volume_family']: return
        if not mover_info['mover']: return
        mover = mover_info['mover']
        if self.dont_update and self.dont_update.has_key(mover):
            if state == self.dont_update[mover]:
                return
            else:
                self._lock.acquire()
                del(self.dont_update[mover])
                self._lock.release()

        storage_group = volume_family.extract_storage_group(mover_info['volume_family'])
        vol_family = mover_info['volume_family']
        mover_info['updated'] = time.time()
        if self.at_movers.has_key(mover):
            if self.at_movers[mover]['external_label'] != mover_info['external_label']:
                return
            self.at_movers[mover].update(mover_info)
        else:
            # new entry
            mover_info['time_started'] = mover_info.get("current_time", time.time())
            self.at_movers[mover] = mover_info
        self.sg_vf.put(mover, mover_info['external_label'], storage_group, vol_family)
        Trace.trace(self.trace_level,"AtMovers put: at_movers: %s" % (self.at_movers,))
        Trace.trace(self.trace_level+1,"AtMovers put: sg_vf: %s" % (self.sg_vf,))

    def delete(self, mover_info):
        """
        Delete active request identified by mover_info

        :type mover_info: :obj:`dict`
        :arg mover_info: dictionary containing the following information:

           mover :obj:`str` - mover name

           extrenal_label :obj:`str` - volume name

           volume_family :obj:`str` - volume faimily

           work :obj:`str` - read_from_hsm, write_to_hsm, volume_assert

           current location :obj:`str` - location cookie
        :rtype: :obj:`int` 0 - success, 1- failure
        """

        Trace.trace(self.trace_level, "AtMovers delete. before: %s" % (self.at_movers,))
        Trace.trace(self.trace_level+1, "AtMovers delete. before: sg_vf: %s" % (self.sg_vf,))
        mover = mover_info['mover']
        mover_state = mover_info.get('state', None)
        rc = -1
        if self.at_movers.has_key(mover):
            Trace.trace(self.trace_level, "MOVER %s" % (self.at_movers[mover],))
            if  mover_info.has_key('volume_family') and mover_info['volume_family']:
                vol_family = mover_info['volume_family']
            else:
                vol_family = self.at_movers[mover]['volume_family']

            if mover_info.has_key('external_label') and mover_info['external_label']:
                label = mover_info['external_label']
            else:
                label = self.at_movers[mover]['external_label']
                # due to the mover bug mticket['volume_family'] may not be a None
                # when mticket['external_label'] is None
                # the following fixes this
                vol_family = self.at_movers[mover]['volume_family']
            #vol_family = self.at_movers[mover]['volume_family']
            #self.sg_vf.delete(mover, self.at_movers[mover]['external_label'], storage_group, vol_family)
            storage_group = volume_family.extract_storage_group(vol_family)
            rc = self.sg_vf.delete(mover, label, storage_group, vol_family)
            Trace.trace(self.trace_level, "AtMovers delete. sg_vf.delete returned %s" % (rc,))
            if (rc < 0 and mover_state == 'IDLE'):
                # the pair (mover, volume) is wrong.
                # This usually happens when mover automatically goes to
                # IDLE after ERROR in cases when the tape mount fails
                rc = self.sg_vf.delete_mover(mover)

            self._lock.acquire()
            del(self.at_movers[mover])
            self._lock.release()
        Trace.trace(self.trace_level+1,"AtMovers delete: at_movers: %s" % (self.at_movers,))
        Trace.trace(self.trace_level,"AtMovers delete: sg_vf: %s" % (self.sg_vf,))
        return rc

    def check(self):
        """
        Check how long movers did not update their state and act according to the rules.
        """
        Trace.trace(self.trace_level+2, "checking at_movers list")
        Trace.trace(self.trace_level+2, "dont_update_list %s"%(self.dont_update,))
        now = time.time()
        movers_to_delete = []
        if self.at_movers:
            try:
                # if check runs in thread at_movers can be modified, while
                # the loop below runs
                # on the other hand we do not want to lock acess to at_movers
                for mover in self.at_movers.keys():
                    Trace.trace(self.trace_level+2, "Check mover %s now %s"%(self.at_movers[mover], now))
                    if int(now) - int(self.at_movers[mover]['updated']) > 600:
                        #Trace.alarm(e_errors.ALARM,
                        #            "The mover %s has not updated its state for %s minutes, will remove it from at_movers list"%
                        #            (mover, int((now - self.at_movers[mover]['updated'])/60)))
                        Trace.log(e_errors.ERROR,
                                  "The mover %s has not updated its state for %s minutes, will remove it from at_movers list"%
                                  (mover, int((now - self.at_movers[mover]['updated'])/60)))
                        movers_to_delete.append(mover)
                    else:
                        Trace.trace(self.trace_level+2, "mover %s"%(mover,))
                        add_to_list = 0
                        time_in_state = int(self.at_movers[mover].get('time_in_state', 0))
                        state = self.at_movers[mover].get('state', 'unknown')
                        operation = self.at_movers[mover].get('operation', 'unknown')
                        current_location = self.at_movers[mover].get('current_location', '')
                        if time_in_state > self.max_time_in_other:
                            if state not in ['IDLE', 'ACTIVE', 'OFFLINE','HAVE_BOUND', 'SEEK', 'MOUNT_WAIT', 'DISMOUNT_WAIT']:
                                add_to_list = 1
                            if time_in_state > self.max_time_in_active and state in ['ACTIVE', 'SEEK', 'MOUNT_WAIT','DISMOUNT_WAIT']:
                                if (state == 'ACTIVE' and operation == 'ASSERT'):
                                    add_to_list = 0
                                else:
                                    if not mover in self.alarm_sent:
                                        # send alarm only once
                                        Trace.alarm(e_errors.ALARM,
                                                    "The mover %s is in state %s for %s minutes, Please check the mover"%
                                                    (mover, state, int(time_in_state)/60))
                                        self.alarm_sent.append(mover)
                            else:
                                if mover in self.alarm_sent:
                                    self.alarm_sent.remove(mover)

                            if add_to_list:
                                self.dont_update[mover] = state
                                movers_to_delete.append(mover)
                                Trace.alarm(e_errors.ALARM,
                                            "The mover %s is in state %s for %s minutes, will remove it from at_movers list"%
                                            (mover, state, int(time_in_state)/60))
                if movers_to_delete:
                    for mover in movers_to_delete:
                        self.delete(self.at_movers[mover])
            except:
                pass
            return movers_to_delete

    def busy_volumes (self, volume_family_name):
        """
        Return a list of busy volumes for a given volume family.

        :type volume_family_name: :obj:`str`
        :arg volume_family_name: string formatted as STORAGE_GROUP.FILE_FAMILY.FILE_FAMILY_WRAPPER
        :rtype: :obj:`tuple` (:obj:`list` - active volumes, :obj:`int` - volumes enabled to write)
        """
        Trace.trace(self.trace_level+3,"busy_volumes: family=%s"%(volume_family_name,))
        vols = []
        write_enabled = 0
        if not  self.sg_vf.vf.has_key(volume_family_name):
            return vols, write_enabled
        # look in the list of work_at_movers
        Trace.trace(self.trace_level+3,"busy_volumes: sg_vf %s" % (self.sg_vf,))
        for rec in self.sg_vf.vf[volume_family_name]:
            # self.sg_vf.vf[volume_family_name] is a tuple: (volume, mover)
            vols.append(rec[1])
            if self.at_movers.has_key(rec[0]):
                Trace.trace(self.trace_level+3,"busy_volumes: vol info %s" % (self.at_movers[rec[0]],))
                if self.at_movers[rec[0]]['volume_status'][0][0] in (e_errors.NOACCESS, e_errors.NOTALLOWED):
                    continue
                if self.at_movers[rec[0]]['volume_status'][0][1] == 'none':
                    # system inhibit
                    # if volume can be potentially written increase number
                    # of write enabled volumes that are currently at work
                    # further comparison of this number with file family width
                    # tells if write work can be given out
                    write_enabled = write_enabled + 1
                elif self.at_movers[rec[0]]['state'] == 'ERROR':
                    if not (enstore_functions2.is_readonly_state(self.at_movers[rec[0]]['volume_status'][0][1])):
                        write_enabled = write_enabled + 1
        Trace.trace(self.trace_level+3,"busy_volumes: returning %s %s" % (vols, write_enabled))
        return vols, write_enabled

    def active_volumes_in_storage_group(self, storage_group):
        """
        Return active volumes for a given storage group for
        a fair share distribution

        :type storage_group: :obj:`str`
        :arg storage_group: storage group
        :rtype: :obj:`list` - list of active volumes
        """

        if self.sg_vf.sg.has_key(storage_group):
            sg = self.sg_vf.sg[storage_group]
        else: sg = []
        return sg

    def get_active_movers(self):
        """
        Return active movers.

        :rtype: :obj:`list` - list of active movers
        """

        mv_list = []
        for key in self.at_movers.keys():
            mv_list.append(self.at_movers[key])
        return mv_list

    # check if a particular volume with given label is busy
    # for read requests
    def is_vol_busy(self, external_label, mover=None):
        """
        Check if a particular volume with given label is busy
        for read requests. If external_label, mover combination is found in
        the list of active movers volume is considered not busy.

        :type external_label: :obj:`str`
        :arg external_label: volume label
        :type mover: :obj:`str`
        :arg mover: volume label

        :rtype: :obj:`int` - 0 - not busy

        """

        rc = 0
        # see if this volume is in voulemes_at movers list
        for key in self.at_movers.keys():
            if ((external_label == self.at_movers[key]['external_label']) and
                (key != mover)):

                Trace.trace(self.trace_level+4, "volume %s is active. Mover=%s"%\
                          (external_label, key))
                rc = 1
                break
        return rc

    # return state of volume at mover
    def get_vol_state(self, external_label, mover=None):
        """
        Return state of volume at mover.
        If external_label, mover combination is found in
        the list of active movers return its state.

        :type external_label: :obj:`str`
        :arg external_label: volume label
        :type mover: :obj:`str`
        :arg mover: volume label

        :rtype: :obj:`str` - mover state (See :class:`AtMovers`)

        """

        rc = None
        # see if this volume is in voulemes_at movers list
        for key in self.at_movers.keys():
            if ((external_label == self.at_movers[key]['external_label']) and
                (key != mover)):

                Trace.trace(self.trace_level+4, "volume state %s. Mover=%s"%\
                          (self.at_movers[key]["state"], key))
                rc = self.at_movers[key]["state"]
                break
        return rc

    # return the list of busy volumes
    def active_volumes(self):
        """
        Return the list of busy volumes.

        :rtype: :obj:`list` - list of volumes
        """
        volumes = []
         # see if this volume is in voulemes_at movers list
        for key in self.at_movers.keys():
            volumes.append(self.at_movers[key]['external_label'])
        return volumes


##############################################################

class PostponedRequests:
    """
    Requests that have been refused because of limit reached go into this "list".
    Initally requests were already sorted by prioroty, so that only one request for a given
    volume or volume family may be in this list.
    """
    def __init__(self, keep_time):
        """
        :type keep_time: :obj:`int`
        :arg keep_time: maximum time interval to keep request in seconds
        """
        self.rq_list = {} # request list (dictionary)
        self.sg_list = {} # storage group list (dictionary)
        self.keep_time = keep_time # time for keeping requsts in the list
        self.start_time = time.time()
        self.trace_level = 330

    def __repr__(self):
        return 'rq_list:%s sg_list:%s'%(self.rq_list, self.sg_list,)

    def init_rq_list(self):
        self.rq_list = {}

    # check if Postponed request list has expired
    def list_expired(self):
        """
        Check if Postponed request list has expired

        :rtype: :obj:`bool` True - if expired
        """
        return (time.time() - self.start_time > self.keep_time)

    def put(self, rq):
        """
        Put request into list.

        :type rq: :obj:`manage_queue.Request`
        :arg rq: request
        """

        replace = 0
        sg = volume_family.extract_storage_group(rq.ticket['vc']['volume_family'])
        if self.rq_list.has_key(sg):
            if rq.adminpri > -1:
                if rq.adminpri > self.rq_list[sg].adminpri:
                    replace = 1
            else:
                if rq.basepri > self.rq_list[sg].basepri:
                    replace = 1
        else:
            replace = 1
        if replace:
            Trace.trace(self.trace_level,"postponed_put %s" % (rq,))
            # request with highest priority to this
            # storage group
            # only one request per storage group!
            self.rq_list[sg] = rq
        if not self.sg_list.has_key(sg):
            # the less is the count for a given sg
            # the less times the request
            # for this sg was selected
            # from postponed requests list.
            self.sg_list[sg] = 0L # to be used to sort list

    def get(self):
        """
        Get postponed request.

        :rtype: :obj:`manage_queue.Request`
        """

        if self.rq_list:
            # find the least counter
            # the more is the counter the more times
            # request was selected from prostponed requests
            # list
            l = []
            remove_these = []
            for sg in self.sg_list.keys():
                if self.rq_list.has_key(sg):
                    l.append((self.sg_list[sg], sg))
                else:
                   remove_these.append(sg)
            if len(l) > 1: l.sort()
            Trace.trace(self.trace_level, "sorted sg_list %s"%(l,))

            for sg in remove_these:
                if self.sg_list.has_key(sg):
                    del(self.sg_list[sg])
            # get sg for the least counter
            sg = l[0][1]
            return self.rq_list[sg], sg
        return None, None

    def update(self, sg, deficiency=0):
        if self.rq_list.has_key(sg): del(self.rq_list[sg])
        if self.sg_list.has_key(sg):
            self.sg_list[sg] = self.sg_list[sg]+deficiency
            if self.sg_list[sg] < 0:
                self.sg_list[sg] = 0L
            if deficiency >= 0:
                # the more is the counter the more times
                # request for a given storage group was picked
                # up from postponed requests
                self.sg_list[sg] = self.sg_list[sg]+1
            Trace.trace(self.trace_level, "postponed update %s %s %s"%(sg, deficiency, self.sg_list[sg]))

class LibraryManagerMethods:
    """
    Library manager request processing methods.
    """

    def init_suspect_volumes(self):
        # make it method for the capability to reinitialze
        # suspect_volumes outside of this class
        self.suspect_volumes = lm_list.LMList()

    def init_postponed_requests(self, keep_time):
        self.postponed_requests_time = keep_time
        ## place to keep all requests that have been postponed due to
        ## storage group limit reached
        self.postponed_requests = PostponedRequests(self.postponed_requests_time)

    def mover_type(self, ticket):
        return ticket.get("mover_type", "Mover")

    def __init__(self, name, csc, sg_limits, min_file_size, max_suspect_movers, max_suspect_volumes):
        """

        :type name: :obj:`str`
        :arg name: library manager log name
        :type csc: :class:`configuration_client.ConfigurationClient`
        :arg csc: configuration client instance
        :type sg_limits: :obj:`dict`
        :arg sg_limits: dictionary defining per storage group limits for fair share
        :type min_file_size: :obj:`int`
        :arg min_file_size: minimum size for selection of the volume for write operation
        :type max_suspect_movers: :obj:`int`
        :arg max_suspect_movers: maximum number of suspect mover to cause alert and primary page
        :type max_suspect_volumes: :obj:`int`
        :arg max_suspect_volumes: maximum number of suspect volumes to cause alarm
       """
        self.name = name
        self.min_file_size = min_file_size
        self.max_suspect_movers = max_suspect_movers
        self.max_suspect_volumes = max_suspect_volumes
        # instantiate volume clerk client
        self.csc = csc
        self.vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        self.vc_address = None
        self.check_interval = 30
        # List of known volumes cached internally
        # This list gets creaated every time
        # when mover request arrives.
        # See LibraryManager._mover_idle()
        # and LibraryManager._mover_bound_volume()
        self.known_volumes ={}

        # storage group limits for fair share
        self.sg_limits = {'use_default' : 1,
                          'default' : 0,
                          'limits' : {}
                          }
        if sg_limits:
            self.sg_limits['use_default'] = 0
            self.sg_limits['limits'] = sg_limits
        self.work_at_movers = lm_list.LMList()
        self.volumes_at_movers = AtMovers() # to keep information about what volumes are mounted at which movers
        self.init_suspect_volumes()
        self.pending_work = manage_queue.Request_Queue() # all incoming copy requests are stored in this queue
        self.idle_movers = [] # list of known idle movers
        self.trace_level = 200



    ########################################
    # Built in networking methods
    ########################################
    def del_udp_client(self, udp_client):
        __pychecker__ = "unusednames=server"
        if not udp_client: return
        # tell server we're done - this allows it to delete our unique id in
        # its dictionary - this keeps things cleaner & stops memory from growing
        try:
            pid = udp_client._os.getpid()
            tsd = udp_client.tsd.get(pid)
            if not tsd:
                return
            for server in tsd.send_done.keys():
                try:
                    tsd.socket.close()
                except:
                    pass
        except:
            pass

    def __send_regret(self, ticket):
        # Body of send_regret to run either in thread or as a function call.

        rc = 0
        try:
            Trace.trace(self.trace_level+10,"send_regret %s" % (ticket,))
            control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            flags = fcntl.fcntl(control_socket.fileno(), fcntl.F_GETFL)
            fcntl.fcntl(control_socket.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)
            # the following insertion is for antispoofing
            host = ticket['wrapper']['machine'][1]
            if ticket.has_key('route_selection') and ticket['route_selection']:
                ticket['mover_ip'] = host
                # bind control socket to data ip
                control_socket.bind((host, 0))
                u = udp_client.UDPClient()
                Trace.trace(self.trace_level+10, "sending IP %s to %s" % (host, ticket['routing_callback_addr']))
                try:
                    x= u.send(ticket,ticket['routing_callback_addr'] , 15, 3, 0)
                except (socket.error, select.error, e_errors.EnstoreError), msg:
                    Trace.log(e_errors.ERROR, "error sending to %s (%s)" %
                              (ticket['routing_callback_addr'], str(msg)))
                    self.del_udp_client(u)
                    return 1
                except errno.errorcode[errno.ETIMEDOUT]:
                    Trace.log(e_errors.ERROR, "error sending to %s (%s)" %
                              (ticket['routing_callback_addr'], os.strerror(errno.ETIMEDOUT)))
                    self.del_udp_client(u)
                    return 1
                if x.has_key('callback_addr'): ticket['callback_addr'] = x['callback_addr']
                Trace.trace(self.trace_level+10, "encp replied with %s"%(x,))
                self.del_udp_client(u)
            Trace.trace(self.trace_level+10, "connecting to %s" % (ticket['callback_addr'],))
	    try:
		control_socket.connect(ticket['callback_addr'])
	    except socket.error, detail:
		Trace.log(e_errors.ERROR, "%s %s" %
			  (detail, ticket['callback_addr']))
		#We have seen that on IRIX, when the connection succeds, we
		# get an ISCONN error.
		if hasattr(errno, 'EISCONN') and detail[0] == errno.EISCONN:
		    pass
		#The TCP handshake is in progress.
		elif detail[0] == errno.EINPROGRESS:
		    pass
		else:
		    Trace.log(e_errors.ERROR, "error connecting to %s (%s)" %
			      (ticket['callback_addr'], os.strerror(detail)))

            callback.write_tcp_obj(control_socket,ticket)
            control_socket.close()

        except:
            exc,msg,tb=sys.exc_info()
            Trace.log(1,"send_regret %s %s %s"%(exc,msg,ticket))
            rc = 1
        return rc

    # send a regret
    def send_regret(self, ticket):
        """
        Send regret to caller. Exit if running in a child process.

        :type ticket: :obj:`dict`
        :arg ticket: ticket to return to the caller
        """

        Trace.trace(self.trace_level, "send_regret")
        if self.do_fork:
            # fork off the regret sender
            pid=self.fork()
            if pid != 0:
                # parent
                Trace.trace(self.trace_level+10, "forking send_regret %s"%(pid,))
                return
            # child
            rc = self.__send_regret(ticket)
            os._exit(rc)

        else:
            dispatching_worker.run_in_thread('Send_Regret', self.__send_regret, args=(ticket,))

    ###################################
    # End built in networking methods
    ###################################

    # add idle mover
    def add_idle_mover(self, mover):
        """
        Add idle mover to the list of known idle movers.

        :type mover: :obj:`str`
        :arg mover: mover name
        """

        if not mover in self.idle_movers:
            self.idle_movers.append(mover)

    def remove_idle_mover(self, mover):
        """
        Remove idle mover from the list of known idle movers.

        :type mover: :obj:`str`
        :arg mover: mover name
        """
        if mover in self.idle_movers:
            self.idle_movers.remove(mover)

    # get storage group limit
    def get_sg_limit(self, storage_group):
        """
        Get storage group limit.

        :type storage_group: :obj:`str`
        :arg storage_group: storage group name
        """

        if self.sg_limits['use_default']:
            return self.sg_limits['default']
        else:
            return self.sg_limits['limits'].get(storage_group, self.sg_limits['default'])

    # obtain host name from ticket
    def get_host_name_from_ticket(self, ticket):
        """
        Obtain host name from ticket

        :type ticket: :obj:`dict`
        :arg ticket: ticket
        """

        host_from_ticket = ''
        try:
            callback = ticket.get('callback_addr', None)
            if callback:
                # preferred way
                host_from_ticket = hostaddr.address_to_name(callback[0])
            else:
                host_from_ticket = ticket['wrapper']['machine'][1]
        except:
            pass
        return host_from_ticket

    def flush_pending_jobs(self, status, external_label=None):
        """
        Remove all pending works.

        :type status: :obj:`str`
        :arg status: status to return to caller(s)
        :type external_label: :obj:`str`
        :arg external_label: remove works for specified volume
        """

        Trace.trace(self.trace_level,"flush_pending_jobs: %s"%(external_label,))
        if not external_label: return
        w = self.pending_work.get(external_label)
        while w:
            w.ticket['status'] = (status, None)
            Trace.log(e_errors.INFO,"flush_pending_jobs:work %s"%(w.ticket,))
            self.send_regret(w.ticket)
            self.pending_work.delete(w)
            w = self.pending_work.get(external_label)
        # this is just for test

    def get_work_at_movers(self, external_label, mover):
        """
        Return ticket if given labeled volume is in mover queue.
        Only one work can be in the work_at_movers for
        a given volume. That's why external label is used
        to identify the work.

        :type external_label: :obj:`str`
        :arg external_label: volume
        :type mover: :obj:`str`
        :arg mover: mover name
        :rtype: :obj:`dict` ticket
        """

        Trace.trace(self.trace_level,'get_work_at_movers: %s %s'%(external_label, mover))
        rc = {}
        if not external_label: return rc
        if not mover: return rc
        work_at_movers = []
        for w in self.work_at_movers.list:
            work_at_movers.append((w["fc"]["external_label"], w.get('mover',None), w['unique_id']))
            #Trace.trace(self.trace_level+1,'get_work_at_movers. ticket info: %s %s'%(w["fc"]["external_label"],  w.get('mover',None)))
            if w["fc"]["external_label"] == external_label:
                if w.has_key('mover') and w['mover'] == mover:
                    rc = w
                    break
        # we need to log this information for investigation of queue processing problems
        Trace.log(DEBUG_LOG, "work at movers list:%s"%(work_at_movers,))
        return rc

    def get_work_at_movers_m(self, mover):
        """
        Alternative to get_work_at_movers to use when there is no external label
        (usually for write requests, because the volume has not yet been assigned).

        :type mover: :obj:`str`
        :arg mover: mover name
        :rtype: :obj:`dict` ticket
        """
        rc = {}
        if not mover: return rc
        for w in self.work_at_movers.list:
            if w.has_key('mover') and w['mover'] == mover:
                rc = w
                break
        return rc

    # This method must run in a separate thread
    def check(self):
        """
        Periodically check volumes at movers (see :class:`AtMovers.check`). This method must run in a separate thread.
        """
        while 1:
           time.sleep(self.check_interval)
           movers = self.volumes_at_movers.check()
           if movers:
               works_to_delete = []
               for mv in movers:
                   w = self.get_work_at_movers_m(mv)
                   if w:
                      works_to_delete.append(w)
               for w in works_to_delete:
                   self.work_at_movers.remove(w)
           if not self.volumes_at_movers.at_movers:
               if len(self.work_at_movers.list) > 0:
                   # sometimes the self.volumes_at_movers is empty
                   # yet work_at_movers is not empty
                   # I could not identify the reason
                   # but here is a fix to deal with this
                   Trace.log(e_errors.ERROR, "volumes_at_movers list is epmty, yet work_at_movers is not empty %s"%
                             (self.work_at_movers.list,))
                   Trace.log(e_errors.ERROR, "Will clear work_at_movers")
                   self.work_at_movers.list = []

    def is_file_available(self, fcc, requested_file_bfid):
        """
        Check if file is available.
        This method applies only to disk movers.
        It checks whether file is avalable on a disk of the disk mover

        :type fcc: :class:`file_clerk_client.FileClient`
        :arg fcc: file clerk client
        :type requested_file_bfid: :obj:`str`
        :arg requested_file_bfid: bit file id of file in question
        :rtype: :obj:`bool`
        """

        Trace.trace(self.trace_level+1, 'is_file_available: requested_file_bfid %s'%(requested_file_bfid,))
        if not requested_file_bfid:
            return False
        ticket = fcc.bfid_info(requested_file_bfid)
        Trace.trace(self.trace_level+1, 'bfid info %s'%(ticket,))
        if ticket['status'][0] ==  e_errors.OK:
            # This was done as a feasibility study
            # for disk movers as enstore cache.
            # Keep this for a while.
            # If we decide to not used disk movers
            # as enstore cache - remove
            if ticket['fc'].has_key('purge') and ticket['fc']['purge'] == 'done':
                # file has been purged
                # purge
                return False
            else:
                return True


    ########################################
    # volume related helper methods
    ########################################
    # get list and count of busy volumes
    # for a specified volume family
    # to be used for slection of volume for
    # write request
    def busy_volumes(self, volume_family_name):
        """
        Get list and count of busy volumes
        for a specified volume family
        to be used for selection of volume for
        write request.

        :type volume_family_name: :obj:`str`
        :arg volume_family_name: volume family name
        :rtype: :obj:`tuple` (:obj:`list` - busy volumes, :obj:`int` - count of write enabled volumes)
        """

        vol_veto_list, wr_en = self.volumes_at_movers.busy_volumes(volume_family_name)
        # look in the list of work_at_movers
        for w in self.work_at_movers.list:
            Trace.trace(self.trace_level+1, 'busy_volumes: %s %s'%(w["vc"], w["fc"]))
            if w["vc"]["volume_family"] == volume_family_name:
                if w["fc"]["external_label"] in vol_veto_list:
                    continue       # already processed
                else:
                    vol_veto_list.append(w["fc"]["external_label"])
                    permissions = w["vc"].get("system_inhibit", None)
                    Trace.trace(self.trace_level+1, 'busy_volumes: permissions %s'%(permissions,))

                    if permissions:
                        if permissions[0] in (e_errors.NOACCESS, e_errors.NOTALLOWED):
                            continue
                        if permissions[1] == 'none':
                            wr_en = wr_en + 1

        return vol_veto_list, wr_en

    # check if a particular volume with given label is busy
    # for read requests
    def is_vol_busy(self, external_label, mover=None):
        """
        Check if a particular volume with given label is busy
        for read requests.

        :type external_label: :obj:`str`
        :arg external_label: volume name
        :type mover: :obj:`str`
        :arg mover: mover name. If :obj:`None` - check the whole list of active movers.
        :rtype: :obj:`int` 1 - success, 0 - failure
        """

        rc = self.volumes_at_movers.is_vol_busy(external_label, mover)
        if rc: return rc
        rc = 0
        for w in self.work_at_movers.list:
            if w["fc"]["external_label"] == external_label:
                rc = 1
                break
        return rc

    def is_disk_vol_available(self, work, external_label, requestor, requested_file_bfid=None):
        """
        Check the availability of the disk volume.
        This method applies only for disk movers.

        :type work: :obj:`str`
        :arg work: ``write_to_hsm`` or ``read_from_hsm``
        :type external_label: :obj:`str`
        :arg external_label: volume name
        :type requestor: :obj:`dict`
        :arg requestor: mover ticket
        :type requested_file_bfid: :obj:`str`
        :arg requested_file_bfid: bit file id of file in question
        :rtype: :obj:`dict` {'status': :obj:`tuple` (:obj:`str` - status, :obj:`None`)}
        """

        if work == 'write_to_hsm':
            return {'status':(e_errors.OK, None)}
        ip_map = string.split(external_label,':')[0]
        Trace.trace(self.trace_level+1, "label %s address #%s# requestor address #%s#"%(external_label, ip_map,
                                                                    requestor['ip_map']))
        # this is for disk movers
        if ip_map == requestor['ip_map']:
            # file is on disk of the mover where it was originally written
            rc = {'status':(e_errors.OK, None)}
        else:
            # check if file is on disk of the mover where it was originally written
            # and if yes return e_errors.MEDIA_IN_ANOTHER_DEVICE
            fcc = file_clerk_client.FileClient(self.csc)
            if self.is_file_available(fcc, requested_file_bfid):
                # skip this mover
                # the request will go to the mover where the file currently is
                rc = {'status': (e_errors.MEDIA_IN_ANOTHER_DEVICE, None)}
            else:
                # allow to stage this file from tape if possible.
                ret = fcc.find_copies(requested_file_bfid)
                if ret['status'][0] == e_errors.OK and ret['copies']:
                    rc = {'status':(e_errors.OK, None)}
                else:
                    # actually this file was lost
                    rc = {'status': (e_errors.NO_FILE, None)}
        Trace.trace(self.trace_level+1, "is_disk_vol_available rc %s"%(rc,))
        return rc

    # set volume clerk client
    def set_vcc(self, vol_server_address=None):
        """
        Set volume clerk client.

        :type vol_server_address: :obj:`tuple`
        :arg vol_server_address: (:obj:`str`- IP address, :obj:`int` - port)
        """

        if vol_server_address == None:
            return
        else:
            Trace.trace(self.trace_level+1, 'set_vcc %s %s'%(vol_server_address, vol_server_address))
            if self.vc_address != None:
                if self.vc_address == vol_server_address:
                    return
            self.vc_address = vol_server_address
            self.vcc = volume_clerk_client.VolumeClerkClient(self.csc,server_address=self.vc_address)
            Trace.trace(self.trace_level+1,"set_vcc returned")

    ###################################################################################
    # To reduce the number of VC requests (which may take a substantial amount of time)
    # use this internal metods
    ###################################################################################

    def is_volume_full_no_rec(self, v, min_remaining_bytes):
        """
        Check if volume is full.
        Same as is_volume_full in volume clerk, but makes no changes in the dictionary or data base.
        Used internally in Library Manager.

        :type v: :obj:`dict`
        :arg v: volume record
        :type min_remaining_bytes: :obj:`int`
        :arg min_remaining_bytes: minimum number of remaining bytes. (Pad to define if volume would be full).
        :rtype: :obj:`str` - e_errors.NOSPACE or ""
        """

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

            ret = e_errors.NOSPACE
        return ret

    def is_vol_available(self, work, label, family=None, size=0, vol_server_address = None):
        """
        Copy of volume clerk method adapted for working with records.

        :type work: :obj:`str`
        :arg work:  ``write_to_hsm`` or ``read_from_hsm``
        :type label: :obj:`str`
        :arg label:  volume name
        :type family: :obj:`str`
        :arg family:  volume family
        :type size: :obj:`int`
        :arg size: size for write requests
        :type vol_server_address: :obj:`tuple`
        :arg vol_server_address: (:obj:`str`- IP address, :obj:`int` - port)
        :rtype: :obj:`dict` {'status': :obj:`tuple` (:obj:`str` - status, :obj:`None`)}
        """

        Trace.trace(self.trace_level+2, 'is_vol_available %s'%(self.known_volumes,))
        # get the current entry for the volume
        if self.known_volumes.has_key(label):
            record = self.known_volumes[label]
            Trace.trace(self.trace_level+2, "is_vol_available system_inhibit = %s user_inhibit = %s" %
                        (record['system_inhibit'],
                         record['user_inhibit']))
            if record["system_inhibit"][0] == e_errors.DELETED:
                ret_stat = (record["system_inhibit"][0],None)
            else:
                if work == 'read_from_hsm':
                    Trace.trace(self.trace_level+2, "is_vol_available: reading")
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
                    Trace.trace(self.trace_level+2, "is_vol_available: writing")
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
                        ff = volume_family.extract_file_family(family)
                        Trace.trace(self.trace_level+2, "is_vol_available: family %s, record %s" %
                                    (family,record['volume_family']))

                        #XXX deal with 2-tuple vs 3-tuple...
                        if (volume_family.match_volume_families(family,record['volume_family']) or
                            ff == 'ephemeral'):
                            ret = self.is_volume_full_no_rec(record,size)
                            Trace.trace(self.trace_level+2, "is_vol_available: ret1 %s"%(ret,))
                            if not ret:
                                ret_stat = (e_errors.OK,None)
                            else:
                                ret_stat = (ret, None)
                        else: ret_stat = (e_errors.NOACCESS,None)
                else:
                    ret_stat = (e_errors.UNKNOWN,None)
                Trace.trace(self.trace_level+2, "is_vol_available: ret2 %s"%(ret_stat,))
                rticket = {'status':ret_stat}
            return rticket
        else:
            self.set_vcc(vol_server_address)
            Trace.trace(self.trace_level+2, 'is_vol_available work %s label %s family %s size %s'%(work, label, family, size))
            rticket = self.vcc.is_vol_available(work, label, family, size, timeout=self.volume_clerk_to, retry=self.volume_clerk_retry)
        Trace.trace(self.trace_level+2, 'is_vol_available %s'%(rticket,))
        return rticket


    def inquire_vol(self, external_label, vol_server_address = None):
        """
        Get volume record.

        :type external_label: :obj:`str`
        :arg external_label:  volume name
        :type vol_server_address: :obj:`tuple`
        :arg vol_server_address: (:obj:`str`- IP address, :obj:`int` - port)
        :rtype: :obj:`dict` - volume record containing status
        """

        Trace.trace(self.trace_level+2, 'inquire_vol')
        if self.known_volumes.has_key(external_label):
            vol_info = self.known_volumes[external_label]
        else:
            self.set_vcc(vol_server_address)
            vol_info = self.vcc.inquire_vol(external_label, timeout=INQUIRE_VOL_TO, retry=INQUIRE_VOL_RETRY)
            Trace.trace(self.trace_level+2, 'inquire_vol %s'%(vol_info,))
            if vol_info['status'][0] == e_errors.TIMEDOUT:
                Trace.alarm(e_errors.INFO, "volume clerk problem inquire_volume %s TIMEDOUT"%(external_label,))
            if not self.known_volumes.has_key(external_label):
                self.known_volumes[external_label] = vol_info
        Trace.trace(self.trace_level+2, 'inquire_vol %s'%(self.known_volumes,))
        Trace.trace(self.trace_level+2, 'inquire_vol returns %s'%(vol_info,))

        return vol_info


    def next_write_volume(self,library, size, volume_family, veto_list, first_found=0, mover={}):
        """
        Get next write volume.

        :type library: :obj:`str`
        :arg library: get volume in this library
        :type size: :obj:`long`
        :arg size: get volume, wich has not less than size remaning bytes
        :type volume_family: :obj:`str`
        :arg volume_family: get volume for this volume family
        :type veto_list: :obj:`list`
        :arg veto_list: list of volume names to skip
        :type first_found: :obj:`int`
        :arg first_found: if > 0 - return first found volume satisfying specified criteria
        :type mover: :obj:`dict`
        :arg mover: mover ticket
        :rtype: :obj:`dict` - volume record containing status
        """

        Trace.trace(self.trace_level+2, 'write_volumes %s'%(self.write_volumes,))
        required_bytes = max(long(size*SAFETY_FACTOR), MIN_LEFT)
        for vol_rec in self.write_volumes:
            if ((library == vol_rec['library']) and
                (required_bytes < vol_rec['remaining_bytes']) and
                (volume_family == vol_rec['volume_family']) and
                (not (vol_rec['external_label'] in veto_list))):
                return vol_rec
        else:
            start_t=time.time()
            v = self.vcc.next_write_volume(library, size, volume_family, veto_list, first_found, mover,
                                           timeout=self.volume_clerk_to, retry=self.volume_clerk_retry)
            Trace.trace(self.trace_level+2, "vcc.next_write_volume, time in state %s"%(time.time()-start_t, ))
            if v['status'][0] == e_errors.TIMEDOUT:
                Trace.alarm(e_errors.INFO, "volume clerk problem next_write_volume: TIMEDOUT")
            if v['status'][0] == e_errors.OK and v['external_label']:
                self.write_volumes.append(v)

            return v

    ########################################
    #  end volume related helper methods
    ########################################

    ############################################
    # discipline related methods
    ############################################

    def restrict_host_access(self, host, max_permitted, rq_host=None, work=None):
        """
        New implementation - no storage group
        The discipline configuration entry last argument is a tuple
        which is:

           1 - the number of allowed concurrent transfers from a given host

           2 - Additional number of allowed concurrent transfers from a given host for read requests

           3 - Additional number of allowed concurrent transfers from a given host for write requests

        :type host: :obj:`str`
        :arg host: name of the host to check
        :type max_permitted: :obj:`tuple`
        :arg max_permitted: as described above for the  discipline configuration entry
        :type rq_host: :obj:`str`
        :arg rq_host: host requesting the work
        :type work: :obj:`str`
        :arg work: ``write_to_hsm`` or ``read_from_hsm``
        :rtype: :obj:`bool`
        """

        disciplineExceptionMounted = 0
        max_perm=max_permitted
        if type(max_permitted) == type(()) and len(max_permitted) == 3:
            # the max_permitted is (maximal_permitted, add_for_reads_for_bound,add_for_writes_for_bound)
            max_perm=max_permitted[0]
            if work:
                # calculate the position in the tuple
                exception_Mounted_index = (work == "write_to_hsm")+1
                disciplineExceptionMounted=int(max_permitted[exception_Mounted_index])

        active = 0
        Trace.trace(self.trace_level+3, "restrict_host_access(%s,%s %s)"%
                    (host, max_permitted, rq_host))
        for w in self.work_at_movers.list:
            host_from_ticket = self.get_host_name_from_ticket(w)
            Trace.trace(self.trace_level+3,'host_from_ticket %s'%(host_from_ticket,))
            try:
                if re.search(host, host_from_ticket):
                    if rq_host:
                        if  host_from_ticket == rq_host:
                            active = active + 1
                    else:
                        active = active + 1
            except KeyError,detail:
                Trace.log(e_errors.ERROR,"restrict_host_access:%s....%s"%(detail, w))
        Trace.trace(self.trace_level+3, "restrict_host_access(%s,%s)"%
                    (active, max_permitted))
        return active >= max_perm+disciplineExceptionMounted


    def restrict_version_access(self, storage_group, legal_version, ticket):
        """
        Restrict client access based on the version of the encp client.

        :type storage_group: :obj:`str`
        :arg storage_group: sturage group
        :type legal_version: :obj:`str`
        :arg legal_version: the oldest allowed client version
        :type ticket: :obj:`disct`
        :arg ticket: client request ticket
        :rtype: :obj:`bool`
        """

        rc = False
        Trace.trace(self.trace_level+3, "restrict_version_access %s %s %s"%(storage_group,
                                                            legal_version,
                                                            ticket))
        if storage_group == ticket['vc']['storage_group']:
            c_legal_version = enstore_functions2.convert_version(legal_version)
            if ticket.has_key('version'):
                version=ticket['version'].split()[0]
                c_version = enstore_functions2.convert_version(version)
            else:
                c_version = (0, "")
            if c_legal_version > c_version:
                rc = True # restrict access
                ticket['status'] = (e_errors.VERSION_MISMATCH,
                                    "encp version too old: %s. Must be not older than %s"%(version, legal_version,))
        return rc


    ## check if there are any additional restrictions
    ## from discipline
    def client_host_busy(self, w):
        """
        Check if there are any additional restrictions
        from discipline.

        :type w: :obj:`dict`
        :arg w: ticket
        :rtype: :obj:`bool`
        """

        ret = False
        rc, fun, args, action = self.restrictor.match_found(w)
        if rc and fun and action:
            w["status"] = (e_errors.OK, None)
            if fun == 'restrict_host_access':
                host_from_ticket = self.get_host_name_from_ticket(w)
                Trace.trace(self.trace_level+3,'client_host_busy: %s %s'%(host_from_ticket, w['wrapper']['machine'][1]))

                args.append(host_from_ticket)
                ret = apply(getattr(self,fun), args)
                Trace.trace(self.trace_level+3, "client_host_busy returning %s"%(ret,))

                if ret and (action in (e_errors.LOCKED, e_errors.IGNORE, e_errors.PAUSE, e_errors.REJECT)):
                    w["reject_reason"] = ("RESTRICTED_ACCESS",None)
                    if host_from_ticket not in self.disabled_hosts:
                        self.disabled_hosts.append(host_from_ticket)
                    Trace.trace(self.trace_level+3,"client_host_busy: RESTRICTED_ACCESS")

        return ret

    def client_host_busy_for_mounted(self, external_label, vol_family, w):
        """
        Check if there are any additional restrictions for mounted
        volumes from discipline.

        :type external_label: :obj:`str`
        :arg external_label: volume name
        :type vol_family: :obj:`str`
        :arg vol_family: volume family
        :type w: :obj:`dict`
        :arg w: ticket
        :rtype: :obj:`bool`
        """

        Trace.trace(self.trace_level+3,"client_host_busy_for_mounted: %s"%(self.restrict_access_in_bound))
        ret = False
        if not self.restrict_access_in_bound:
            return False

        rc, fun, args, action = self.restrictor.match_found(w)
        args_copy = copy.copy(args)
        Trace.trace(self.trace_level+3,"client_host_busy_for_mounted args %s" %(args,))
        if rc and fun and action:
            w["status"] = (e_errors.OK, None)
            if fun == 'restrict_host_access':
                host_from_ticket = self.get_host_name_from_ticket(w)
                # for admin priority requests check if current volume
                # would get dismounted
                # we may allow extra work only for the same volume
                adminpri = -1
                client = w.get('encp','')
                if client:
                    adminpri = client.get('adminpri',-1)
                Trace.trace(self.trace_level+3,"client_host_busy_2:adminpri %s"%(adminpri))

                if adminpri >= 0:
                    # check if request woould get rejected for idle mover
                    args_copy.append(host_from_ticket)
                    would_reject = apply(getattr(self,fun), args_copy)
                    if would_reject:
                        # see if this request can be satisfied for the currently
                        # mounted volume
                        if ((w['work'] == "read_from_hsm" and w["fc"]["external_label"] == external_label) or
                            (w['work'] == "write_to_hsm" and w["vc"]["volume_family"] == vol_family)):
                            pass
                        else:
                            # otherwise reject this request
                            return True
                mp=args[-1]
                Trace.trace(self.trace_level+3,"client_host_busy_for_mounted mp %s" %(mp,))
                if type(mp) == type(()) and len(mp) == 3:
                    mp1=(mp[0]+1, mp[1], mp[2]) # allow 1 more request for bount volume
                else:
                    mp1=mp+1 # allow 1 more request for bount volume
                Trace.trace(self.trace_level+3,"client_host_busy_for_mounted mp_1 %s" %(mp,))
                args[-1]=mp1

                args.append(host_from_ticket)
                Trace.trace(self.trace_level+3,"client_host_busy_for_mounted args_1 %s" %(args,))
                if ((w['work'] == "read_from_hsm" and w["fc"]["external_label"] == external_label) or
                    (w['work'] == "write_to_hsm" and w["vc"]["volume_family"] == vol_family)):
                    args.append(w['work'])
                ret = apply(getattr(self,fun), args)
                if ret and (action in (e_errors.LOCKED, e_errors.IGNORE, e_errors.PAUSE, e_errors.REJECT)):
                    w["reject_reason"] = ("RESTRICTED_ACCESS",None)
                    if host_from_ticket not in self.disabled_hosts:
                        self.disabled_hosts.append(host_from_ticket)
                    Trace.trace(self.trace_level+3, "client_host_busy_for_mounted: RESTRICTED_ACCESS")

        Trace.trace(self.trace_level+3,"client_host_busy_for_mounted returning %s" %(ret,))
        return ret

    ############################################
    # end discipline related methods
    ############################################

    ############################################
    # Request processing methods
    ############################################

    def is_packaged(self, request):
        """
        Check if request is for a packaged file.

        :type request: :obj:`dict`
        :arg request: request ticket
        :rtype: :obj:`str` - package id or :obj:`None`
        """

        Trace.trace(self.trace_level+3, "is_packaged fc: %s"%(request['fc'],))
        package_id = request['fc'].get("package_id", None)
        if package_id and package_id == request['fc']['bfid']: # file is a package itself
            package_id = None
        return package_id

    def _get_request(self, requestor, method, *args, **kwargs):
        """
        Wrapper method for manage_queue.Request_Queue.get.
        This method applies only for packaged disk files.
        It checks if request pulled from request queue
        has a package id identical with package id of
        any active request and, if yes, skips it to allow
        completion of staging the package.
        This allows to avoid submission of more than one
        requests belonging to the same package to movers
        until all files in the package are staged.

        :type requestor: :obj:`dict`
        :arg requestor: mover ticket
        :type method: :obj:`callable`
        :arg method: method for extracting request from the queue
        :type args: :obj:`tuple`
        :arg args: arguments for method
        :type kwargs: :obj:`tuple`
        :arg kwargs: kw arguments for method
        :rtype: :obj:`manage_queue.Request` - request ticket or :obj:`None`
        """

        mover_type = requestor.get('mover_type', None)
        if mover_type and mover_type == 'DiskMover':
            if kwargs.has_key('active_volumes'):
                del(kwargs['active_volumes']) # multiple disk movers can access the same active volume
        request = method(*args, **kwargs)
        Trace.trace(self.trace_level+3, "_get_request: method %s args %s kwargs %s request %s"%(method.__name__, args, kwargs, request,))
        if not mover_type or mover_type != 'DiskMover':
            # return request right away
            # only DiskMover requests need further processing inside of
            # _get_request
            return request
        if request and request.ticket['work'] != "read_from_hsm":
            # only read_from_hsm requests need further processing
            return request
        while request:
            rq_id = request.unique_id
            Trace.trace(self.trace_level+3, "_get_request: %s"%(request,))
            # check if file is part of a package
            package_id = self.is_packaged(request.ticket)
            Trace.trace(self.trace_level+3, "_get_request: package_id %s"%(package_id,))
            if package_id:
                # Find the any file in the work at movers.
                # If it is found this means that at least the
                # package is being staged
                for w in self.work_at_movers.list:
                    # Check if this is a disk file:
                    Trace.trace(self.trace_level+3, "_get_request: w %s"%(w,))
                    if w['mover_type'] != 'DiskMover':
                        # not a disk file: continue search
                        continue
                    wam_package_id = w['fc'].get("package_id", None)
                    if wam_package_id == package_id:
                        # The package is being processed by mover.
                        # Check the cache status
                        Trace.trace(self.trace_level+3, "_get_request: found package in wam")
                        if (request.ticket['fc']['cache_status']  == file_cache_status.CacheStatus.CACHED or
                            w['fc']['cache_status'] == file_cache_status.CacheStatus.CACHED):
                            # the request can be sent to the mover
                            # return here
                            Trace.trace(self.trace_level+3, "_get_request: returning %s"%(request,))
                            return request
                        else:
                            # get next request
                            kwargs['next'] = 1
                            request = method(*args, **kwargs)
                            Trace.trace(self.trace_level+3, "_get_request: next_rq %s"%(request,))
                            if request and request.unique_id == rq_id:
                                # if it is the same request
                                # break, we have no more requests to process
                                request = None
                            break # return to while loop to check request against active works

                else:
                    break
            else:
                break
        Trace.trace(self.trace_level+3, "_get_request: returning1 %s"%(request,))
        return request

    # allow HIPR request to be sent to the current mover
    # this method is used with Admin Priority requests
    # inputs
    # request to process
    # label of currently mounted volume
    # volume family of currently mounted volume
    # last work on currently mounted volume
    # requestor - mover information on which the volume is mounted
    # priority - priority of completed request
    # it is a tuple (current_priority, admin_priority)
    # returns rq (possibly modified)
    # flag confirming whether HIPRI request could go
    # flag indicating that the request will preempt the mounted volume
    def allow_hipri(self, rq, external_label, vol_family, last_work, requestor, priority):
        """
        Allow High Piority request to be sent to the current mover.
        This method is used with Admin Priority requests.

        :type rq: :obj:`manage_queue.Request`
        :arg rq: request to process
        :type external_label: :obj:`str`
        :arg external_label: label of volume mounted on requestor
        :type vol_family: :obj:`str`
        :arg vol_family: volume family of volume mounted on requestor
        :type last_work: :obj:`str`
        :arg last_work: last work performed on volume mounted on requestor
        :type requestor: :obj:`dict`
        :arg requestor: mover ticket
        :type priority: :obj:`tuple`
        :arg priority: (:obj:`int` - current_priority, :obj:`int` - admin_priority) - priority of last completed request
        :rtype: :obj:`tuple` (:obj:`manage_queue.Request` or :obj:`None` - request,
                             :obj:`bool` - flag confirming whether HIPRI request could go,
                             :obj:`bool` - flag indicating that the request will preempt the mounted volume)
        """

        Trace.trace(self.trace_level+3, "allow_hi_pri %s %s %s %s %s %s"%
                    (external_label, vol_family, last_work, requestor,priority, rq))
        if rq.adminpri < 0: # regular priority
            return rq, False, False
        ret = True

        if priority and priority[1] >= 0:
            Trace.trace(self.trace_level+3, "allow_hi_pri: returning1 %s %s"%(rq, ret))
            return rq, ret, False

        would_preempt = False
        if rq.work == "read_from_hsm" and rq.ticket["fc"]["external_label"] != external_label:
            would_preempt = True

        elif rq.work == "write_to_hsm":
            if rq.ticket["vc"]["volume_family"] != vol_family:
                would_preempt = True
            else:
                # same file family
                if last_work == "READ":
                    would_preempt = True
                    Trace.trace(self.trace_level+3, "allow_hi_pri: calling check_write_request")
                    nrq, status = self.check_write_request(external_label, rq, requestor)
                    Trace.trace(self.trace_level+3, "allow_hi_pri: check_write_request returned: %s %s"%
                                (nrq, status))
                    if nrq and status[0] == e_errors.OK:
                        if nrq.ticket["fc"]["external_label"] == external_label:
                            would_preempt = False
        if would_preempt:
            # check whether there are idle movers availabe
            if len(self.idle_movers) > 0:
                Trace.trace(self.trace_level+3, "There are idle movers. Will not preempt the current one %s"%
                            (self.idle_movers,))
                ret = False
        Trace.trace(self.trace_level+3, "allow_hi_pri: returning %s %s %s"%(rq, ret, would_preempt))
        return rq, ret, would_preempt

    def init_request_selection(self):
        """"
        Make all necessary resets
        before starting a new cycle of request selection
        """

        self.write_volumes = []
        self.write_vf_list = {}
        self.tmp_rq = None   # need this to temporarily store selected request
        # initialize postponed requests list
        if self.postponed_requests.list_expired():
            Trace.trace(self.trace_level, "postponed list expired")
            self.postponed_requests = PostponedRequests(self.postponed_requests_time)
        else: self.postponed_requests.init_rq_list()
        self.postponed_rq = 0
        self.pending_work.start_cycle()

        self.checked_keys = [] # list of checked tag keys
        self.continue_scan = 0
        self.process_for_bound_vol = None # if not None volume is bound
        self.disabled_hosts = [] # hosts exceeding the number of simult. transfers

    def request_key(self, request):
        """
        Returns a request key.
        For write request it is a file family.
        For read request it is a volume label

        :type request: :obj:`manage_queue.Request`
        :arg request: request to process
        :rtype: :obj:`str` - request key
        """

        storage_group = None
        key = None
        if request:
            work = request.ticket.get("work", None)
            if work:
               if work == "read_from_hsm":
                   storage_group = volume_family.extract_storage_group(request.ticket['vc']['volume_family'])
                   key = request.ticket["fc"]["external_label"]
               elif work == "write_to_hsm":
                   storage_group = request.ticket["vc"]["storage_group"]
                   key = request.ticket["vc"]["volume_family"]
        return storage_group, key


    def fair_share(self, rq):
        """
        If request satisfies fair share of tape drives for its storage group
        return the key, otherwise return None.

        :type rq: :obj:`manage_queue.Request`
        :arg rq: request to process
        :rtype: :obj:`str` - request key or :obj:`None`
        """

        self.sg_exceeded = None
        Trace.trace(self.trace_level+4, "fair_share: sg_exceeded %s"%(self.sg_exceeded,))
        if (rq.ticket.get('ignore_fair_share', None)):
            # do not count this request against fair share
            # this is an automigration request
            return None
        # fair share
        # see how many active volumes are in this storage group
        if self.process_for_bound_vol:
            # allow "ease" more volumes for bound volumes
            ease = 1
        else:
            ease = 0
        storage_group, check_key = self.request_key(rq)

        pw_sgs = self.pending_work.storage_groups.keys()
        if len(pw_sgs)==1 and pw_sgs[0] == storage_group:
            # what else?
            # All requests in the queue are for only one storage group.
            # No need to apply fair share
            return None

        if not check_key in self.checked_keys:
            self.checked_keys.append(check_key)
        active_volumes = self.volumes_at_movers.active_volumes_in_storage_group(storage_group)
        Trace.trace(self.trace_level+4, "fair_share: SG LIMIT %s"%(self.get_sg_limit(storage_group),))
        if len(active_volumes) >= self.get_sg_limit(storage_group)+ease:
            rq.ticket["reject_reason"] = ("PURSUING",None)
            self.sg_exceeded = (True, storage_group)
            Trace.trace(self.trace_level+4, "fair_share: active work limit exceeded for %s" % (storage_group,))
            if rq.adminpri > -1:
                self.continue_scan = 1
                return None
            # we have saturated system with requests from the same storage group
            # see if there are pending requests for different storage group
            start_t=time.time()
            tags = self.pending_work.get_tags()
            Trace.trace(self.trace_level+4,"fair_share:tags: %s"%(tags,))
            Trace.trace(100, "fair_share:TAGS TIME %s"%(time.time()-start_t, ))
            start_t=time.time()
            Trace.trace(self.trace_level+4, 'fair_share:postponed rqs %s'%(self.postponed_requests))
            if len(tags) > 1:
                for key in tags:
                    if self.pending_work.get_sg(key) in self.postponed_requests.sg_list.keys():
                        pass # request for this SG is already in postponed list, no nee to process
                    else:
                        if not key in self.checked_keys:
                            self.checked_keys.append(key)
                            if key != check_key:
                                Trace.trace(self.trace_level+4, "fair_share: key %s"%(key,))
                                Trace.trace(100, "fair_share:keys TIME %s"%(time.time()-start_t, ))
                                return key

        return None

    def process_read_request(self, request, requestor):
        """
        Process read request.

        :type request: :obj:`manage_queue.Request`
        :arg request: request to process
        :type requestor: :obj:`dict`
        :arg requestor: mover ticket
        :rtype: :obj:`tuple` (:obj:`manage_queue.Request` - request or :obj:`None`,
                              :obj:`str` - key to check next or :obj:`None`)
        """

        self.continue_scan = 0 # disable "scan" of pending queue
        rq = request
        Trace.trace(self.trace_level+4,"process_read_request %s"%(rq))

        key_to_check = self.fair_share(rq)
        if key_to_check:
            self.continue_scan = 1

        mover = requestor.get('mover', None)
        label = rq.ticket["fc"]["external_label"]

        if self.is_vol_busy(rq.ticket["fc"]["external_label"], mover) and self.mover_type(requestor) != 'DiskMover':
            rq.ticket["reject_reason"] = ("VOL_BUSY",rq.ticket["fc"]["external_label"])
            self.continue_scan = 1
            Trace.trace(self.trace_level+4,"process_read_request: VOL_BUSY %s"%(rq.ticket["fc"]["external_label"]))
            return rq, key_to_check
        # otherwise we have found a volume that has read work pending
        Trace.trace(self.trace_level+4,"process_read_request: found volume %s"%(rq.ticket,))
        # ok passed criteria.
        ## check if there are any discipline restrictions
        host_busy = False
        if self.process_for_bound_vol:
            host_busy = self.client_host_busy_for_mounted(self.process_for_bound_vol,
                                                          self.current_volume_info['volume_family'],
                                                          rq.ticket)

        else:
            host_busy = self.client_host_busy(rq.ticket)
        if host_busy:
            self.continue_scan = 1
            sg, key_to_check = self.request_key(rq)
            #return None, None
            return None, key_to_check

        # Check the presence of current_location field
        if not rq.ticket["vc"].has_key('current_location'):
            try:
                rq.ticket["vc"]['current_location'] = rq.ticket['fc']['location_cookie']
            except KeyError:
                Trace.log(e_errors.ERROR,"process_read_request loc cookie missing %s" %
                          (rq.ticket,))
                raise KeyError

        # request has passed about all the criterias
        # check if it passes the fair share criteria
        # temprorarily store selected request to use it in case
        # when other request(s) based on fair share criteria
        # for some other reason(s) do not get selected

        # in any case if request SG limit is 0 and temporarily stored rq. SG limit is not,
        # do not update temporarily store rq.


        # legacy encp ticket
        if not rq.ticket['vc'].has_key('volume_family'):
            rq.ticket['vc']['volume_family'] = rq.ticket['vc']['file_family']
        rq_sg = volume_family.extract_storage_group(rq.ticket['vc']['volume_family'])
        if (rq.ticket.get('ignore_fair_share', None)):
            # do not count this request against fair share
            # this is an automigration request
            sg_limit = 0
        else:
            sg_limit = self.get_sg_limit(rq_sg)
            self.postponed_requests.put(rq)
        Trace.trace(self.trace_level+4, 'process_read_request:postponed rqs %s'%(self.postponed_requests))
        if self.tmp_rq:
            #tmp_rq_sg = volume_family.extract_storage_group(self.tmp_rq.ticket['vc']['volume_family'])
            #tmp_sg_limit = self.get_sg_limit(tmp_rq_sg)
            if sg_limit != 0:     # replace tmp_rq if rq SG limit is not 0
                # replace tmp_rq based on priority
                if rq.pri > self.tmp_rq.pri:
                    self.tmp_rq = rq
        else: self.tmp_rq = rq
        Trace.trace(self.trace_level+4,'process_read_request:tmp_rq %s rq %s key %s'%(self.tmp_rq, rq, key_to_check))
        if self.process_for_bound_vol and (rq.ticket["fc"]["external_label"] == self.process_for_bound_vol):
            # do not continue scan if we have a bound volume.
            self.continue_scan = 0
        # is this mover, volume in suspect mover list?
        suspect_v,suspect_mv = self.is_mover_suspect(requestor['mover'], rq.ticket["vc"]["external_label"])
        if suspect_mv:
            Trace.log(e_errors.INFO,"mover %s is suspect for %s cannot assign a read work"%
                      (requestor['mover'], rq.ticket["fc"]["external_label"]))
            rq = None

        Trace.trace(self.trace_level+4, "process_read_request: returning %s %s"%(rq, key_to_check))
        return rq, key_to_check

    def process_write_request(self, request, requestor, last_work=None, would_preempt=False):
        """
        Process write request.

        :type request: :obj:`manage_queue.Request`
        :arg request: request to process
        :type requestor: :obj:`dict`
        :arg requestor: mover ticket
        :type last_work: :obj:`str`
        :arg last_work: last work completed by requestor
        :type would_preempt: :obj:`bool`
        :arg would_preempt: may this request preempt mounted on requestor volume?
        :rtype: :obj:`tuple` (:obj:`manage_queue.Request` - request or :obj:`None`,
                              :obj:`str` - key to check next or :obj:`None`)
        """

        self.continue_scan = 0 # disable "scan" of pending queue
        rq = request
        Trace.trace(self.trace_level+4, "process_write_request: %s"%(rq,))
        key_to_check = self.fair_share(rq) # check this volume label or FF
        Trace.trace(self.trace_level+4, "process_write_request: exceeded rqs %s"%(self.sg_exceeded,))
        Trace.trace(self.trace_level+4,"process_write_request: key %s process for bound %s"%(key_to_check, self.process_for_bound_vol))
        if key_to_check:
            self.continue_scan = 1
            if self.process_for_bound_vol and (key_to_check != self.process_for_bound_vol):
                Trace.trace(self.trace_level+4, "process_write_request: got here")
                #return rq, key_to_check
        vol_family = rq.ticket["vc"]["volume_family"]
        if self.mover_type(requestor) != 'DiskMover':
            if not self.write_vf_list.has_key(vol_family):
                vol_veto_list, wr_en = self.busy_volumes(vol_family)
                Trace.trace(self.trace_level+4,"process_write_request: vol veto list:%s, width:%d ff width %s"%\
                            (vol_veto_list, wr_en, rq.ticket["vc"]["file_family_width"]))
                self.write_vf_list[vol_family] = {'vol_veto_list':vol_veto_list, 'wr_en': wr_en}
            else:
                vol_veto_list =  self.write_vf_list[vol_family]['vol_veto_list']
                wr_en = self.write_vf_list[vol_family]['wr_en']

            # only so many volumes can be written to at one time
            permitted = rq.ticket["vc"]["file_family_width"]
            if self.process_for_bound_vol: # allow one more for bound to avoid dismounts
                # but check if this is a HiPri request and it will require dismount of currently
                # mounted volume
                permitted = permitted + (not would_preempt)
                if self.process_for_bound_vol in vol_veto_list:
                    # check if request can go to this volume
                    ret = self.is_vol_available(rq.work,
                                            self.process_for_bound_vol,
                                             rq.ticket['vc']['volume_family'],
                                             rq.ticket['wrapper'].get('size_bytes', 0L),
                                             rq.ticket['vc']['address'])
                    if ret['status'][0] == e_errors.OK:
                        # permit one more write request to avoid
                        # tape dismount
                        permitted = permitted+1

            Trace.trace(self.trace_level+4,
                        "process_write_request: self.process_for_bound_vol %s permitted %s"%
                        (self.process_for_bound_vol, permitted))


            if wr_en >= permitted:
                if self.process_for_bound_vol and self.process_for_bound_vol in vol_veto_list:
                    # check if there are volumes in bound state
                    # in veto list and if yes (they are not active),
                    # allow this request go to avoid dismount of the current volume
                    # do not check if volume bound for the current request does not
                    # belong to volume family of selected request
                    for vol in vol_veto_list:
                        if vol != self.process_for_bound_vol:
                            volume_state = self.volumes_at_movers.get_vol_state(vol)
                            if volume_state == "HAVE_BOUND" and last_work == "WRITE":
                                permitted = permitted + 1
                                break

            if wr_en >= permitted:
                rq.ticket["reject_reason"] = ("VOLS_IN_WORK","")
                if self.process_for_bound_vol:
                    self.continue_scan = 0 # do not continue scan for bound volume
                else:
                    self.continue_scan = 1
                #return rq, key_to_check
                return None, key_to_check
            else:
                ## check if there are any discipline restrictions
                host_busy = None
                if self.process_for_bound_vol:
                    #current_volume_info = self.current_volume_info
                    host_busy = self.client_host_busy_for_mounted(self.process_for_bound_vol,
                                                                  self.current_volume_info['volume_family'],
                                                                  request.ticket)
                else:
                    host_busy = self.client_host_busy(rq.ticket)
                if host_busy:
                    sg, key_to_check = self.request_key(rq)
                    self.continue_scan = 1
                    return None, key_to_check # continue with key_to_ckeck

                if not self.process_for_bound_vol and rq.ticket["vc"]["file_family_width"] > 1:
                    # check if there is a potentially available tape at bound movers
                    # and if yes skip request so that it will be picked by bound mover
                    # this is done to aviod a single stream bouncing between different tapes
                    # if FF width is more than 1
                    Trace.trace(self.trace_level+4,'process_write_request: veto %s, wr_en %s'%(vol_veto_list, wr_en))
                    movers = self.volumes_at_movers.get_active_movers()
                    found_mover = 0
                    Trace.trace(self.trace_level+4, 'process_write_request: movers %s'%(movers,))
                    for mover in movers:
                        Trace.trace(self.trace_level+40, "process_write_request: mover %s state %s time %s"%(mover['mover'], mover['state'],mover['time_in_state']))
                        if mover['state'] == 'HAVE_BOUND' and  mover['external_label'] in vol_veto_list:
                            found_mover = 1
                            break
                    if found_mover:
                        # if the number of write requests for a given file family more than the
                        # file family width then let it go.
                        Trace.trace(self.trace_level+40, "process_write_request: pending work families %s"%(self.pending_work.families,))
                        if (self.pending_work.families.has_key(rq.ticket["vc"]["file_family"])) and \
                           (self.pending_work.families[rq.ticket["vc"]["file_family"]] > rq.ticket["vc"]["file_family_width"]):
                           #len(matching_movers) == 1:
                            Trace.trace(self.trace_level+40, "process_write_request: will let this request go to idle mover")
                        else:
                            # check if file will fit to the volume at mover
                            fsize = rq.ticket['wrapper'].get('size_bytes', 0L)
                            ret = self.is_vol_available(rq.work,  mover['external_label'],
                                                        rq.ticket['vc']['volume_family'],
                                                        fsize, rq.ticket['vc']['address'])
                            Trace.trace(self.trace_level+40, "process_write_request: check_write_volume returned %s"%(ret,))
                            if (rq.work == "write_to_hsm" and
                                (ret['status'][0] == e_errors.VOL_SET_TO_FULL or
                                 ret['status'][0] == e_errors.NOSPACE or
                                 ret['status'][0] == 'full' or
                                 ret['status'][0] == 'readonly')):
                                Trace.trace(self.trace_level+40, "process_write_request: will let this request go to idle mover")
                            else:
                                Trace.trace(self.trace_level+40, 'process_write_request: will wait with this request go to %s'%
                                            (mover,))
                                self.continue_scan = 1
                                return rq, key_to_check # this request might go to the mover

        else:
            # disk mover
            vol_veto_list = []
            host_busy = self.client_host_busy(rq.ticket)
            if host_busy:
                sg, key_to_check = self.request_key(rq)
                self.continue_scan = 1
                return None, key_to_check # continue with key_to_ckeck

        Trace.trace(self.trace_level+4,"process_write_request: request next write volume for %s" % (vol_family,))

        # before assigning volume check if it is bound for the current family
        bound_vol = self.process_for_bound_vol
        # for bound volumes check what was priority of the last request
        if bound_vol and requestor["current_priority"][1] < 0:
            # last prority was regular
            if rq.adminpri > -1: # HIRI
                if bound_vol not in vol_veto_list:
                    bound_vol = None # this will allow preemption of regular priority requests
                else:
                    # Case when completed request was regular priority read request from this file family
                    # but the file in the next request can not be written to this volume
                    if would_preempt:
                        bound_vol = None # this will allow preemption of regular priority requests
        if bound_vol not in vol_veto_list:
            # width not exceeded, ask volume clerk for a new volume.
            Trace.trace(self.trace_level+4,"process_write_request for %s" % (rq.ticket,))
            self.set_vcc(rq.ticket['vc']['address'])

            start_t=time.time()
            v = self.next_write_volume(rq.ticket["vc"]["library"],
                                       rq.ticket["wrapper"]["size_bytes"]+self.min_file_size,
                                       vol_family,
                                       vol_veto_list,
                                       first_found=0,
                                       mover=requestor)

            Trace.trace(100, "process_write_request: next_write_volume, time in state %s"%(time.time()-start_t, ))
            Trace.trace(self.trace_level+4,"process_write_request: next write volume returned %s" % (v,))

            # volume clerk returned error
            if v["status"][0] != e_errors.OK:
                rq.ticket["reject_reason"] = (v["status"][0],v["status"][1])
                if v['status'][0] == e_errors.BROKEN: # too many volumes set to NOACCESS
                    if self.lm_lock != e_errors.BROKEN:
                        Trace.alarm(e_errors.ERROR,"LM %s goes to %s state" %
                                    (self.name, e_errors.BROKEN))
                        self.lm_lock = e_errors.BROKEN
                    return None, None

                if v["status"][0] == e_errors.NOVOLUME or v["status"][0] == e_errors.QUOTAEXCEEDED:
                    if not self.process_for_bound_vol:
                        #if wr_en > rq.ticket["vc"]["file_family_width"]:

                        # if volume veto list is not empty then work can be done later after
                        # the tape is available again
                        if not vol_veto_list or v["status"][0] == e_errors.QUOTAEXCEEDED:
                            # remove this request and send regret to the client
                            rq.ticket['status'] = v['status']
                            self.send_regret(rq.ticket)
                            self.pending_work.delete(rq)
                        rq = None
                else:
                    rq.ticket["status"] = v["status"]
                    #rq.ticket["reject_reason"] = (v["status"][0],v["status"][1])
                self.continue_scan = 1
                return rq, key_to_check
            else:
                suspect_v,suspect_mv = self.is_mover_suspect(requestor['mover'], v['external_label'])
                if suspect_mv:
                    Trace.log(e_errors.INFO,"mover %s is suspect for %s cannot assign a write work"%
                              (requestor['mover'], v["external_label"]))
                    self.continue_scan = 1
                    return rq, key_to_check
                rq.ticket["status"] = v["status"]
                external_label = v["external_label"]
        else:
            external_label = self.process_for_bound_vol

        # found a volume that has write work pending - return it
        rq.ticket["fc"]["external_label"] = external_label
        rq.ticket["fc"]["size"] = rq.ticket["wrapper"]["size_bytes"]

        # request has passed about all the criterias
        # check if it passes the fair share criteria
        # temprorarily store selected request to use it in case
        # when other request(s) based on fair share criteria
        # for some other reason(s) do not get selected

        # in any case if request SG limit is 0 and temporarily stored rq. SG limit is not,
        # do not update temporarily stored rq.
        rq_sg = volume_family.extract_storage_group(vol_family)
        if (rq.ticket.get('ignore_fair_share', None)):
            # do not count this request against fair share
            # this is an automigration request
            sg_limit = 0
        else:
            sg_limit = self.get_sg_limit(rq_sg)
            self.postponed_requests.put(rq)
        if self.tmp_rq:
            if sg_limit != 0:     # replace tmp_rq if rq SG limit is not 0
                # replace tmp_rq based on priority
                if rq.pri > self.tmp_rq.pri:
                    self.tmp_rq = rq
        else: self.tmp_rq = rq
        if self.sg_exceeded and self.process_for_bound_vol:
            rq = None
            self.continue_scan = 0
            key_to_check = None
        Trace.trace(self.trace_level+4, "process_write_request: returning %s %s"%(rq, key_to_check))

        return rq, key_to_check


    # is there any work for any volume?
    def next_work_any_volume(self, requestor):
        """
        Is there any work for any volume?

        :type requestor: :obj:`dict`
        :arg requestor: mover ticket
        :rtype: :obj:`tuple` (:obj:`manage_queue.Request` - request or :obj:`None`,
                              :obj:`tuple` - (error, :obj:`str` or :obj:`None`) - status)
        """


        Trace.trace(self.trace_level, "next_work_any_volume")
        self.init_request_selection() # start request selection cycle
        # The list of the active volumes.
        # Read request for the file on the active volume
        # can not get assigned to the idle mover.
        active_vols = self.volumes_at_movers.active_volumes()

        # look in pending work queue for reading or writing work
        #rq=self.pending_work.get(active_volumes=active_vols)
        rq=self._get_request(requestor, self.pending_work.get, active_volumes=active_vols)
        Trace.trace(self.trace_level+10, "next_work_any_volume: RQ: %s"%(rq,))
        while rq:
            rej_reason = None

            Trace.trace(self.trace_level+10, "next_work_any_volume: rq %s"%(rq.ticket,))
            if rq.ticket.has_key('reject_reason'):
                try:
                    rej_reason = rq.ticket['reject_reason'][0]
                    del(rq.ticket['reject_reason'])
                except KeyError:
                    exc, msg, tb = sys.exc_info()
                    Trace.handle_error(exc, msg, tb)
                    Trace.trace(self.trace_level+10, "next_work_any_volume KeyError: rq %s"%(rq.ticket,))
                    continue


            if rq.work == "read_from_hsm":
                rq, key = self.process_read_request(rq, requestor)
                Trace.trace(self.trace_level+41,"next_work_any_volume: process_read_request returned %s %s %s" % (rq, key,self.continue_scan))

                if self.continue_scan:
                    if key:
                        #rq = self.pending_work.get(key, next=1, active_volumes=active_vols, disabled_hosts=self.disabled_hosts)
                        rq = self._get_request(requestor, self.pending_work.get, key, next=1, active_volumes=active_vols, disabled_hosts=self.disabled_hosts)
                        # if there are no more requests for a given volume
                        # rq will be None, but we do not want to stop here
                        if rq:
                            # continue check with current volume
                            continue
                    #rq = self.pending_work.get(next=1, active_volumes=active_vols, disabled_hosts=self.disabled_hosts) # get next request
                    rq = self._get_request(requestor, self.pending_work.get, next=1, active_volumes=active_vols, disabled_hosts=self.disabled_hosts) # get next request
                    Trace.trace(self.trace_level+41,"next_work_any_volume: new rq %s" % (rq,))

                    continue
                break
            elif rq.work == "write_to_hsm":
                rq, key = self.process_write_request(rq, requestor)
                Trace.trace(self.trace_level+10,"next_work_any_volume: process_write_request returned %s %s %s" % (rq, key,self.continue_scan))
                if self.continue_scan:
                    if key:
                        #rq = self.pending_work.get(key, next=1, active_volumes=active_vols, disabled_hosts=self.disabled_hosts)
                        rq = self._get_request(requestor, self.pending_work.get, key, next=1, active_volumes=active_vols, disabled_hosts=self.disabled_hosts)
                        # if there are no more requests for a given volume family
                        # rq will be None, but we do not want to stop here
                        if rq:
                            # continue check with current volume
                            continue
                    #rq = self.pending_work.get(next=1, active_volumes=active_vols, disabled_hosts=self.disabled_hosts) # get next request
                    rq = self._get_request(requestor, self.pending_work.get, next=1, active_volumes=active_vols, disabled_hosts=self.disabled_hosts) # get next request
                    Trace.trace(self.trace_level+41,"next_work_any_volume: new rq %s" % (rq,))
                    continue
                break

            # alas, all I know about is reading and writing
            else:
                Trace.log(e_errors.ERROR,
                          "next_work_any_volume assertion error in next_work_any_volume %s"%(rq.ticket,))
                raise AssertionError
            Trace.trace(self.trace_level+41,"next_work_any_volume: continue")
            #rq = self.pending_work.get(next=1, active_volumes=active_vols, disabled_hosts=self.disabled_hosts)
            rq = self._get_request(requestor, self.pending_work.get, next=1, active_volumes=active_vols, disabled_hosts=self.disabled_hosts)

        if not rq or (rq.ticket.has_key('reject_reason') and rq.ticket['reject_reason'][0] == 'PURSUING'):
            saved_rq = rq
            # see if there is a temporary stored request
            Trace.trace(self.trace_level+10,"next_work_any_volume: using exceeded mover limit request")
            rq, self.postponed_sg = self.postponed_requests.get()
            Trace.trace(self.trace_level+10,"next_work_any_volume: get from postponed %s"%(rq,))
            if rq:
                self.postponed_rq = 1 # request comes from postponed requests list
                # check postponed request
                if rq.work == "read_from_hsm":
                    rq, key = self.process_read_request(rq, requestor)
                    Trace.trace(self.trace_level+10,"next_work_any_volume: process_read_request for postponed returned %s %s" %
                                (rq, key))
                elif rq.work == "write_to_hsm":
                    rq, key = self.process_write_request(rq, requestor)
                    Trace.trace(self.trace_level+10,"next_work_any_volume: process_write_request for postponed returned %s %s" %
                                (rq, key))
            else:
                if saved_rq:
                    rq = saved_rq
                    if rq.ticket.has_key('reject_reason'):
                        del rq.ticket['reject_reason']
                    Trace.trace(self.trace_level+10,"next_work_any_volume: proceed with rejected %s"%(rq,))
                elif self.tmp_rq:
                    rq = self.tmp_rq
                    Trace.trace(self.trace_level+10,"next_work_any_volume: get from tmp_rq %s"%(rq,))
                    if rq.work == "write_to_hsm":
                        rq, key = self.process_write_request(rq, requestor)
                        Trace.trace(self.trace_level+10, "next_work_any_volume: tmp_rq %s %s"%(rq.ticket, key))

        # check if this volume is ok to work with
        if rq:
            w = rq.ticket
            if w["status"][0] == e_errors.OK:
                if self.mover_type(requestor) == 'DiskMover':
                    key = 'vc' if (w['work'] == 'read_from_hsm') else 'fc'
                    label = w[key]['external_label']
                    ret = self.is_disk_vol_available(rq.work,label, requestor)
                else:
                    fsize = w['wrapper'].get('size_bytes', 0L)
                    method = w.get('method', None)
                    if method and method != "read_tape_start":
                        # size has a meaning only for general rq
                        fsize = fsize+self.min_file_size

                    try:
                        start_t=time.time()
                        ret = self.is_vol_available(rq.work,
                                                    w['fc']['external_label'],
                                                    w["vc"]["volume_family"],
                                                    fsize,
                                                    w['vc']['address'])
                        Trace.trace(100, "next_work_any_volume: vcc.is_vol_available, time in state %s"%(time.time()-start_t, ))

                    except KeyError, msg:
                        ret = w
                        ret['status'] = (e_errors.ERROR, "KeyError")
                        Trace.log(e_errors.ERROR, "Keyerror calling is_vol_available %s %s"%(w, msg))
                        return (None, (e_errors.NOWORK, None))

                if ret['status'][0] != e_errors.OK:
                    if ret['status'][0] == e_errors.BROKEN:
                        if self.lm_lock != e_errors.BROKEN:
                            Trace.alarm(e_errors.ERROR,"LM %s goes to %s state" %
                                        (self.name, e_errors.BROKEN))
                            self.lm_lock = e_errors.BROKEN
                        return  None, (e_errors.NOWORK, None)
                    Trace.trace(self.trace_level+10,"next_work_any_volume: work can not be done at this volume %s"%(ret,))
                    #w['status'] = ret['status']
                    if not (ret['status'][0] == e_errors.VOL_SET_TO_FULL or
                            ret['status'][0] == 'full' or
                            ret['status'][0] == e_errors.MEDIA_IN_ANOTHER_DEVICE):
                        w['status'] = ret['status']
                        self.pending_work.delete(rq)
                        self.send_regret(w)
                    Trace.log(e_errors.ERROR,
                              "next_work_any_volume: cannot do the work for %s status:%s" %
                              (rq.ticket['fc']['external_label'], rq.ticket['status'][0]))
                    return (None, (e_errors.NOWORK, None))
            else:
                if (w['work'] == 'write_to_hsm' and
                    (w['status'][0] == e_errors.VOL_SET_TO_FULL or
                     w['status'][0] == 'full')):
                    return None, (e_errors.NOWORK, None)
            return (rq, rq.ticket['status'])
        return (None, (e_errors.NOWORK, None))


    def schedule(self, mover):
        """
        What is next on our list of work?

        :type mover: :obj:`dict`
        :arg mover: mover ticket
        :rtype: :obj:`tuple` (:obj:`manage_queue.Request` - request or :obj:`None`,
                              :obj:`tuple` - (error, :obj:`str` or :obj:`None`) - status)
         """

        while 1:
            rq, status = self.next_work_any_volume(mover)
            if (status[0] == e_errors.OK or
                status[0] == e_errors.NOWORK):
                if rq and rq.ticket.has_key('reject_reason') and rq.ticket['reject_reason'][0] == "RESTRICTED_ACCESS":
                    Trace.trace(self.trace_level, "schedule: This request should not get here %s"%(rq,))
                    status = (e_errors.NOWORK, None)
                    rq = None
                return rq, status
            # some sort of error, like write work and no volume available
            # so bounce. status is already bad...
            self.pending_work.delete(rq)
            self.send_regret(rq.ticket)
            Trace.log(e_errors.INFO,"schedule: Error detected %s" % (rq.ticket,))

        return None, status

    def check_write_request(self, external_label, rq, requestor):
        """
        Check if write request can be sent to the mover.

        :type external_label: :obj:`str`
        :arg external_label: label of the volume to check
        :type rq: :obj:`manage_queue.Request`
        :arg rq: request to process
        :type requestor: :obj:`dict`
        :arg requestor: mover ticket
        :rtype: :obj:`tuple` (:obj:`manage_queue.Request` - request or :obj:`None`,
                              :obj:`str` - key to check next or :obj:`None`)
        """

        Trace.trace(self.trace_level, "check_write_request: label %s rq %s requestor %s"%
                    (external_label, rq, requestor))
        if self.mover_type(requestor) == 'DiskMover':
            ret = self.is_disk_vol_available(rq.work, external_label, requestor)
        else:
            vol_veto_list, wr_en = self.busy_volumes(rq.ticket['vc']['volume_family'])
            Trace.trace(self.trace_level+11, "check_write_request: vet_list %s wr_en %s"%(vol_veto_list, wr_en))
            label = rq.ticket['fc'].get('external_label', external_label)
            if label != external_label:
                # this is a case with admin pri
                # process it carefuly
                # check if tape is already mounted somewhere
                if label in vol_veto_list:
                    rq.ticket["reject_reason"] = ("VOLS_IN_WORK","")
                    Trace.trace(self.trace_level+11, "check_write_request: request for volume %s rejected %s Mounted somwhere else"%
                                    (external_label, rq.ticket["reject_reason"]))
                    rq.ticket['status'] = ("VOLS_IN_WORK",None)
                    return rq, rq.ticket['status']
            external_label = label
            Trace.trace(self.trace_level+11, "check_write_request %s %s"%(external_label, rq.ticket))
            if wr_en >= rq.ticket["vc"]["file_family_width"]:
                if (not external_label in vol_veto_list) and   (wr_en > rq.ticket["vc"]["file_family_width"]):
                    #if rq.adminpri < 0: # This allows request with admin pri to go even it exceeds its limit
                    rq.ticket["reject_reason"] = ("VOLS_IN_WORK","")
                    Trace.trace(self.trace_level+11, "check_write_request: request for volume %s rejected %s"%
                                (external_label, rq.ticket["reject_reason"]))
                    rq.ticket['status'] = ("VOLS_IN_WORK",None)
                    return rq, rq.ticket['status']

            fsize = rq.ticket['wrapper'].get('size_bytes', 0L)
            method = rq.ticket.get('method', None)
            if method and method != "read_tape_start":
                # size has a meaning only for general rq
                fsize = fsize+self.min_file_size


            start_t=time.time()
            ret = self.is_vol_available(rq.work,  external_label,
                                        rq.ticket['vc']['volume_family'],
                                        fsize,
                                        rq.ticket['vc']['address'])
            Trace.trace(100, "check_write_request: vcc.is_vol_avail, time in state %s"%(time.time()-start_t, ))
        # this work can be done on this volume
        if ret['status'][0] == e_errors.OK:
            rq.ticket['vc']['external_label'] = external_label
            rq.ticket['status'] = ret['status']
            rq.ticket["fc"]["size"] = rq.ticket["wrapper"]["size_bytes"]
            rq.ticket['fc']['external_label'] = external_label
            return rq, ret['status']
        else:
            rq.ticket['reject_reason'] = (ret['status'][0], ret['status'][1])
            if ret['status'][0] == e_errors.BROKEN:
                if self.lm_lock != e_errors.BROKEN:
                    Trace.alarm(e_errors.ERROR,"LM %s goes to %s state" %
                                (self.name, e_errors.BROKEN))
                    self.lm_lock = e_errors.BROKEN
                return  None,ret['status']
            # if work is write_to_hsm and volume has just been set to full
            # return this status for the immediate dismount
            if (rq.work == "write_to_hsm" and
                (ret['status'][0] == e_errors.VOL_SET_TO_FULL or
                 ret['status'][0] == 'full')):
                return None, ret['status']
        return rq, ret['status']

    def check_read_request(self, external_label, rq, requestor):
        """
        Check if read request can be sent to the mover.

        :type external_label: :obj:`str`
        :arg external_label: label of the volume to check
        :type rq: :obj:`manage_queue.Request`
        :arg rq: request to process
        :type requestor: :obj:`dict`
        :arg requestor: mover ticket
        :rtype: :obj:`tuple` (:obj:`manage_queue.Request` - request or :obj:`None`,
                              :obj:`str` - key to check next or :obj:`None`)
        """
        Trace.trace(self.trace_level,"check_read_request %s %s %s"%(rq.work,external_label, requestor))
        if self.mover_type(requestor) == 'DiskMover':
            ret = self.is_disk_vol_available(rq.work,external_label, requestor)
        else:
            fsize = rq.ticket['wrapper'].get('size_bytes', 0L)
            start_t=time.time()

            ret = self.is_vol_available(rq.work,  external_label,
                                        rq.ticket['vc']['volume_family'],
                                        fsize, rq.ticket['vc']['address'])
            Trace.trace(100, "vcc.is_vol_avail, time in state %s"%(time.time()-start_t, ))
        Trace.trace(self.trace_level+12,"check_read_request: ret %s" % (ret,))
        if ret['status'][0] != e_errors.OK:
            if ret['status'][0] == e_errors.BROKEN:
                if self.lm_lock != e_errors.BROKEN:
                    Trace.alarm(e_errors.ERROR,"LM %s goes to %s state" %
                                (self.name, e_errors.BROKEN))
                    self.lm_lock = e_errors.BROKEN
                return  None,ret['status']
            Trace.trace(self.trace_level+12,"check_read_request: work can not be done at this volume %s"%(ret,))
            rq.ticket['status'] = ret['status']
            self.pending_work.delete(rq)
            self.send_regret(rq.ticket)
            Trace.log(e_errors.ERROR,
                      "check_read_request: cannot do the work for %s status:%s" %
                                  (rq.ticket['fc']['external_label'], rq.ticket['status'][0]))
        else:
            rq.ticket['status'] = (e_errors.OK, None)
        return rq, rq.ticket['status']


    def next_work_this_volume(self, external_label, vol_family, last_work, requestor, current_location, priority=None):
        """
        Is there any work for this volume (mounted on requestor mover)?

        :type external_label: :obj:`str`
        :arg external_label: label of the volume to check
        :type vol_family: :obj:`str`
        :arg vol_family: volume family of current volume
        :type last_work: :obj:`str`
        :arg last_work: last work completed by requestor
        :type requestor: :obj:`dict`
        :arg requestor: mover ticket
        :type current_location: :obj:`str`
        :arg current_location: location cookie describing current position on tape
        :type priority: :obj:`tuple`
        :arg priority: (:obj:`int` - current_priority, :obj:`int` - admin_priority) - priority of last completed request
        :rtype: :obj:`tuple` (:obj:`manage_queue.Request` or :obj:`None` - request,
                             :obj:`bool` - flag confirming whether HIPRI request could go,
                             :obj:`bool` - flag indicating that the request will preempt the mounted volume)

        """
        Trace.trace(self.trace_level, "next_work_this_volume for %s %s %s %s %s %s" %
                    (external_label,vol_family, last_work, requestor, current_location, priority))
        status = None
        # what is current volume state?
        if requestor['mover_type'] != 'DiskMover':
            label = external_label
        else:
            # For disk mover external label is package_id
            if requestor['volume']:
                label = requestor['volume']
            else:
                return  None, (e_errors.NOWORK, None)

        self.current_volume_info = self.inquire_vol(label)

        Trace.trace(self.trace_level, "next_work_this_volume: current volume info: %s"%(self.current_volume_info,))
        if self.current_volume_info['status'][0] == e_errors.TIMEDOUT:
            Trace.log(e_errors.ERROR, "No volume info %s. Do not know how to proceed"%
                      (self.current_volume_info,))
            return  None, (e_errors.NOWORK, None)
        if self.current_volume_info['system_inhibit'][0] in (e_errors.NOACCESS, e_errors.NOTALLOWED):
            Trace.log(e_errors.ERROR, "Volume %s is unavailable: %s"%(external_label, self.current_volume_info['system_inhibit']))
            return  None, (e_errors.NOWORK, None)

        self.init_request_selection()
        self.process_for_bound_vol = external_label
        # use this key for slecting admin priority requests
        # this will select from reads for last op
        # read and writes for last op writes
        # these will remain "None" for elevated
        # non-admin priority
        key_for_admin_priority = None
        # use this key for selecting admin priority requests
        # if no admin priority request was found using
        # key_for_admin_priority
        alt_key_for_admin_priority = None

        # if current rq for bound volume has adminpri, process only admin requests for current
        # volume or current file family
        # priority is a tuple (regular_priority, admin_priority)
        # request has admin pri if (priority[1]) >= 0
        if priority and priority[1] >= 0:
            if last_work == 'WRITE':
               key_for_admin_priority = vol_family
               alt_key_for_admin_priority = external_label
            else:
                # read
                key_for_admin_priority = external_label
                alt_key_for_admin_priority = vol_family

        # first see if there are any HiPri requests
        # To avoid bouncing back and forth, if last
        # op was read, we look at admin reads first
        # (via "key_for_admin_priority") and writes
        # second. vice versa for last op write
        #rq = self.pending_work.get_admin_request(key=key_for_admin_priority)
        rq = self._get_request(requestor, self.pending_work.get_admin_request, key=key_for_admin_priority)
        if not rq:
            #rq = self.pending_work.get_admin_request(key=alt_key_for_admin_priority)
            rq = self._get_request(requestor, self.pending_work.get_admin_request, key=alt_key_for_admin_priority)
        checked_request = None
        would_preempt = False # no preemption of low pri requests by default
        while rq:
            Trace.trace(self.trace_level+42, "next_work_this_volume: rq1 %s"%(rq,))

            # The completed request had a regular priority
            if priority and priority[0] > 0 and priority[1] < 0:

                # If priority of completed request was regular
                # check if selected request is allowed to go to a mounted tape
                rq, allow, would_preempt = self.allow_hipri(rq, external_label, vol_family,
                                                            last_work, requestor, priority)
                if not allow:
                    # completed request had a regular proirity
                    # but the mounted tape can not be preempted by HIPRI request
                    # because there are idle movers tah could pick up HIPRI request
                    #rq = self.pending_work.get_admin_request(next=1,
                    rq = self._get_request(requestor, self.pending_work.get_admin_request, next=1,
                                                             disabled_hosts=self.disabled_hosts) # get next request
                    continue
                if rq and rq.work == "write_to_hsm":
                    # check if there is a potentially available tape at bound movers
                    # and if yes skip request so that it will be picked by bound mover
                    # this is done to aviod a sinle stream bouncing between different tapes
                    # if FF width is more than 1
                    if self.mover_type(requestor) == 'DiskMover':
                        rq = self._get_request(requestor, self.pending_work.get_admin_request, next=1,
                                               disabled_hosts=self.disabled_hosts) # get next request
                        continue

                    else:
                        vol_veto_list, wr_en = self.busy_volumes(rq.ticket["vc"]["volume_family"])
                        Trace.trace(self.trace_level+42,'next_work_this_volume: veto %s, wr_en %s'%
                                    (vol_veto_list, wr_en))
                        if wr_en < rq.ticket["vc"]["file_family_width"]:
                            movers = self.volumes_at_movers.get_active_movers()
                            found_mover = 0
                            for vol in vol_veto_list:
                                found_mover = 0
                                for mover in movers:
                                    Trace.trace(self.trace_level+42,'next_work_this_volume: vol %s mover %s'%
                                                (vol, mover))
                                    if vol == mover['external_label']:
                                        if mover['state'] == 'HAVE_BOUND' and mover['time_in_state'] < 31:
                                            # check if the volume mounted on this mover
                                            # fits to request
                                            fsize = rq.ticket['wrapper'].get('size_bytes', 0L)
                                            ret = self.vcc.is_vol_available(rq.work,  mover['external_label'],
                                                                            rq.ticket['vc']['volume_family'],
                                                                            fsize)

                                            if ret["status"][0] == e_errors.OK:
                                                found_mover = 1
                                                break
                                if found_mover:
                                    break
                            if found_mover:
                                if mover != requestor['mover']:
                                    Trace.trace(self.trace_level+42, 'next_work_this_volume: will wait with this request to go to %s %s'%
                                                (mover['mover'], mover['external_label']))

                                    #rq = self.pending_work.get_admin_request(next=1, disabled_hosts=self.disabled_hosts) # get next request
                                    rq = self._get_request(requestor, self.pending_work.get_admin_request, next=1, disabled_hosts=self.disabled_hosts) # get next request
                                    continue

            Trace.trace(self.trace_level+10, "next_work_this_volume: next admin rq %s"%(rq,))

            if rq.ticket.has_key('reject_reason'):
                del(rq.ticket['reject_reason'])

            if rq.work == 'read_from_hsm':
                rq, key = self.process_read_request(rq, requestor)
                if self.continue_scan:
                    if rq:
                        # before continuing check if it is a request
                        # for v['external_label']
                        if rq.ticket['fc']['external_label'] == external_label:
                            checked_request = rq
                            break
                    #rq = self.pending_work.get_admin_request(key=key_for_admin_priority,
                    rq = self._get_request(requestor, self.pending_work.get_admin_request, key=key_for_admin_priority,
                                            next=1,
                                            disabled_hosts=self.disabled_hosts) # get next request
                    if not rq:
                        # try alternative key
                        #rq = self.pending_work.get_admin_request(key=alt_key_for_admin_priority,
                        rq = self._get_request(requestor, self.pending_work.get_admin_request, key=alt_key_for_admin_priority,
                                                next=1,
                                                disabled_hosts=self.disabled_hosts)
                    Trace.trace(self.trace_level+10, "next_work_this_volume: continue with %s"%(rq,))
                    continue
                break
            elif rq.work == 'write_to_hsm':
                if self.mover_type(requestor) == 'DiskMover':
                    rq = self._get_request(requestor, self.pending_work.get_admin_request, key=key_for_admin_priority,
                                            next=1,
                                            disabled_hosts=self.disabled_hosts) # get next request
                    continue

                rq, key = self.process_write_request(rq, requestor, last_work=last_work, would_preempt=would_preempt)
                if self.continue_scan:
                    if rq:
                        rq, status = self.check_write_request(external_label, rq, requestor)
                        if rq and status[0] == e_errors.OK:
                            checked_request = rq
                            break
                    #rq = self.pending_work.get_admin_request(key=key_for_admin_priority,
                    rq = self._get_request(requestor, self.pending_work.get_admin_request, key=key_for_admin_priority,
                                            next=1,
                                            disabled_hosts=self.disabled_hosts) # get next request
                    if not rq:
                        # try alternative key
                        #rq = self.pending_work.get_admin_request(key=alt_key_for_admin_priority,
                        rq = self._get_request(requestor, self.pending_work.get_admin_request, key=alt_key_for_admin_priority,
                                                next=1,
                                                disabled_hosts=self.disabled_hosts)
                    Trace.trace(self.trace_level+10, "next_work_this_volume: continue with %s"%(rq,))
                    continue
                break
        # end while

        if not rq:
            # no request matching to all criterias
            # use a temporarily stored request
            Trace.trace(self.trace_level+10, "next_work_this_volume: use tmp_rq %s"%(self.tmp_rq,))
            rq = self.tmp_rq
        if rq:
            Trace.trace(self.trace_level+10, "next_work_this_volume: HIPRI processing result %s" % (rq.ticket,))
            if rq.work == 'read_from_hsm':
                if checked_request and checked_request.unique_id == rq.unique_id:
                    # This is a case when rq != self.tmp_rq.
                    # Request was already checked
                    status = (e_errors.OK, None)
                else:
                    # This is a case when rq == self.tmp_rq
                    # Need to check this request
                    rq, status = self.check_read_request(external_label, rq, requestor)
                if rq and status[0] == e_errors.OK:
                    return rq, status
            elif rq.work == 'write_to_hsm':
                if checked_request and checked_request.unique_id == rq.unique_id:
                    # This is a case when rq != self.tmp_rq.
                    # Request was already checked
                   status = (e_errors.OK, None)
                else:
                    # This is a case when rq == self.tmp_rq
                    # Need to check this request
                    rq, status = self.check_write_request(external_label, rq, requestor)
                if rq and status[0] == e_errors.OK:
                    return rq, status

        # no HIPri requests: look in pending work queue for reading or writing work
        # see what priority has a completed request
        use_this_volume = 1
        if priority and priority[0] and  priority[0] <= 0:
            self.init_request_selection()
            # this is a lower priority request (usually used for migration)
            # it can be preempted by any normal priority request
            # process request
            start_t=time.time()
            rq, status = self.schedule(requestor)
            Trace.trace(self.trace_level+10,"next_work_this_volume: SCHEDULE RETURNED %s %s"%(rq, status))
            Trace.trace(100, "next_work_this_volume: SCHEDULE, time in state %s"%(time.time()-start_t, ))
            if rq and rq.ticket['encp']['curpri'] > 0:
                # preempt current low priority request
                # by request with normal priority
                use_this_volume = 0

        if use_this_volume:
            self.init_request_selection()
            self.process_for_bound_vol = external_label
            # for tape positioning optimization check what was
            # a last work for this volume
            if last_work == 'WRITE':
                # see if there is another work for this volume family
                # disable retrival of HiPri requests as they were
                # already treated above
                #rq = self.pending_work.get(vol_family, use_admin_queue=0)
                rq = self._get_request(requestor, self.pending_work.get, vol_family, use_admin_queue=0)
                Trace.trace(self.trace_level+10, "next_work_this_volume: use volume family %s rq %s"%
                            (vol_family, rq))
                if not rq:
                    #rq = self.pending_work.get(external_label, current_location, use_admin_queue=0)
                    rq = self._get_request(requestor, self.pending_work.get, external_label, current_location, use_admin_queue=0)
                    Trace.trace(self.trace_level+10, "next_work_this_volume: use label %s rq %s"%
                                (external_label, rq))

            else:
                # see if there is another work for this volume
                # disable retrival of HiPri requests as they were
                # already treated above
                #rq = self.pending_work.get(external_label, current_location, use_admin_queue=0)
                rq = self._get_request(requestor, self.pending_work.get, external_label, current_location, use_admin_queue=0)
                Trace.trace(self.trace_level+10, "next_work_this_volume: use label %s rq %s"%
                            (external_label, rq))
                if not rq:
                    #rq = self.pending_work.get(vol_family, use_admin_queue=0)
                    rq = self._get_request(requestor, self.pending_work.get, vol_family, use_admin_queue=0)
                    Trace.trace(self.trace_level+10, "next_work_this_volume: use volume family %s rq %s"%
                                (vol_family, rq))

            exc_limit_rq = None # exceeded limit requests
            rqs = []
            while rq:
                found = 0
                for r in rqs:
                    if r.unique_id == rq.unique_id:
                        found = 1
                        Trace.log(e_errors.INFO, "Found the same id. Looks like going in cycles. Will break")
                        break
                else:
                    rqs.append(rq)
                if found:
                    rq = None
                    break
                if rq.ticket.has_key('reject_reason'):
                    del(rq.ticket['reject_reason'])

                if rq:
                    Trace.trace(self.trace_level+10, "next_work_this_volume: s2 rq %s" % (rq.ticket,))
                    if rq.work == 'read_from_hsm':
                        rq, key = self.process_read_request(rq, requestor)
                        if self.continue_scan:
                            # before continuing check if it is a request
                            # for v['external_label']
                            if rq and rq.ticket['fc']['external_label'] == external_label:
                                Trace.trace(self.trace_level+10, "next_work_this_volume:exc_limit_rq 1 %s"%(rq,))
                                exc_limit_rq = rq
                                checked_request = rq
                                break
                            if last_work == "READ":
                                # volume is readonly: get only read requests
                                #rq = self.pending_work.get(external_label,  next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts) # get next request
                                rq = self._get_request(requestor, self.pending_work.get, external_label,  next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts) # get next request
                                if not rq:
                                    # see if there is a write work for the current volume
                                    # volume family
                                    #rq = self.pending_work.get(vol_family, next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts)
                                    rq = self._get_request(requestor, self.pending_work.get, vol_family, next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts)
                            else:
                                #rq = self.pending_work.get(vol_family,  next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts) # get next request
                                rq = self._get_request(requestor, self.pending_work.get, vol_family,  next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts) # get next request
                            continue
                        break
                    elif rq.work == 'write_to_hsm':
                        rq, key = self.process_write_request(rq, requestor, last_work=last_work)
                        Trace.trace(self.trace_level+10, "next_work_this_volume:process_write_request returned %s continue_scan %s "%((rq, key), self.continue_scan))
                        if self.continue_scan:
                            if rq:
                                if checked_request and checked_request.unique_id == rq.unique_id:
                                    status = (e_errors.OK, None)
                                else:
                                    rq, status = self.check_write_request(external_label, rq, requestor)
                                if rq and status[0] == e_errors.OK:
                                    Trace.trace(self.trace_level+10, "next_work_this_volume: exc_limit_rq 2 %s"%(rq,))
                                    exc_limit_rq = rq
                                    checked_request = rq
                                    break
                            Trace.trace(self.trace_level+10, "next_work_this_volume: current_volume_info %s"%(self.current_volume_info,))
                            if last_work == "WRITE":
                                #rq = self.pending_work.get(vol_family,  next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts) # get next request
                                rq = self._get_request(requestor, self.pending_work.get, vol_family,  next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts) # get next request
                                if not rq:
                                    # see if there is a read work for the current volume
                                    # volume family
                                    #rq = self.pending_work.get(external_label,  next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts) # get next request
                                    rq = self._get_request(requestor, self.pending_work.get, external_label,  next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts) # get next request
                            else:
                                #rq = self.pending_work.get(external_label,  next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts) # get next request
                                rq =  self._get_request(requestor, self.pending_work.get, external_label,  next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts) # get next request

                            continue
                        break
            # end while

            if not rq and self.tmp_rq:
                rq = self.tmp_rq

            if exc_limit_rq: # request with exceeded SG limit
                rq = exc_limit_rq

            if rq and rq.work == 'write_to_hsm' and self.mover_type(requestor) != 'DiskMover':
                while rq:
                    Trace.trace(self.trace_level+10,"next_work_this_volume: LABEL %s RQ %s" % (external_label, rq))
                    # regular write request must have the same volume label
                    rq.ticket['fc']['external_label'] = external_label
                    if checked_request and checked_request.unique_id == rq.unique_id:
                        status = (e_errors.OK, None)
                    else:
                        rq, status = self.check_write_request(external_label, rq, requestor)
                    Trace.trace(self.trace_level+10,"next_work_this_volume: RQ %s STAT %s" %(rq,status))
                    if rq: Trace.trace(self.trace_level+10,"next_work_this_volume: TICK %s" %(rq.ticket,))
                    if rq and status[0] == e_errors.OK:
                        return rq, status
                    if not rq: break
                    Trace.trace(self.trace_level+10, "next_work_this_volume: got here")
                    #rq = self.pending_work.get(vol_family, next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts)
                    rq = self._get_request(requestor, self.pending_work.get, vol_family, next=1, use_admin_queue=0, disabled_hosts=self.disabled_hosts)
            # return read work
            if rq:
                Trace.trace(self.trace_level+10, "next_work_this_volume: s4 rq %s" % (rq.ticket,))
                if checked_request and checked_request.unique_id == rq.unique_id:
                    status = (e_errors.OK, None)
                else:
                    rq, status = self.check_read_request(label, rq, requestor)
                return rq, status

        return (None, (e_errors.NOWORK, None))

    def is_volume_suspect(self, external_label):
        """
        Check if volume is in the suspect volume list.

        :type external_label: :obj:`str`
        :arg external_label: label of the volume
        :rtype: :obj:`dict` - volume record or :obj:`None`
        """

        # remove volumes time in the queue for wich has expired
        if self.suspect_vol_expiration_to:
            # create a list of expired volumes
            now = time.time()
            expired_vols = []
            for vol in self.suspect_volumes.list:
                if now - vol['time'] >= self.suspect_vol_expiration_to:
                    expired_vols.append(vol)
            # cleanup suspect volume list
            if expired_vols:
                for vol in expired_vols:
                    Trace.log(e_errors.INFO,"%s has been removed from suspect volume list due to TO expiration"%
                              (vol['external_label'],))
                    self.suspect_volumes.remove(vol)

        Trace.trace(self.trace_level+11, "is_volume_suspect: external label %s suspect_volumes.list: %s"%(external_label,self.suspect_volumes.list))
        for vol in self.suspect_volumes.list:
            if external_label == vol['external_label']:
                Trace.trace(self.trace_level+11, "is_volume_suspect: returning %s"%(vol, ))
                return vol
        Trace.trace(self.trace_level+11, "is_volume_suspect: returning None")
        return None

    # check if mover is in the suspect volume list
    # return tuple (suspect_volume, suspect_mover)
    def is_mover_suspect(self, mover, external_label):
        """
        Check if mover is in the suspect volume list.

        :type mover: :obj:`str`
        :arg mover: mover name
        :type external_label: :obj:`str`
        :arg external_label: label of the volume
        :rtype: :obj:`tuple` (:obj:`str`- volume name or :obj:`None`,
                              :obj:`str`- mover name or :obj:`None`)
        """

        Trace.trace(self.trace_level+11, "is_mover_suspect: %s %s"%(mover, external_label))
        vol = self.is_volume_suspect(external_label)
        if vol:
            for mov in vol['movers']:
                if mover == mov:
                    break
            else:
                Trace.trace(self.trace_level+11, "is_mover_suspect: returning %s, None"%(vol,))
                return vol,None
            Trace.trace(self.trace_level+11, "is_mover_suspect: returning %s %s"%(vol, mov))
            return vol,mov
        else:
            Trace.trace(self.trace_level+11, "is_mover_suspect: returning None, None")
            return None,None

    # update suspect volumer list
    def update_suspect_vol_list(self, external_label, mover):
        """
        Update suspect volumer list.

        :type external_label: :obj:`str`
        :arg external_label: label of the volume
        :type mover: :obj:`str`
        :arg mover: mover name
        :rtype: :obj:`dict` - suspect volume dictionary
        """

        # update list of suspected volumes
        Trace.trace(self.trace_level+11,"update_suspect_vol_list: SUSPECT VOLUME LIST BEFORE %s"%(self.suspect_volumes.list,))
        if not external_label: return None
        vol_found = 0
        for vol in self.suspect_volumes.list:
            if external_label == vol['external_label']:
                vol_found = 1
                break
        if not vol_found:
            vol = {'external_label' : external_label,
                   'movers' : [],
                   'time':time.time()
                   }
        for mv in vol['movers']:
            if mover == mv:
                break
        else:
            vol['movers'].append(mover)
        if not vol_found:
            self.suspect_volumes.append(vol)
            # send alarm if number of suspect volumes is above a threshold
            if len(self.suspect_volumes.list) >= self.max_suspect_volumes:
                Trace.alarm(e_errors.WARNING, e_errors.ABOVE_THRESHOLD,
                            {"volumes":"Number of suspect volumes is above threshold"})

        Trace.trace(self.trace_level+11, "update_suspect_vol_list: SUSPECT VOLUME LIST AFTER %s" % (self.suspect_volumes,))
        return vol
    ############################################
    # End request processing methods
    ############################################



class LibraryManager(dispatching_worker.DispatchingWorker,
                     generic_server.GenericServer,
                     LibraryManagerMethods):
    """
    Library manager methods processing movers and enstore command-line requests.
    """

    def __init__(self, libman, csc):
        """
        :type libman: :obj:`str`
        :arg libman: unique library manager name
        :type csc: :class:`configuration_client.ConfigurationClient`
        :arg csc: configuration client instance. Also can be server address:
                 :obj:`tuple` (:obj:`str`- IP address, :obj:`int` - port)
        """

        self.name_ext = "LM"
        self.csc = csc
        generic_server.GenericServer.__init__(self, self.csc, libman,
					      function = self.handle_er_msg)
        self.name = libman
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        self.keys = self.csc.get(libman)
        self.alive_interval = monitored_server.get_alive_interval(self.csc,
                                                                  libman,
                                                                  self.keys)
        self.pri_sel = priority_selector.PriSelector(self.csc, self.name)

        self.lm_lock = self.get_lock()
        if not self.lm_lock:
            self.lm_lock = e_errors.UNLOCKED
        self.set_lock(self.lm_lock)
        Trace.log(e_errors.INFO,"Library manager started in state:%s"%(self.lm_lock,))

        # setup a start up delay
        # this delay is needed to update state of the movers
        self.startup_delay = self.keys.get('startup_delay', 32)
        # add this to file size when requesting
        # a tape for writes to avoid FTT_ENOSPC at the end of the tape
        # due to inaccurate REMAINING_BYTES
        min_file_size = self.keys.get('min_file_size',0L)
        # maximal file size
        self.max_file_size = self.keys.get('max_file_size', 2*GB - 2*KB)
        self.max_suspect_movers = self.keys.get('max_suspect_movers',3) # maximal number of movers in the suspect volume list
        self.max_suspect_volumes = self.keys.get('max_suspect_volumes', 100) # maximal number of suspected volumes for alarm generation
        self.blank_error_increment = self.keys.get('blank_error_increment', 5) # this + max_suspect_movers shuold not be more than total number of movers
        self.use_raw = self.keys.get('use_raw_input', None)
        self.time_started = time.time()
        self.startup_flag = 1   # this flag means that LM is in the startup state
        self.my_trace_level = 11

        dispatching_worker.DispatchingWorker.__init__(self, (self.keys['hostip'],
                                                             self.keys['port']),
                                                      use_raw=self.use_raw)
        if self.keys.has_key('mover_port'):
            self.mover_server = dispatching_worker.DispatchingWorker((self.keys['hostip'],
                                                                      self.keys['mover_port']),
                                                                     use_raw=self.use_raw)
            self.mover_server.disable_reshuffle() # disble reshuffling of incoming requests
            self.mover_server.set_keyword("mover")
            # do not allow getting erc messages via this port
            self.mover_server.disable_callback()

        if self.keys.has_key('encp_port'):
            self.encp_server = dispatching_worker.DispatchingWorker((self.keys['hostip'],
                                                                     self.keys['encp_port']),
                                                                    use_raw=self.use_raw)
            # do not allow getting erc messages via this port
            self.encp_server.disable_callback()


        # setup the communications with the event relay task
        self.resubscribe_rate = 300
        self.erc = event_relay_client.EventRelayClient(self, function = self.handle_er_msg)
        Trace.erc = self.erc # without this Trace.notify takes 500 times longer
        self.erc.start([event_relay_messages.NEWCONFIGFILE],
                       self.resubscribe_rate)
        # start our heartbeat to the event relay process
        self.erc.start_heartbeat(self.name, self.alive_interval,
                                 self.return_state)

        sg_limits = None
        if self.keys.has_key('storage_group_limits'):
            sg_limits = self.keys['storage_group_limits']
        self.legal_encp_version = self.keys.get('legal_encp_version','')
        self.suspect_vol_expiration_to = self.keys.get('suspect_volume_expiration_time',None)
        self.share_movers = self.keys.get('share_movers', None) # for the federation to fair share
                                                                # movers across multiple library managers
        self.allow_access = self.keys.get('allow', None) # allow host access on a per storage group
        self.max_time_in_active = self.keys.get('max_in_active', 7200)
        self.max_time_in_other = self.keys.get('max_in_other', 2000)

        LibraryManagerMethods.__init__(self, self.name,
                                       self.csc,
                                       sg_limits,
                                       min_file_size,
                                       self.max_suspect_movers,
                                       self.max_suspect_volumes)
        self.init_postponed_requests(self.keys.get('rq_wait_time',3600))
        self.restrictor = discipline.Restrictor(self.csc, self.name)
        self.reinit()

        self.set_udp_client()

        self.volume_assert_list = []
        # run mover requests processing methods in separate threads
        # this causes unpredictable behavior
        # False by default
        self.use_threads = self.keys.get('use_threads', False)
        if self.use_threads:
            self.in_progress_lock = threading.Lock()
            self.mover_request_in_progress = False # this is needed only in threaded mode.
        # allow running some methods as forked unix processes
        # False bay default
        self.do_fork = self.keys.get('do_fork', False)
        if hasattr(self, "mover_server"):
            self.mover_requests =  Requests(self.mover_server, self)
        self.client_requests = Requests(self)
        if hasattr(self, "encp_server"):
            self.encp_requests = Requests(self.encp_server, self)


    # this is replaced by dispatching_worker.run_in_thread
    def run_in_thread(self, thread_name, function, args=(), after_function=None):
        thread = getattr(self, thread_name, None)
        # there was a race condition seen whet thread said it exited and run in thread said it was running
        # this is why retry
        Trace.trace(5,"run_in_thread %s"%(thread,))

        for wait in range(2):
            if thread and thread.isAlive():
                Trace.trace(5, "run_in_thread: thread %s is already running, waiting %s" % (thread_name, wait))
                time.sleep(1)
        if thread and thread.isAlive():
            Trace.trace(5, "run_in_thread: thread %s is already running" % (thread_name,))
            return 1
        if after_function:
            args = args + (after_function,)
        Trace.trace(5, "run_in_thread: create thread: target %s name %s args %s" % (function, thread_name, args))
        thread = threading.Thread(group=None, target=function,
                                  name=thread_name, args=args, kwargs={})
        setattr(self, thread_name, thread)
        Trace.trace(5, "run_in_thread: starting thread %s"%(dir(thread,)))
        try:
            thread.start()
        except:
            exc, detail, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "starting thread %s: %s" % (thread_name, detail))
        return 0

    def request_thread(self, function, ticket):
        t0 = time.time()
        apply(function, (ticket,))
        dt = time.time() - t0
        Trace.trace(5, 'request_thread:thread finished %s %s %s'%(ticket['work'], ticket['mover'], dt))
        """
        if dt >= 3.:
            Trace.trace(5,"request_thread:process_mover_request: changing logging")
            try:
                if not self.lch:
                    self._do_print({'levels':range(500)})

            except AttributeError:
                self._do_print({'levels':range(500)})
                self.lch = True
        """

        '''
        if (function, ticket) in self.mover_rq_in_progress:
            Trace.trace(5, 'removing %s from %s'%((function, ticket), self.mover_rq_in_progress))
            self.mover_rq_in_progress.remove((function, ticket))
        '''
        #self.set_mover_request_in_progress(False)

    def set_mover_request_in_progress(self, value=False):
        if self.mover_request_in_progress != value:
            self.in_progress_lock.acquire()
            self.mover_request_in_progress = value
            self.in_progress_lock.release()

    def accept_request(self, ticket):
        Trace.trace(self.my_trace_level+100,"accept_request:queue %s max rq %s priority %s"%
                    (self.pending_work.queue_length, self.max_requests, ticket['encp']['adminpri']))
        if self.pending_work.queue_length > self.max_requests:
            # allow only adminpri
            if ticket['encp']['adminpri'] > -1:
                rc= 1
            rc = 0
        else:
            rc = 1
        Trace.trace(self.my_trace_level+100, "accept_request: rc %s"%(rc,))
        return rc


    # check startup flag
    def is_starting(self):
        if self.startup_flag:
            if time.time() - self.time_started > self.startup_delay:
               self.startup_flag = 0
        return self.startup_flag


    def lockfile_name(self):
        d=os.environ.get("ENSTORE_TMP","/tmp")
        return os.path.join(d, "%s_lock"%(self.name,))


    # get lock from a lock file
    def get_lock(self):
        if self.keys.has_key('lock'):
            # get starting state from configuration
            # it can be: unlocked, locked, ignore, pause
            # the meaning of these states:
            # unlocked -- no comments
            # locked -- reject encp requests, give out works in the pending queue to movers
            # ignore -- do not put encp requests into pending queue, but return ok to encp,
            #           and give out works in the pending queue to movers
            # pause -- same as ignore, but also do not give out works in the pending
            #          queue to movers
            # nowrite -- locked for write requests
            # noread -- locked for read requests

            if self.keys['lock'] in (e_errors.LOCKED, e_errors.UNLOCKED, e_errors.IGNORE, e_errors.PAUSE, e_errors.NOWRITE, e_errors.NOREAD):
                return self.keys['lock']
        try:
            lock_file = open(self.lockfile_name(), 'r')
            lock_state = lock_file.read()
            lock_file.close()
        except IOError:
            lock_state = None
        return lock_state

    # set lock in a lock file
    def set_lock(self, lock):
        if not os.path.exists(self.lockfile_name()):
           dirname, basename = os.path.split(self.lockfile_name())
           if not os.path.exists(dirname):
               os.makedirs(dirname)
        lock_file = open(self.lockfile_name(), 'w')
        lock_file.write(lock)
        lock_file.close()


    def set_udp_client(self):
        self.udpc = udp_client.UDPClient()
        self.rcv_timeout = 10 # set receive timeout

    def access_granted(self, ticket):
        """
        Grant client access based on the list of allowed hosts
        for a given storage group in configuration if such exists.
        This controls read or write access for the specified client hosts.

        :type ticket: :obj:`dict`
        :arg ticket: work request ticket

        :rtype: :obj:`int` 1 - allowed, 0 - not allowed
        """
        self.allow_access = self.keys.get('allow', None) # allow host access on a per storage group
        Trace.trace(self.my_trace_level+100, 'access_granted: allow_access %s'%(self.allow_access,))
        if self.allow_access == None:
            return 1
        if ticket['vc'].has_key('storage_group'):
            if  self.allow_access.has_key(ticket['vc']['storage_group']):
                host_from_ticket = self.get_host_name_from_ticket(ticket)
                Trace.trace(self.my_trace_level+100, 'access_granted: host %s, list %s'%
                            (host_from_ticket, self.allow_access[ticket['vc']['storage_group']]))
                for host in self.allow_access[ticket['vc']['storage_group']]:
                    if re.search('^%s'%(host,), host_from_ticket):  # host is in the list: acccess granted
                        return 1
                return 0  # host is not in the list acccess permitted
            else:
                return 1
        return 1


    def verify_data_transfer_request(self, ticket):
        """
        Verify client work ticket.
        It must contain specific keys and have specific structure

        :type ticket: :obj:`dict`
        :arg ticket: work request ticket
        :rtype ticket: :obj:`dict` ticket or :obj:`None`
        """

        saved_reply_address = ticket.get('r_a', None)
        work = ticket["work"]
        error_detected = False # no errors detected yet
        # have we exceeded the number of allowed requests?
        if self.accept_request(ticket) == 0:
            ticket["status"] = (e_errors.OK, None)
            error_detected = True

        if not error_detected and self.access_granted(ticket) == 0:
            if work == "write_to_hsm":
                ticket['status'] = (e_errors.NOWRITE,
                                    "You have no permission to write from this host")
            else:
                ticket['status'] = (e_errors.NOREAD,
                                    "You have no permission to read from this host")
                error_detected = True

        if not error_detected and ticket.has_key('vc') and ticket['vc'].has_key('file_family_width'):
            try:
                ticket['vc']['file_family_width'] = int(ticket['vc']['file_family_width']) # ff width must be an integer
            except:
                # there was a traceback: ValueError: invalid literal for int() with base 10: 'test'
                # several times
                exc, detail, tb = sys.exc_info()
                Trace.handle_error(exc, detail, tb)
                Trace.log(e_errors.ERROR, "ticket %s"%(ticket,))
                raise exc, detail
        if not error_detected:
            if work == "write_to_hsm":
                key = encp_ticket.write_request_ok(ticket)
            else:
                key = encp_ticket.read_request_ok(ticket)
            if key:
                ticket['status'] = (e_errors.MALFORMED,
                                    "ticket does not have a mandatory key %s"%(key,))
                error_detected = True

        if not error_detected:
            if ticket.get("work_in_work") == "volume_assert":
                # do not check version
                pass
            else:
                error_detected = self.restrict_version_access(ticket['vc']['storage_group'], self.legal_encp_version, ticket)

        if not error_detected:
            # check if request is alredy in the queue
            rc, status = self.pending_work.test(ticket)
            if rc:
                ticket['status'] = (status, None)

                if work =="write_to_hsm":
                    _format = "Rq. is already in the queue %s %s (%s) -> %s : library=%s family=%s requester:%s volume_family:%s"
                    Trace.log(e_errors.INFO, _format%
                              (ticket["work"],
                               ticket["wrapper"]["fullname"],
                               ticket["unique_id"],
                               ticket["wrapper"]["pnfsFilename"],
                               ticket["vc"]["library"],
                               ticket["vc"]["file_family"],
                               ticket["wrapper"]["uname"],
                               ticket['vc']["volume_family"]))
                else:
                    _format = "Rq. is already in the queue %s %s (%s) -> %s : library=%s family=%s requester:%s"
                    Trace.log(e_errors.INFO, _format%
                              (ticket["work"],
                               ticket["wrapper"]["fullname"],
                               ticket["unique_id"],
                               ticket["wrapper"]["pnfsFilename"],
                               ticket["vc"]["library"],
                               ticket["vc"]["volume_family"],
                               ticket["wrapper"]["uname"]))

                #Trace.trace(self.my_trace_level+100, "call back %s pending callback %s"%(ticket["callback_addr"], rq.callback))
                Trace.trace(self.my_trace_level+100,"%s: request is already in the queue %s"%(work, ticket["unique_id"],))
                error_detected = True
        if error_detected:
            # it has been observerd that in multithreaded environment
            # ticket["r_a"] somehow gets modified
            # so to be safe restore  ticket["r_a"] just before sending
            ticket["r_a"] = saved_reply_address
            self.reply_to_caller(ticket) # reply now to avoid deadlocks
            ticket = None
        return ticket


    ########################################
    # data transfer requests
    ########################################

    def write_to_hsm(self, ticket):
        """
        Process client write work request.
        Check if it can be accepted and put into pending requests queue.
        This method is called within :class:`dispatching_worker.DispatchingWorker`

        :type ticket: :obj:`dict`
        :arg ticket: work request ticket
        """
        Trace.trace(self.my_trace_level+100, "write_to_hsm: ticket %s"%(ticket))
        saved_reply_address = ticket.get('r_a', None)

        # mangle file family for file copy request
        if ticket.has_key('copy'):
            if ticket['fc'].has_key('original_bfid'):
                if ticket.has_key('vc') and ticket['vc'].has_key('original_file_family'):
                    ticket['vc']['file_family'] = "%s_%s_%s"%(ticket['vc']['original_file_family'],'copy',int(ticket['copy']))
                else:
                    ticket['status'] = (e_errors.MALFORMED,
                                        "ticket does not have a key for copy %s"%('original_file_family',))
                    # it has been observerd that in multithreaded environment
                    # ticket["r_a"] somehow gets modified
                    # so to be safe restore  ticket["r_a"] just before sending
                    ticket["r_a"] = saved_reply_address
                    self.reply_to_caller(ticket)
                    return
            else:
                ticket['status'] = (e_errors.MALFORMED,
                                    "ticket does not have a key for copy %s"%('original_bfid',))
                # it has been observerd that in multithreaded environment
                # ticket["r_a"] somehow gets modified
                # so to be safe restore  ticket["r_a"] just before sending
                ticket["r_a"] = saved_reply_address
                self.reply_to_caller(ticket)
                return

        if ticket.has_key('vc') and ticket['vc'].has_key('file_family_width'):
            ticket['vc']['file_family_width'] = int(ticket['vc']['file_family_width']) # ff width must be an integer


        fsize = ticket['wrapper'].get('size_bytes',0L)
        if fsize > self.max_file_size:
                ticket['status'] = (e_errors.USERERROR,
                                    "file size %s more than max. %s"%(fsize, self.max_file_size))
                # it has been observerd that in multithreaded environment
                # ticket["r_a"] somehow gets modified
                # so to be safe restore  ticket["r_a"] just before sending
                ticket["r_a"] = saved_reply_address
                self.reply_to_caller(ticket)
                return

        if ticket.has_key('mover'):
            Trace.log(e_errors.WARNING,'input ticket has key mover in it %s'%(ticket,))
            del(ticket['mover'])
        if ticket['vc'].has_key('external_label'):
            del(ticket['vc']['external_label'])
        if ticket['fc'].has_key('external_label'):
            del(ticket['fc']['external_label'])

        # verify data transfer request here after some entries in incoming
        # ticket were modified
        ticket = self.verify_data_transfer_request(ticket)
        if not ticket:
            # ticket did not pass verification
            # client response has been sent by
            # verify_data_transfer_request()
            return

        # data for Trace.notify
        host = ticket['wrapper']['machine'][1]
        work = 'write'
        ff = ticket['vc']['file_family']
        #if self.lm_lock == 'locked' or self.lm_lock == e_errors.IGNORE:


        if self.lm_lock in (e_errors.LOCKED, e_errors.IGNORE, e_errors.PAUSE, e_errors.NOWRITE, e_errors.BROKEN):
            if self.lm_lock in  (e_errors.LOCKED, e_errors.NOWRITE):
                ticket["status"] = (self.lm_lock, "Library manager is locked for external access")
            else:
                ticket["status"] = (e_errors.OK, None)
            # it has been observerd that in multithreaded environment
            # ticket["r_a"] somehow gets modified
            # so to be safe restore  ticket["r_a"] just before sending
            ticket["r_a"] = saved_reply_address
            self.reply_to_caller(ticket)
            Trace.notify("client %s %s %s %s" % (host, work, ff, self.lm_lock))
            return


        # check file family width
        ff_width = ticket["vc"].get("file_family_width", 0)
        if ff_width <= 0:
            ticket["status"] = (e_errors.USERERROR, "wrong file family width %s" % (ff_width,))
            # it has been observerd that in multithreaded environment
            # ticket["r_a"] somehow gets modified
            # so to be safe restore  ticket["r_a"] just before sending
            ticket["r_a"] = saved_reply_address
            self.reply_to_caller(ticket)
            Trace.notify("client %s %s %s %s" % (host, work, ff, 'rejected'))
            return

        ticket["status"] = (e_errors.OK, None)

        for item in ('storage_group', 'file_family', 'wrapper'):
            if ticket['vc'].has_key(item):
                if not charset.is_in_charset(ticket['vc'][item]):
                    ticket['status'] = (e_errors.USERERROR,
                                        "%s contains illegal character"%(item,))
                    # it has been observerd that in multithreaded environment
                    # ticket["r_a"] somehow gets modified
                    # so to be safe restore  ticket["r_a"] just before sending
                    ticket["r_a"] = saved_reply_address
                    self.reply_to_caller(ticket)
                    return

        ## check if there are any additional restrictions
        rc, fun, args, action = self.restrictor.match_found(ticket)

        Trace.trace(self.my_trace_level+100,"write_to_hsm:match returned %s %s %s %s"% (rc, fun, args, action))
        if fun == 'restrict_host_access' and action != e_errors.REJECT:
            action = None   # do nothing here
        if rc and fun and action:
            ticket["status"] = (e_errors.OK, None)
            if fun == 'restrict_version_access':
                #replace last argument with ticket
                #args.remove({})
                # for some reason discipline has begun to return a complete ticket as a
                # last argument on 05/10/2002 after update
                # that's why I excplicitely remove a 3rd argument
                del(args[2])
                args.append(ticket)
            elif fun == 'restrict_host_access':
                host_from_ticket = self.get_host_name_from_ticket(ticket)
                args.append(host_from_ticket)

            ret = apply(getattr(self,fun), args)
            if ret and (action in (e_errors.LOCKED, e_errors.IGNORE, e_errors.PAUSE, e_errors.NOWRITE, e_errors.REJECT)):
                _format = "access restricted for %s : library=%s family=%s requester:%s "
                Trace.log(e_errors.INFO, _format%(ticket["wrapper"]["fullname"],
                                                  ticket["vc"]["library"],
                                                  ticket["vc"]["file_family"],
                                                  ticket["wrapper"]["uname"]))
                if action in (e_errors.LOCKED, e_errors.NOWRITE, e_errors.REJECT):
                    ticket["status"] = (action, "Library manager is locked for external access")
                # it has been observerd that in multithreaded environment
                # ticket["r_a"] somehow gets modified
                # so to be safe restore  ticket["r_a"] just before sending
                ticket["r_a"] = saved_reply_address
                self.reply_to_caller(ticket)
                Trace.notify("client %s %s %s %s" % (host, work, ff, action))
                return


        # check if work is in the at mover list before inserting it
        for wt in self.work_at_movers.list:
            # 2 requests cannot have the same output file names
            if ((wt["wrapper"]['pnfsFilename'] == ticket["wrapper"]["pnfsFilename"]) and
                (wt["unique_id"] == ticket["unique_id"])):
                ticket['status'] = (e_errors.OK,"Operation in progress")
                # it has been observerd that in multithreaded environment
                # ticket["r_a"] somehow gets modified
                # so to be safe restore  ticket["r_a"] just before sending
                ticket["r_a"] = saved_reply_address
                self.reply_to_caller(ticket)
                _format = "write rq. is already in the at mover queue %s (%s) -> %s : library=%s family=%s requester:%s sg:%s"
                Trace.log(e_errors.INFO, _format%(ticket["wrapper"]["fullname"],
                                                  ticket["unique_id"],
                                                  ticket["wrapper"]["pnfsFilename"],
                                                  ticket["vc"]["library"],
                                                  ticket["vc"]["file_family"],
                                                  ticket["wrapper"]["uname"],
                                                  ticket['vc']["storage_group"]))
                Trace.log(e_errors.INFO, "CB ADDR %s PEND %s"%(ticket["callback_addr"], wt["callback_addr"]))
                return

        if self.keys.has_key('mover_port'):
             ticket['lm'] = {'address': (self.keys['hostip'], self.keys['mover_port'])}
        else:
            ticket['lm'] = {'address':self.server_address }
        # set up priorities
        ticket['encp']['basepri'],ticket['encp']['adminpri'] = self.pri_sel.priority(ticket)
	log_add_to_pending_queue(ticket['vc'])
	# put ticket into request queue
        rq, status = self.pending_work.put(ticket)
        ticket['status'] = (status, None)

        # it has been observerd that in multithreaded environment
        # ticket["r_a"] somehow gets modified
        # so to be safe restore  ticket["r_a"] just before sending
        ticket["r_a"] = saved_reply_address
        self.reply_to_caller(ticket) # reply now to avoid deadlocks

        if status == e_errors.OK:
            if not rq:
                _format = "write rq. is already in the queue %s (%s) -> %s : library=%s family=%s requester:%s volume_family:%s"
            else:
                _format = "write Q'd %s (%s) -> %s : library=%s family=%s requester:%s volume_family:%s"
            Trace.log(e_errors.INFO, _format%(ticket["wrapper"]["fullname"],
                                             ticket["unique_id"],
                                             ticket["wrapper"]["pnfsFilename"],
                                             ticket["vc"]["library"],
                                             ticket["vc"]["file_family"],
                                             ticket["wrapper"]["uname"],
                                             ticket['vc']["volume_family"]))

            Trace.notify("client %s %s %s %s" % (host, work, ff, 'queued'))

    def read_from_hsm(self, ticket):
        """
        Process client read work request.
        Check if it can be accepted and put into pending requests queue.
        This method is called within :class:`dispatching_worker.DispatchingWorker`

        :type ticket: :obj:`dict`
        :arg ticket: work request ticket
        """
        Trace.trace(self.my_trace_level+100, "read_from_hsm: ticket %s"%(ticket))

        saved_reply_address = ticket.get('r_a', None)
        ticket = self.verify_data_transfer_request(ticket)
        if not ticket:
            # ticket did not pass verification
            # client response has been sent by
            # verify_data_transfer_request()
            return

        method = ticket.get('method', None)
        if method and method == 'read_next': # this request must go directly to mover
            ticket['status'] = (e_errors.USERERROR, "Wrong method used %s"%(method,))
            # it has been observerd that in multithreaded environment
            # ticket["r_a"] somehow gets modified
            # so to be safe restore  ticket["r_a"] just before sending
            ticket["r_a"] = saved_reply_address
            self.reply_to_caller(ticket)
            return

        if ticket.has_key('mover'):
            Trace.log(e_errors.WARNING,'input ticket has key mover in it %s'%(ticket,))
            del(ticket['mover'])
        # data for Trace.notify
        host = ticket['wrapper']['machine'][1]
        work = 'read'
        vol = ticket['fc']['external_label']

        if self.lm_lock in (e_errors.LOCKED, e_errors.IGNORE, e_errors.PAUSE, e_errors.NOREAD, e_errors.BROKEN):
            if self.lm_lock in (e_errors.LOCKED, e_errors.NOREAD):
                ticket["status"] = (self.lm_lock, "Library manager is locked for external access")
            else:
                ticket["status"] = (e_errors.OK, None)
            # it has been observerd that in multithreaded environment
            # ticket["r_a"] somehow gets modified
            # so to be safe restore  ticket["r_a"] just before sending
            ticket["r_a"] = saved_reply_address
            self.reply_to_caller(ticket)
            Trace.notify("client %s %s %s %s" % (host, work, vol, self.lm_lock))
            return
        ## check if there are any additional restrictions
        rc, fun, args, action = self.restrictor.match_found(ticket)
        Trace.trace(self.my_trace_level+100,"read_from_hsm: match returned %s %s %s %s"% (rc, fun, args, action))
        if fun == 'restrict_host_access' and action != e_errors.REJECT:
            action = None    # do nothing here
        if rc and fun and action:
            ticket["status"] = (e_errors.OK, None)
            if fun == 'restrict_version_access':
                #replace last argument with ticket
                #args.remove({})
                # for some reason discipline has begun to return a complete ticket as a
                # last argument on 05/10/2002 after update
                # that's why I excplicitely remove a 3rd argument
                del(args[2])
                args.append(ticket)
            elif fun == 'restrict_host_access':
                host_from_ticket = self.get_host_name_from_ticket(ticket)
                args.append(host_from_ticket)
            ret = apply(getattr(self,fun), args)
            if ret and (action in (e_errors.LOCKED, e_errors.IGNORE, e_errors.PAUSE, e_errors.NOREAD, e_errors.REJECT)):
                _format = "access restricted for %s : library=%s family=%s requester:%s"
                Trace.log(e_errors.INFO, _format%(ticket['wrapper']['pnfsFilename'],
                                                  ticket["vc"]["library"],
                                                  ticket["vc"]["volume_family"],
                                                  ticket["wrapper"]["uname"]))
                if action in (e_errors.LOCKED, e_errors.NOREAD, e_errors.REJECT):
                    ticket["status"] = (action, "Library manager is locked for external access")
                # it has been observerd that in multithreaded environment
                # ticket["r_a"] somehow gets modified
                # so to be safe restore  ticket["r_a"] just before sending
                ticket["r_a"] = saved_reply_address
                self.reply_to_caller(ticket)
                Trace.notify("client %s %s %s %s" % (host, work, vol, action))
                return

        # check if this volume is OK
        # use vc subticket
        v = ticket['vc']

        if (v['system_inhibit'][0] == e_errors.NOACCESS or
            v['system_inhibit'][0] == e_errors.NOTALLOWED):
            # tape cannot be accessed, report back to caller and do not
            # put ticket in the queue
            ticket["status"] = (v['system_inhibit'][0], None)
            # it has been observerd that in multithreaded environment
            # ticket["r_a"] somehow gets modified
            # so to be safe restore  ticket["r_a"] just before sending
            ticket["r_a"] = saved_reply_address
            self.reply_to_caller(ticket)
            _format = "read request discarded for unique_id=%s : volume %s is marked as %s"
            Trace.log(e_errors.ERROR, _format%(ticket['unique_id'],
                                              ticket['fc']['external_label'],
                                              ticket["status"][0]))
            Trace.trace(self.my_trace_level,"read_from_hsm: volume has no access")
            Trace.notify("client %s %s %s %s" % (host, work, vol, 'rejected'))
            return

        #if not ticket.has_key('lm'):
        if self.keys.has_key('mover_port'):
             ticket['lm'] = {'address': (self.keys['hostip'], self.keys['mover_port'])}
        else:
            ticket['lm'] = {'address':self.server_address }


        # check if work is in the at mover list before inserting it
        _format = None
        for wt in self.work_at_movers.list:
            if wt["unique_id"] == ticket["unique_id"]:
                status = e_errors.OK
                ticket['status'] = (status, None)
                rq = None
                _format = "read rq. is already in the at mover queue %s (%s) -> %s : library=%s family=%s requester:%s"

                break
        else:
            # set up priorities
            ticket['encp']['basepri'],ticket['encp']['adminpri'] = self.pri_sel.priority(ticket)
	    log_add_to_pending_queue(ticket['vc'])
            if ('media_type' in ticket['vc']
                and ticket['vc']['media_type'] == 'disk'
                and ticket['fc']['package_id']):
                # replace external_label to allow processing of HAVE_BOUND disk mover requests
                 ticket['fc']['external_label'] =  ticket['fc']['package_id']
            # put ticket into request queue
            rq, status = self.pending_work.put(ticket)
            ticket['status'] = (status, None)

        # it has been observerd that in multithreaded environment
        # ticket["r_a"] somehow gets modified
        # so to be safe restore  ticket["r_a"] just before sending
        ticket["r_a"] = saved_reply_address
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        if status == e_errors.OK:
            if not rq:
                if not _format:
                    _format = "read rq. is already in the queue %s (%s) -> %s : library=%s family=%s requester:%s"
                file1 = ticket['wrapper']['fullname']
                file2 = ticket['wrapper']['pnfsFilename']
            else:
                _format = "read Q'd %s (%s) -> %s : library=%s family=%s requester:%s"
                file1 = ticket['wrapper']['pnfsFilename']
                file2 = ticket['wrapper']['fullname']

            # legacy encp ticket
            if not ticket['vc'].has_key('volume_family'):
                ticket['vc']['volume_family'] = ticket['vc']['file_family']

            Trace.log(e_errors.INFO, _format%(file1, ticket['unique_id'], file2,
                                             ticket["vc"]["library"],
                                             ticket["vc"]["volume_family"],
                                             ticket["wrapper"]["uname"]))
            Trace.notify("client %s %s %s %s" % (host, work, vol, 'queued'))

    ########################################
    # End data transfer requests
    ########################################

    ########################################
    # mover requests
    ########################################

    # mover_idle wrapper for threaded implementation
    def mover_idle(self, mticket):
        """
        Mover_idle wrapper for threaded implementation.
        This method is called within :class:`dispatching_worker.DispatchingWorker`

        :type mticket: :obj:`dict`
        :arg mticket: mover request
        """

        t=time.time()
        Trace.trace(5, "mover_idle: %s"%(mticket['mover'],))
        if self.lm_lock == e_errors.MOVERLOCKED:
            mticket['work'] = 'no_work'
            Trace.trace(5, "mover_idle: mover request in progress sending nowork %s"%(mticket,))
            self.reply_to_caller(mticket)
        else:
            if self.use_threads:
                if not self.mover_request_in_progress:
                    self.set_mover_request_in_progress(value=True)
                    self._mover_idle(mticket)
                    self.set_mover_request_in_progress(value=False)
                else:
                    mticket['work'] = 'no_work'
                    Trace.trace(5, "mover_idle: mover request in progress sending nowork %s"%(mticket,))
                    self.reply_to_caller(mticket)
            else:
               self._mover_idle(mticket)
        Trace.trace(7, "mover_idle:timing mover_idle %s %s %s"%
                    (mticket['mover'], time.time()-t, self.pending_work.queue_length))

    def _mover_idle(self, mticket):
        """
        Process mover_idle call.

        :type mticket: :obj:`dict`
        :arg mticket: mover request
        """

        Trace.trace(self.my_trace_level,"_mover_idle:IDLE RQ %s"%(mticket,))
        Trace.trace(self.my_trace_level,"_mover_idle:idle movers %s"%(self.idle_movers,))

        # thread safe
        saved_reply_address = mticket.get('r_a', None)
        nowork = {'work': 'no_work', 'r_a': saved_reply_address}
        self.known_volumes = {}
        self.add_idle_mover(mticket["mover"])
        # Mover is idle remove it from volumes_at_movers.
        # Library manager may not be able to respond to a mover
        # in time which results in the mover retry message.
        # The mover requests may also be kept in the udp buffer before
        # getting processed by the library manager.
        # Both cases resul in sending a work to the mover and then removing
        # work from the active list as a result of the second idle request
        # to avoid this problem  compare the time of the mover requst submission
        # and the time when the work for the same mover became active
        # This code requires a new key in the mover ticket
        if mticket.has_key("current_time"):
            mover = mticket['mover']
            if mover in self.volumes_at_movers.get_active_movers():
                # how idle mover can be in the active list?
                # continue checking. This check requires synchronization between LM and mover machines.
                if self.volumes_at_movers.at_movers[mover]['time_started'] > mticket['current_time']:
                    # idle request was issued before the request became active
                    # ignore this request, but send something to mover.
                    # If nothing is sent the mover may hang wating for the
                    # library manager reply in case when previous reply was lost (there were such cases)
                    Trace.log(e_errors.INFO, "Duplicate IDLE request. Will send blank reply to %s"%(mover,))
                    blank_reply = {'work': None, 'r_a': saved_reply_address}
                    self.reply_to_caller(blank_reply)
                    return
        self.volumes_at_movers.delete(mticket)

        if self.is_starting():
            # LM needs a certain startup delay before it
            # starts processing mover requests to update
            # its volumes at movers table
            self.reply_to_caller(nowork)
            return

        if self.lm_lock in (e_errors.PAUSE, e_errors.BROKEN):
            Trace.trace(self.my_trace_level,"mover_idle: LM state is %s no mover request processing" % (self.lm_lock,))
            self.reply_to_caller(nowork)
            return

        self.requestor = mticket

        # check if there is a work for this mover in work_at_movers list
        # it should not happen in a normal operations but it may when for
        # instance mover detects that encp is gone and returns idle or
        # mover crashes and then restarts

        # find mover in the work_at_movers
        found = 0
        Trace.trace(self.my_trace_level+1, "mover_idle: work_at_movers: %s" % (self.work_at_movers.list,))
        for wt in self.work_at_movers.list:
            Trace.trace(self.my_trace_level, "mover_idle:work_at_movers: mover %s id %s" % (wt['mover'], wt['unique_id']))
            if wt['mover'] == self.requestor['mover']:
                found = 1     # must do this. Construct. for...else will not
                              # do better
                break
        if found:
            mover_rq_unique_id = mticket.get('unique_id',None)
            work_at_mover_unique_id = wt.get('unique_id', None)
            method = wt.get('method', None)
            if method == 'read_tape_start':
                # this was a tape copy request
                # it requires a special way of comparing request ids
                if mover_rq_unique_id:
                    s1 = mover_rq_unique_id.split("-")
                    mover_rq_unique_id = string.join(s1[:-1], "-")
                s1 = work_at_mover_unique_id.split("-")
                work_at_mover_unique_id = string.join(s1[:-1], "-")
                Trace.trace(self.my_trace_level, "mover_idle: m_id %s w_id %s"%(mover_rq_unique_id,work_at_mover_unique_id))
            # check if it is a backed up request
            if ((mover_rq_unique_id and mover_rq_unique_id != work_at_mover_unique_id) and
                ( mticket["time_in_state"] < 60)): # allow 60 s for possible communication re-tries
                Trace.trace(self.my_trace_level+1,"mover_idle: found backed up mover %s" % (mticket['mover'],))
                Trace.trace(self.my_trace_level+1,"mover_idle: mover_rq_unique_id %s work_at_mover_unique_id %s"%(mover_rq_unique_id, work_at_mover_unique_id))
                self.reply_to_caller(nowork) # AM!!!!!
                return

            self.work_at_movers.remove(wt)
            _format = "Removing work from work at movers queue for idle mover. Work:%s mover:%s"
            Trace.log(e_errors.INFO, _format%(wt,mticket))

        start_t=time.time()
        rq, status = self.schedule(mticket)
        Trace.trace(self.my_trace_level,"mover_idle: SCHEDULE RETURNED %s %s"%(rq, status))
        Trace.trace(100, "mover_idle: SHEDULE, time in state %s"%(time.time()-start_t, ))

        # no work means we're done
        if status[0] == e_errors.NOWORK:
            ##Before actually saying we are done, if any volume assert
            # requests are pending, handle them.
            if self.volume_assert_list:
                for i in range(len(self.volume_assert_list)):
                    external_label = \
                         self.volume_assert_list[i]['vc'].get('external_label',
                                                              None)
                    if external_label:
                        if not self.volumes_at_movers.is_vol_busy(
                            external_label):
                            #Add some items to the dictionary.
                            self.volume_assert_list[i]['mover'] = \
                                                              mticket['mover']
                            #Update the dequeued time.
                            self.volume_assert_list[i]['times']['lm_dequeued'] = \
                                time.time()
                            #Add the volume and mover to the list of currently
                            # busy volumes and movers.
                            self.work_at_movers.append(
                                self.volume_assert_list[i])
                            self.volumes_at_movers.put(mticket)
                            # remove this mover from idle_movers list
                            self.remove_idle_mover(mticket["mover"])

                            #Record this action in log file.
                            Trace.log(e_errors.INFO,
                                      "IDLE:sending %s to mover" %
                                      (self.volume_assert_list[i],))

                            #Tell the mover it has something to do.
                            # it has been observerd that in multithreaded environment
                            # ticket["r_a"] somehow gets modified
                            # so to be safe restore  ticket["r_a"] just before sending
                            self.volume_assert_list[i]["r_a"] = saved_reply_address
                            self.reply_to_caller(self.volume_assert_list[i])
                            #Remove the job from the list of volumes to check.
                            del self.volume_assert_list[i]
                            break
                        else:
                            self.reply_to_caller(nowork)
                    else:
                        self.reply_to_caller(nowork)
            else:
                self.reply_to_caller(nowork)
            return

        if status[0] != e_errors.OK:
            self.reply_to_caller(nowork)
            Trace.log(e_errors.ERROR,"mover_idle: assertion error w=%s ticket=%s"%(rq, mticket))
            raise AssertionError

        # ok, we have some work - try to bind the volume
        w = rq.ticket
        if self.mover_type(mticket) == 'DiskMover':
            # volume clerk may not return external_label in vc ticket
            # for write requests
            if not w["vc"].has_key("external_label"):
                w["vc"]["external_label"] = None
                #w["vc"]["wrapper"] = "null"

        # reply now to avoid deadlocks
        _format = "%s work on vol=%s mover=%s requester:%s"
        Trace.log(e_errors.INFO, _format%\
                       (w["work"],
                       w["fc"]["external_label"],
                       mticket["mover"],
                       w["wrapper"]["uname"]))
        if w.has_key('reject_reason'): del(w['reject_reason'])
        self.pending_work.delete(rq)
        w['times']['lm_dequeued'] = time.time()
        # set the correct volume family for write request
        if w['work'] == 'write_to_hsm':
            initial_file_family = w['vc']['file_family'] # to deal with ephemeral FF
            # update volume info
            vol_info = self.inquire_vol(w["fc"]["external_label"], w['vc']['address'])
            if vol_info['status'][0] == e_errors.OK:
                w['vc'].update(vol_info)
            if initial_file_family == 'ephemeral':
                # set the correct volume family for write reques
                w['vc']['volume_family'] = volume_family.make_volume_family(w['vc']['storage_group'],
                                                                            w['fc']['external_label'],
                                                                            w['vc']['wrapper'])
        w['vc']['file_family'] = volume_family.extract_file_family(w['vc']['volume_family'])

        w['mover'] = mticket['mover']
        w['mover_type'] = self.mover_type(mticket) # this is needed for packaged files processing
        if w['mover_type'] == "DiskMover" and w['work'] == 'read_from_hsm':
            # save external_label (package_id)
            label = w['fc']['external_label']
            fcc = file_clerk_client.FileClient(self.csc)
            # update file info
            rc = fcc.bfid_info(w['fc']['bfid'])
            if rc['status'][0] == e_errors.OK:
                w['fc'].update(rc)
                # restore external_label
                w['fc']['external_label'] = label
            else:
                Trace.log(e_errors.ERROR, "bfid_info %s"%(rc,))
                self.reply_to_caller(nowork)
                return
            if w['fc']['cache_status'] == file_cache_status.CacheStatus.PURGED:
                # open_bitfile tells file clerk to initiate disk cache file staging if it is not in the cache.
                # The mover to which this work (w) is submitted waits until file is cached
                # and transfers file to the client.
                # If file is a part of a package open the corresponding package istead of opening a requested file.
                # This guaraties that the files in the package will be opened syncronously.
                bfid_to_open = self.is_packaged(w) # package id
                Trace.trace(self.my_trace_level+1, "_mover_idle: bfid_to_open %s"%(bfid_to_open,))

                if not bfid_to_open:
                    bfid_to_open = w['fc']['bfid']
                    rc = fcc.open_bitfile(bfid_to_open)
                else:
                    rc = fcc.open_bitfile_for_package(bfid_to_open)

                if rc['status'][0] != e_errors.OK:
                    Trace.log(e_errors.ERROR, "open_bitfile returned %s"%(rc,))
                    self.reply_to_caller(nowork)
                    return

        Trace.trace(self.my_trace_level, "mover_idle: File Family = %s" % (w['vc']['file_family']))

	log_add_to_wam_queue(w['vc'])
        Trace.trace(self.my_trace_level, "mover_idle: appending to work_at_movers %s"%(w,))
        if not w in self.work_at_movers.list:
            self.work_at_movers.append(w)
        else:
            Trace.trace(self.my_trace_level, "mover_idle: work is already in work_at_movers")
        #work = string.split(w['work'],'_')[0]
        Trace.log(e_errors.INFO,"IDLE:sending %s to mover"%(w,))
        #Thread safe
        w['r_a'] = saved_reply_address
        # remove this mover from idle_movers list
        self.remove_idle_mover(mticket["mover"])
        if w.has_key("work_in_work"):
            w['work'] = w['work_in_work']
        self.reply_to_caller(w)

        ### XXX are these all needed?
        mticket['external_label'] = w["fc"]["external_label"]
        mticket['current_location'] = None
        mticket['volume_family'] =  w['vc']['volume_family']
        mticket['unique_id'] =  w['unique_id']
        mticket['status'] =  (e_errors.OK, None)
        mticket['state'] = 'SETUP'
        mticket['time_in_state'] = 0.

        # update volume status
        # get it directly from volume clerk as mover
        # in the idle state does not have it
        if self.mover_type(mticket) == 'DiskMover':
            mticket['volume_status'] = (['none', 'none'], ['none', 'none'])
        else:
            Trace.trace(self.my_trace_level+1,"mover_idle:inquire_vol")

            vol_info = self.inquire_vol(mticket['external_label'], w['vc']['address'])
            mticket['volume_status'] = (vol_info.get('system_inhibit',['Unknown', 'Unknown']),
                                        vol_info.get('user_inhibit',['Unknown', 'Unknown']))
            if "Unknown" in mticket['volume_status'][0] or "Unknown" in mticket['volume_status'][1]:
                # sometimes it happens: why?
                Trace.trace(e_errors.ERROR,"mover_idle:Unknown! %s"%(vol_info,))

        Trace.trace(self.my_trace_level+1,"mover_idle: Mover Ticket %s" % (mticket,))
        self.volumes_at_movers.put(mticket)
        Trace.trace(self.my_trace_level+1,"mover_idle:IDLE:postponed%s %s"%(self.postponed_requests.sg_list,self.postponed_requests.rq_list))
        if self.postponed_rq:
            self.postponed_requests.update(self.postponed_sg, 1)

    def mover_busy(self, mticket):
        """
        Mover busy wrapper for threaded implementation.
        This method is called within :class:`dispatching_worker.DispatchingWorker`

        :type mticket: :obj:`dict`
        :arg mticket: mover request
        """

        """
        Leave the following commnted
        if self.use_threads:
            if self.mover_request_in_progress == False:
               self.set_mover_request_in_progress(value=True)
               self._mover_busy(mticket)
               self.set_mover_request_in_progress(value=False)
            else:
                mticket['work'] = 'no_work'
                self.reply_to_caller(mticket)
        else:
            self._mover_busy(mticket)
        """
        self._mover_busy(mticket)

    def _mover_busy(self, mticket):
        """
        Process mover_busy call.
        Mover is busy - update :obj:`LibraryManagerMethods.volumes_at_movers`

        :type mticket: :obj:`dict`
        :arg mticket: mover request
        """

        Trace.trace(self.my_trace_level,"_mover_busy: BUSY RQ %s"%(mticket,))
        library = mticket.get('library', None)
        if library and library != self.name.split(".")[0] and not self.share_movers:
            # this mover is currently assigned to another library
            # remove this mover from volumes_at_movers
            movers = self.volumes_at_movers.get_active_movers()
            movers_to_delete = []
            for mv in movers:
                if mticket['mover'] == mv['mover']:
                    movers_to_delete.append(mv)
            if movers_to_delete:
                for mv in movers_to_delete:
                    Trace.trace(self.my_trace_level+1, "mover_busy: removing from at movers %s"%(mv,))
                    self.volumes_at_movers.delete(mv)
            return
        state = mticket.get('state', None)
        if state == 'IDLE':
            # mover dismounted a volume on a request to mount another one
            self.volumes_at_movers.delete(mticket)
        else:
            if ("Unknown" in mticket['volume_status'][0] or "Unknown" in mticket['volume_status'][1]):
                # Mover did not return a "good" status for this volume
                # Try to get volume info.
                # If it fails then log this and do not update at_movers list
                volume_clerk_address = mticket.get("volume_clerk", None)
                vol_info = self.inquire_vol(mticket['external_label'], volume_clerk_address)
                if vol_info['status'][0] == e_errors.OK:
                    mticket['volume_family'] = vol_info['volume_family']
                    mticket['volume_status'] = (vol_info.get('system_inhibit',['none', 'none']),
                                                vol_info.get('user_inhibit',['none', 'none']))

                    Trace.trace(self.my_trace_level, "mover_busy: updated mover ticket: %s"%(mticket,))
                    self.volumes_at_movers.put(mticket)
                else:
                   Trace.log(e_errors.ERROR, "mover_busy: can't update volume info, status:%s"%
                               (vol_info['status'],))
            else:
                Trace.trace(self.my_trace_level, "mover_busy: updated mover ticket: %s"%(mticket,))
                self.volumes_at_movers.put(mticket)

        # do not reply to mover as it does not
        # expect reply for "mover_busy" work

    def mover_bound_volume(self, mticket):
        """
        Mover Bound Volume wrapper for threaded implementation
        This method is called within :class:`dispatching_worker.DispatchingWorker`

        :type mticket: :obj:`dict`
        :arg mticket: mover request
        """

        t=time.time()
        Trace.trace(5, "mover_bound_volume %s"%(mticket['mover'],))
        if self.use_threads:
            if not self.mover_request_in_progress:
               self.set_mover_request_in_progress(value=True)
               self._mover_bound_volume(mticket)
               self.set_mover_request_in_progress(value=False)
            else:
                mticket['work'] = 'no_work'
                self.reply_to_caller(mticket)
        else:
            self._mover_bound_volume(mticket)
        Trace.trace(7, "mover_bound_volume: timing mover_bound_volume %s %s %s"%
                    (mticket['mover'], time.time()-t, self.pending_work.queue_length))

    def _mover_bound_volume(self, mticket):
        """
        Process mover_bound_volume call.

        :type mticket: :obj:`dict`
        :arg mticket: mover request
        """

        Trace.trace(self.my_trace_level, "mover_bound_volume for %s: request: %s"%(mticket['mover'],mticket))
        Trace.trace(self.my_trace_level,"_mover_bound_volume:idle movers %s"%(self.idle_movers,))
        # thread safe
        saved_reply_address = mticket.get('r_a', None)
        nowork = {'work': 'no_work', 'r_a': saved_reply_address}

        library = mticket.get('library', None)
        if library and library != self.name.split(".")[0]:
            self.reply_to_caller(nowork)
            return

        if not mticket['external_label']:
            Trace.log(e_errors.ERROR,"mover_bound_volume: mover request with suspicious volume label %s" %
                      (mticket['external_label'],))
            self.reply_to_caller(nowork)
            return

        last_work = mticket['operation']
        self.known_volumes = {}
        # Library manager may not be able to respond to a mover
        # in time which results in the mover retry message.
        # The mover requests may also be kept in the udp buffer before
        # getting processed by the library manager.
        # Both cases result in reproting  a work to the mover and then removing
        # work from the active list as a result of the second idle request
        # to avoid this problem  compare the time of the mover requst submission
        # and the time when the work for the same mover became active
        # This code requires a new key in the mover ticket
        if mticket.has_key("current_time"):
            mover = mticket['mover']
            Trace.trace(self.my_trace_level, "_mover_bound_volume: active movers %s"%(self.volumes_at_movers.at_movers,))
            if mover in self.volumes_at_movers.get_active_movers():
                # continue checking. This check requires synchronization between LM and mover machines.
                if self.volumes_at_movers.at_movers[mover]['time_started'] >= mticket['current_time']:
                    # request was issued before the request became active
                    # ignore this request, but send something to mover.
                    # If nothing is sent the mover may hang wating for the
                    # library manager reply in case when previous reply was lost (there were such cases)
                    Trace.log(e_errors.INFO, "Duplicate HAVE_BOUND request will be ignored for %s"%(mover,))
                    blank_reply = {'work': None, 'r_a': saved_reply_address}
                    self.reply_to_caller(blank_reply)
                    return

        if (not mticket['volume_family'] or  ("Unknown" in mticket['volume_status'][0] or "Unknown" in mticket['volume_status'][1])):
            # Mover restarted with bound volume and it has not
            # all the volume info (volume_family is None).
            # Or mover did not return a "good" status for this volume
            # Try to get volume info.
            # If it fails then log this and send "nowork" to mover

            if self.mover_type(mticket) == 'DiskMover':
                mticket['volume_status'] = (['none', 'none'], ['none', 'none'])
            else:
                volume_clerk_address = mticket.get("volume_clerk", None)
                vol_info = self.inquire_vol(mticket['external_label'], volume_clerk_address)
                if vol_info['status'][0] == e_errors.OK:
                    mticket['volume_family'] = vol_info['volume_family']
                    mticket['volume_status'] = (vol_info.get('system_inhibit',['none', 'none']),
                                                vol_info.get('user_inhibit',['none', 'none']))

                    Trace.trace(self.my_trace_level, "mover_bound_volume: updated mover ticket: %s"%(mticket,))
                else:
                   Trace.log(e_errors.ERROR, "mover_bound_volume: can not update volume info, status:%s"%
                               (vol_info['status'],))
                   self.reply_to_caller(nowork)
                   return

        #transfer_deficiency = mticket.get('transfer_deficiency', 1)
        sg = volume_family.extract_storage_group(mticket['volume_family'])
        self.postponed_requests.update(sg, 1)

        if self.is_starting():
            # LM needs a certain startup delay before it
            # starts processing mover requests to update
            # its volumes at movers table
            self.reply_to_caller(nowork)
            return
        # just did some work, delete it from queue
        w = self.get_work_at_movers(mticket['external_label'], mticket['mover'])

        current_priority = mticket.get('current_priority', None)
        if w:
            # check if it is a backed up request
            if mticket['unique_id'] and mticket['unique_id'] != w['unique_id']:
                if mticket.has_key("current_time"):
                    mover = mticket['mover']
                    if self.volumes_at_movers.at_movers[mover]['time_started'] ==  mticket['current_time']:
                        Trace.log(e_errors.INFO, "Duplicate MOVER_BOUND request will be ignored for %s"%(mover,))
                        blank_reply = {'work': None, 'r_a': saved_reply_address}
                        self.reply_to_caller(blank_reply)
                        return
                        #
                Trace.trace(self.my_trace_level+1,"_mover_bound_volume: found backed up mover %s " % (mticket['mover'], ))
                Trace.trace(self.my_trace_level+1,"_mover_bound_volume %s %s"%(mticket['unique_id'],  w['unique_id']))
                self.reply_to_caller(nowork)  #AM !!!!!!!
                return
            Trace.trace(self.my_trace_level+1,"_mover_bound_volume: removing %s  from the queue"%(w,))
            # file family may be changed by VC during the volume
            # assignment. Set file family to what VC has returned
            if mticket['external_label']:
                w['vc']['volume_family'] = mticket['volume_family']
            self.work_at_movers.remove(w)

        # put volume information
        # if this mover is already in volumes_at_movers
        # it will not get updated
        self.volumes_at_movers.put(mticket)

        if self.lm_lock in (e_errors.PAUSE, e_errors.BROKEN, e_errors.MOVERLOCKED):
            Trace.trace(self.my_trace_level+1,"_mover_bound_volume: LM state is %s no mover request processing" % (self.lm_lock,))
            self.reply_to_caller(nowork)
            return
        # see if this volume will do for any other work pending
        rq, status = self.next_work_this_volume(mticket['external_label'], mticket['volume_family'],
                                                last_work, mticket,
                                                mticket['current_location'], priority=current_priority)
        Trace.trace(self.my_trace_level+1, "_mover_bound_volume: next_work_this_volume returned: %s %s"%(rq,status))
        if status[0] == e_errors.OK:
            w = rq.ticket
            if self.mover_type(mticket) == 'DiskMover':
                # volume clerk may not return external_label in vc ticket
                # for write requests
                if not w["vc"].has_key("external_label"):
                    w["vc"]["external_label"] = None
                    #w["vc"]["wrapper"] = "null"

            _format = "%s next work on vol=%s mover=%s requester:%s"
            try:
                Trace.log(e_errors.INFO, _format%(w["work"],
                                                 w["vc"]["external_label"],
                                                 mticket["mover"],
                                                 w["wrapper"]["uname"]))
            except KeyError:
                Trace.log(e_errors.ERROR, "mover_bound_volume: Bad ticket: %s"%(w,))
                self.reply_to_caller(nowork)
                return
            w['times']['lm_dequeued'] = time.time()
            if w.has_key('reject_reason'): del(w['reject_reason'])
            Trace.log(e_errors.INFO,"HAVE_BOUND:sending %s %s to mover %s %s DEL_DISM %s"%
                      (w['work'],w['wrapper']['pnfsFilename'], mticket['mover'],
                       mticket['address'], w['encp']['delayed_dismount']))
            Trace.trace(self.my_trace_level, "HAVE_BOUND: Ticket %s"%(w,))
            self.pending_work.delete(rq)
            Trace.trace(self.my_trace_level+1, "_mover_bound_volume: HAVE_BOUND: DELETED")
            w['times']['lm_dequeued'] = time.time()
            w['mover'] = mticket['mover']
            w['mover_type'] = self.mover_type(mticket) # this is needed for packaged files processing

            if w['work'] == 'write_to_hsm':
                # update volume info
                vol_info = self.inquire_vol(w["fc"]["external_label"], w['vc']['address'])
                if vol_info['status'][0] == e_errors.OK:
                    w['vc'].update(vol_info)
	    log_add_to_wam_queue(w['vc'])
            #self.work_at_movers.append(w)
            Trace.trace(self.my_trace_level+1, "mover_bound_volume: appending to work_at_movers %s"%(w,))
            if not w in self.work_at_movers.list:
                self.work_at_movers.append(w)
            else:
                Trace.trace(self.my_trace_level, "mover_bound_volume: work is already in work_at_movers")
            Trace.log(e_errors.INFO,"HAVE_BOUND:sending %s to mover"%(w,))
            # thread safe
            w['r_a'] = saved_reply_address
            Trace.trace(self.my_trace_level, "_mover_bound_volume: HAVE_BOUND: Sending")
            if w.has_key("work_in_work"):
                w['work'] = w['work_in_work']

            self.reply_to_caller(w)
            Trace.trace(self.my_trace_level, "_mover_bound_volume: HAVE_BOUND: Sent")

            # if new work volume is different from mounted
            # which may happen in case of high pri. work
            # update volumes_at_movers
            if w["vc"]["external_label"] != mticket['external_label']:
                # update mticket volume status
                # perhaps this is a hipri request forcing a tape replacement
                mticket['external_label'] = w["vc"]["external_label"]
                # update volume status
                # get it directly from volume clerk as mover
                # in the idle state does not have it
                if self.mover_type(mticket) == 'DiskMover':
                    mticket['volume_status'] = (['none', 'none'], ['none', 'none'])
                else:
                    vol_info = self.inquire_vol(mticket['external_label'], w['vc']['address'])
                    if vol_info['status'][0] != e_errors.OK:
                        Trace.log(e_errors.ERROR, "mover_bound_volume 2: can not update volume info, status:%s"%
                                  (vol_info['status'],))
                    mticket['volume_status'] = (vol_info.get('system_inhibit',['Unknown', 'Unknown']),
                                                vol_info.get('user_inhibit',['Unknown', 'Unknown']))
                    if "Unknown" in mticket['volume_status'][0] or "Unknown" in mticket['volume_status'][1]:
                        # sometimes it happens: why?
                        Trace.trace(e_errors.ERROR,"mover_bund_volume:Unknown! %s"%(vol_info,))

            # create new mover_info
            mticket['status'] = (e_errors.OK, None)

            # legacy encp ticket
            if not w['vc'].has_key('volume_family'):
                w['vc']['volume_family'] = w['vc']['file_family']

            mticket['volume_family'] = w['vc']['volume_family']
            mticket['unique_id'] = w['unique_id']
            mticket['state'] = 'SETUP'
            mticket['time_in_state'] = 0.
            Trace.trace(self.my_trace_level,"mover_bound_volume: mover %s label %s vol_fam %s" %
                        (mticket['mover'], mticket['external_label'],
                         mticket['volume_family']))

            self.volumes_at_movers.put(mticket)

        # if the pending work queue is empty, then we're done
        elif  (status[0] == e_errors.NOWORK or
               status[0] == e_errors.VOL_SET_TO_FULL or
               status[0] == 'full'):
            ret_ticket = {'work': 'no_work'}
            if self.mover_type(mticket) == 'DiskMover':
                # No work for this package.
                # To avoid delay between mover solicitation for next request
                # pick up any request as if disk mover was idle.
                Trace.trace(self.my_trace_level, "no work for bound volume, will try idle request")
                self._mover_idle(mticket)
                return
            if (status[0] == e_errors.VOL_SET_TO_FULL or
                status[0] == 'full'):
                # update at_movers information
                vol_stat = mticket['volume_status']
                s0 = [vol_stat[0][0],status[0]]
                s1 = vol_stat[1]
                mticket['volume_status'] = (s0, s1)
                Trace.trace(self.my_trace_level, "mover_bound_volume: update at_movers %s"%(mticket['volume_status'],))
                self.volumes_at_movers.put(mticket)
                ret_ticket = {'work':'update_volume_info',
                              'external_label': mticket['external_label']
                              }
            # do not dismount
            # thread safe
            ret_ticket['r_a'] = saved_reply_address
            self.reply_to_caller(ret_ticket)
            return
        else:
            Trace.log(e_errors.ERROR,"HAVE_BOUND: .next_work_this_volume returned %s:"%(status,))
            self.reply_to_caller(nowork)
            return
        Trace.trace(self.my_trace_level, "mover_bound_volume: DONE")

    def mover_error(self, mticket):
        """
        Process mover_error call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`

        :type mticket: :obj:`dict`
        :arg mticket: mover request
        """

        Trace.log(e_errors.ERROR,"MOVER ERROR RQ %s"%(mticket,))
        Trace.trace(self.my_trace_level, "mover_error: %s"%(mticket,))
        library = mticket.get('library', None)
        if library and library != self.name.split(".")[0]:
            return
        if mticket["state"] == "IDLE":
            self.add_idle_mover(mticket["mover"])
            self.volumes_at_movers.delete(mticket)
        else:
            # just for a case when for some reason
            # mover error request comes with mover state != IDLE
            self.remove_idle_mover(mticket["mover"])
            self.volumes_at_movers.put(mticket)
        Trace.trace(self.my_trace_level,"mover_error:idle movers %s"%(self.idle_movers,))

        # get the work ticket for the volume
        w = {}
        if mticket['external_label']:
            w = self.get_work_at_movers(mticket['external_label'], mticket['mover'])
        if w == {}:
            # Try to get work by mover name.
            # This may be a case for the volume preempted with admin priority request.
            # In this case external label from mover and external label in at_movers list
            # may be different.
            w = self.get_work_at_movers_m(mticket['mover'])
        Trace.trace(self.my_trace_level,"mover_error: work_at_movers %s"%(w,))
        if w:
            self.work_at_movers.remove(w)
        if ((mticket['state'] == mover_constants.OFFLINE) or # mover finished request and went offline
            (((mticket['status'][0] == e_errors.ENCP_GONE) or
              (mticket['status'][0] == e_errors.ENCP_STUCK) or
              (mticket['status'][0] == e_errors.DISMOUNTFAILED)) and # preempted volume dismount failed
             mticket['state'] != 'HAVE_BOUND')):
            rc = self.volumes_at_movers.delete(mticket)
            Trace.trace(self.my_trace_level,"mover_error: volumes_at_movers.delete returned %s"%(rc,))
            if mticket['status'][0] == e_errors.DISMOUNTFAILED:
                if rc != 0:
                    # check what kind of request was the last request, sent to this mover
                    if w:
                        admin_pri = w.get('encp',{}).get('adminpri', -1)
                        Trace.trace(self.my_trace_level,"mover_error: admin_pri %s"%(admin_pri,))
                        if admin_pri > 0:
                            # There was an attempt to preempt a mounted volume
                            # with admin. priority request.
                            # This is why the volume returned by mover
                            # was not equal to the volume in at_movers list
                            # and self.volumes_at_movers.delete has not succeded
                            # (see volumes_at_movers.delete).
                            # Try to delete mover from the at_movers list.
                            rc = self.volumes_at_movers.sg_vf.delete_mover(mticket['mover'])
                            Trace.trace(self.my_trace_level,"mover_error: volumes_at_movers.sg_vf.delete_mover returned %s"%(rc,))
                # put back the actual mover, tape combination
                self.volumes_at_movers.put(mticket)
            return
        if mticket.has_key('returned_work') and mticket['returned_work']:
            # put this ticket back into the pending queue
            # if work is write_to_hsm remove currently assigned volume label
            # because later it may be different
            d= mticket['returned_work']
            if d['work'] == 'write_to_hsm':
                if d['fc'].has_key('external_label'): del(d['fc']['external_label'])
                if d['vc'].has_key('external_label'): del(d['vc']['external_label'])

            del(mticket['returned_work']['mover'])
            Trace.trace(self.my_trace_level, "mover_error: put returned work back to pending queue %s"%
                        (mticket['returned_work'],))
            rq, status = self.pending_work.put(mticket['returned_work'])
            # return

        # update suspected volume list if error source is ROBOT or TAPE
        error_source = mticket.get('error_source', 'none')
        vol_status = mticket.get('volume_status', 'none')
        if ((error_source in ("ROBOT", "TAPE")) or
            mticket['status'][0] == e_errors.POSITIONING_ERROR): # bugzilla 947
            # Put volume into suspect volume list if there is
            # a positioning error. This error category is "DRIVE" in error_source, but for
            # older mover.py versions it was not set.
            # This is why e_errors.POSITIONING_ERROR match in status is used.
            # If volume is not put into suspect volume list
            # may cause lots of drives set offline by one defective tape.
            if vol_status and vol_status[0][0] == 'none':
                vol = self.update_suspect_vol_list(mticket['external_label'],
                                                   mticket['mover'])
                Trace.log(e_errors.INFO,"mover_error updated suspect volume list for %s"%(mticket['external_label'],))
                if vol:
                    # need a special processing for FTT_EBLANK. For 9940A tape drives
                    # it is mainly a firmware bug, but we need to make tape to not
                    # go NOACCESS in such a case.
                    ftt_eblank_error = (mticket['status'][0] == e_errors.READ_ERROR
                                        and mticket['status'][1] and
                                        ((mticket['status'][1] == 'FTT_EBLANK') or mticket['status'][1] == 'FTT_SUCCESS'))
                    if ((len(vol['movers']) >= self.max_suspect_movers and not ftt_eblank_error) or
                        (len(vol['movers']) >= self.max_suspect_movers + self.blank_error_increment and ftt_eblank_error)):

                        if w:
                            w['status'] = (e_errors.NOACCESS, None)

                        # set volume as noaccess
                        if mticket.has_key('volume_clerk'):
                            if mticket['volume_clerk'] == None:
                                # mover starting, no volume info
                                return
                            self.set_vcc(mticket['volume_clerk'])
                            #self.vcc = volume_clerk_client.VolumeClerkClient(self.csc,
                            #                                                 server_address=mticket['volume_clerk'])
                        else:
                            self.vcc = volume_clerk_client.VolumeClerkClient(self.csc)
                        self.vcc.set_system_noaccess(mticket['external_label'], timeout=self.volume_clerk_to, retry=self.volume_clerk_retry)
                        Trace.alarm(e_errors.ERROR,
                                    "Mover error (%s) caused volume %s to go NOACCESS"%(mticket['mover'],
                                                                                   mticket['external_label']))
                        # set volume as read only
                        #v = self.vcc.set_system_readonly(w['fc']['external_label'], timeout=5, retry=2)
                        label = mticket['external_label']

                        #remove entry from suspect volume list
                        self.suspect_volumes.remove(vol)
                        Trace.trace(self.my_trace_level,"mover_error: removed from suspect volume list %s"%(vol,))

                        self.flush_pending_jobs(e_errors.NOACCESS, label)
                else:
                    pass

    ########################################
    # End mover requests
    ########################################

    ########################################
    # Client service requests
    ########################################

    # body of getwork to run either in thread or as a function call
    def __getwork(self,ticket):
        try:
            control_socket,data_socket = self.get_user_sockets(ticket)
            if not control_socket:
                return 1
            # make it thread safe
            rticket = {}
            rticket['r_a'] = ticket.get('r_a', None)
            rticket["status"] = (e_errors.OK, None)
            rticket["at movers"] = self.work_at_movers.list
            adm_queue, write_queue, read_queue = self.pending_work.get_queue()
            rticket["pending_work"] = adm_queue + write_queue + read_queue
            callback.write_tcp_obj_new(data_socket,rticket)
            data_socket.close()
            callback.write_tcp_obj_new(control_socket,ticket)
            control_socket.close()
            return 0
        except:
            return 1


    def getwork(self,ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Sends list of pending and active works to the caller.

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'getwork'

        """

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.do_fork:
            fp = self.fork()
            if fp != 0:
                return
            rc = self.__getwork(ticket)
            os._exit(rc)
        else:
            dispatching_worker.run_in_thread('GetWork', self.__getwork, args=(ticket,))


    def print_queue(self, ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Sends list of pending requests to STDOUT.

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'print_queue'

        """
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        if self.do_fork:
            fp = self.fork()
            if fp != 0:
                return
            try:
                self.pending_work.wprint()

            except:
                pass
            os._exit(0)
        else:
            dispatching_worker.run_in_thread('Wprint', self.pending_work.wprint)

    # body of getworks_sorted to run either in thread or as a function call
    def __getworks_sorted(self,ticket):
        rc = 0
        try:
            control_socket,data_socket = self.get_user_sockets(ticket)
            if not control_socket:
                return 1
            rticket = {}
            rticket["status"] = (e_errors.OK, None)
            rticket["at movers"] = self.work_at_movers.list
            adm_queue, write_queue, read_queue = self.pending_work.get_queue()
            rticket["pending_works"] = {'admin_queue': adm_queue,
                                        'write_queue': write_queue,
                                        'read_queue':  read_queue,
                                        }
            callback.write_tcp_obj_new(data_socket,rticket)
            data_socket.close()
            callback.write_tcp_obj_new(control_socket,ticket)
            control_socket.close()
        except:
            rc = 1
        return rc


    def getworks_sorted(self,ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Sends sorted list of pending and active works to the caller.

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'getworks_sorted'

        """

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.do_fork:
            pid=self.fork()
            if pid != 0:
                return
            rc = self.__getworks_sorted(ticket)
            os._exit(rc)
        else:
            dispatching_worker.run_in_thread('GetWorks_Sorted', self.__getworks_sorted, args=(ticket,))


   #body of get_asserts to run either in thread or as a function call
    def __get_asserts(self, ticket):
        rc = 0
        try:
            control_socket,data_socket = self.get_user_sockets(ticket)
            if not control_socket:
                return 1
            rticket = {}
            rticket["status"] = (e_errors.OK, None)
            rticket['pending_asserts'] = self.volume_assert_list
            callback.write_tcp_obj_new(data_socket,rticket)
            data_socket.close()
            callback.write_tcp_obj_new(control_socket,ticket)
            control_socket.close()
        except:
            rc = 1
        return rc

    def get_asserts(self, ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Sends sorted list of pending volume assert works to the caller.

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'get_asserts'

        """

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.do_fork:
            pid=self.fork()
            if pid != 0:
                return
            rc = self.__get_asserts(ticket)
            os._exit(rc)
        else:
            dispatching_worker.run_in_thread('GetAsserts', self.__get_asserts, args=(ticket,))


    # body of get_suspect_volumes to run either in thread or as a function call
    def __get_suspect_volumes(self,ticket):
        rc = 0
        try:
            control_socket,data_socket = self.get_user_sockets(ticket)
            if not control_socket:
                return 1
            rticket = {}
            rticket['r_a'] = ticket.get('r_a', None)
            rticket["status"] = (e_errors.OK, None)
            rticket["suspect_volumes"] = self.suspect_volumes.list
            callback.write_tcp_obj_new(data_socket,rticket)
            data_socket.close()
            callback.write_tcp_obj_new(control_socket,ticket)
            control_socket.close()
        except:
            return 1
        return rc

    # get list of suspected volumes
    def get_suspect_volumes(self,ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Sends list of suspect volume assert works to the caller.

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'get_suspect_volumes'
        """

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.do_fork:
            pid = self.fork()
            if pid != 0:
                return
            rc = self.__get_suspect_volumes(ticket)
            os._exit(rc)
        else:
            dispatching_worker.run_in_thread('Get_Suspect_Volumes', self.__get_suspect_volumes, args=(ticket,))

    def get_user_sockets(self, ticket):
        """
        Get a port for the data transfer.
        Tell the user I'm your library manager and here's your ticket.
        Used for delivering replies over TCP (big messages).

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request
        """

        library_manager_host, library_manager_port, listen_socket =\
                              callback.get_callback()
        listen_socket.listen(4)
        ticket["library_manager_callback_addr"] = (library_manager_host, library_manager_port)
        control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        control_socket.connect(ticket['callback_addr'])
        callback.write_tcp_obj_new(control_socket, ticket)
        r,w,x = select.select([listen_socket], [], [], 15)
        if not r:
            listen_socket.close()
            return None, None
        data_socket, address = listen_socket.accept()
        if not hostaddr.allow(address):
            data_socket.close()
            listen_socket.close()
            return None, None
        listen_socket.close()
        return control_socket, data_socket

    def remove_work(self, ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Remove work specified by its unique id from list of pending works

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'remove_work'
        """

        rq = self.pending_work.find(ticket["unique_id"])
        if not rq:
            ticket["status"] = (e_errors.NOWORK,"No such work")
            self.reply_to_caller(ticket)
        else:
            self.pending_work.delete(rq)
            _format = "Request:%s deleted. Complete request:%s"
            Trace.log(e_errors.INFO, _format % (rq.unique_id, rq))
            ticket["status"] = (e_errors.OK, "Work deleted")
            self.reply_to_caller(ticket)

    def change_priority(self, ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Change priority of work specified by its unique id in the list of pending works

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'change_priority'
        """

        rq = self.pending_work.find(ticket["unique_id"])
        if not rq:
            ticket["status"] = (e_errors.NOWORK,"No such work")
            self.reply_to_caller(ticket)
            return
        ret = self.pending_work.change_pri(rq, ticket["priority"])
        if not ret:
            ticket["status"] = (e_errors.NOWORK, "Attempt to set wrong priority")
            self.reply_to_caller(ticket)
        else:
            _format = "Changed priority to:%s Complete request:%s"
            Trace.log(e_errors.INFO, _format % (ret.pri, ret.ticket))
            ticket["status"] = (e_errors.OK, "Priority changed")
            self.reply_to_caller(ticket)

    def change_lm_state(self, ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Change state of the library manager.

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'change_lm_state'
        """

        if ticket.has_key('state'):
            if ticket['state'] in (e_errors.LOCKED, e_errors.IGNORE, e_errors.UNLOCKED, e_errors.PAUSE, e_errors.NOREAD, e_errors.NOWRITE, e_errors.MOVERLOCKED):
                lock = ticket['state']
                if ticket['state'] == e_errors.UNLOCKED:
                    # use the default state if present

                    if ((self.keys.has_key('lock')) and
                        (self.keys['lock'] in (e_errors.LOCKED, e_errors.UNLOCKED, e_errors.IGNORE, e_errors.PAUSE,
                                               e_errors.NOWRITE, e_errors.NOREAD, e_errors.MOVERLOCKED))):
                        lock = self.keys['lock']

                self.lm_lock = lock
                self.set_lock(lock)
                ticket["status"] = (e_errors.OK, None)
                Trace.log(e_errors.INFO,"Library manager state is changed to:%s"%(self.lm_lock,))
            else:
                ticket["status"] = (e_errors.WRONGPARAMETER, None)
        else:
            ticket["status"] = (e_errors.KEYERROR,None)
        self.reply_to_caller(ticket)

    # this is used to include the state information in with the heartbeat
    def return_state(self):
        return self.lm_lock

    def get_lm_state(self, ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Send library manager state to caller.

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'get_lm_state'
        """

        ticket['state'] = self.lm_lock
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

    def get_pending_queue_length(self, ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Send pending queue length to caller.

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'get_pending_queue_length'
        """

        ticket['queue_length'] = self.pending_work.queue_length
        ticket['put_delete'] = (self.pending_work.put_into_queue,
                           self.pending_work.deleted)
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

    def reset_pending_queue_counters(self, ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Reset pending request queue counters.

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'reset_pending_queue_counters'
        """

        self.pending_work.put_into_queue = self.pending_work.queue_length
        self.pending_work.deleted = 0L
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

    def get_active_volumes(self, ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Send active volumes information to caller.

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'get_active_volumes'
        """

        saved_reply_address = ticket.get('r_a', None)
        movers = self.volumes_at_movers.get_active_movers()
        ticket['movers'] = []
        for mover in movers:
            ticket['movers'].append({'mover'          : mover['mover'],
                                     'external_label' : mover['external_label'],
                                     'volume_family'  : mover['volume_family'],
                                     'operation'      : mover['operation'],
                                     'volume_status'  : mover['volume_status'],
                                     'state'   : mover['state'],
                                     'updated' : mover['updated'],
                                     'total_time': time.time()-mover['time_started'],
                                     'time_in_state' : int(mover.get('time_in_state', 0)),
                                     'id' : mover.get('unique_id', None),
                                     })

        ticket['status'] = (e_errors.OK, None)
        ticket["r_a"] = saved_reply_address
        self.reply_to_caller(ticket)

    def remove_active_volume(self, ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Remove active volume from :obj:`LibraryManagerMethods.volumes_at_movers`

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'remove_active_volume'
        """

        saved_reply_address = ticket.get('r_a', None)
        # find the mover to which volume is assigned
        movers = self.volumes_at_movers.get_active_movers()
        for mover in movers:
            if mover['external_label'] == ticket['external_label']:
                break
        else:
            ticket['status'] = (e_errors.DOESNOTEXIST, "Volume not found")
            ticket["r_a"] = saved_reply_address
            self.reply_to_caller(ticket)
            return

        found = 0
        for wt in self.work_at_movers.list:
            if wt['mover'] == mover['mover']:
                found = 1     # must do this. Construct. for...else will not
                              # do better
                break

        Trace.log(e_errors.INFO, "removing active volume %s , mover %s" %
                  (mover['external_label'],mover['mover']))
        self.volumes_at_movers.delete({'mover': mover['mover']})
        if found:
          self.work_at_movers.remove(wt)
        ticket['status'] = (e_errors.OK, None)
        ticket["r_a"] = saved_reply_address
        self.reply_to_caller(ticket)



    # get storage groups
    def storage_groups(self, ticket):
        ticket['storage_groups'] = []
        ticket['storage_groups'] = self.sg_limits
        self.reply_to_caller(ticket)

    # reply to the vol_assert client
    def volume_assert(self, ticket):
        ticket['lm'] = {'address':self.server_address }
        self.volume_assert_list.append(ticket)
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlock

    def remove_suspect_volume(self, ticket):
        """
        Process enstore client call.
        This method is called within :class:`dispatching_worker.DispatchingWorker`
        Remove suspect volume from suspect volumes list

        :type ticket: :obj:`dict`
        :arg ticket: enstore library manager client request containig ticket['work'] = 'remove_suspect_volume'
        """
        saved_reply_address = ticket.get('r_a', None)
        if ticket['volume'] == 'all': # magic word
            self.init_suspect_volumes()
            ticket['status'] = (e_errors.OK, None)
            Trace.log(e_errors.INFO, "suspect volume list cleaned")
        else:
            found = 0
            for vol in self.suspect_volumes.list:
                if ticket['volume'] ==  vol['external_label']:
                    ticket['status'] = (e_errors.OK, None)
                    found = 1
                    break
            else:
                ticket['status'] = (e_errors.NOVOLUME, "No such volume %s"%(ticket['volume']))
            if found:
                self.suspect_volumes.remove(vol)
                Trace.log(e_errors.INFO, "%s removed from suspect volume list"%(vol,))
        ticket["r_a"] = saved_reply_address
        self.reply_to_caller(ticket)

    def reinit(self):
        """
        Overrides GenericServer reinit when received notification of new configuration file.
        """

        Trace.log(e_errors.INFO, "(Re)initializing server")
        self.keys = self.csc.get(self.name)
        Trace.trace(self.my_trace_level+2,"reinit:new keys %s"%(self.keys,))

        self.allow_access = self.keys.get('allow', None)
        self.pri_sel.read_config()
        self.restrictor.read_config()
        self.max_requests = self.keys.get('max_requests', 3000) # maximal number of requests in the queue
        self.volume_clerk_to = self.keys.get('volume_clerk_timeout', 10)
        self.volume_clerk_retry = self.keys.get('volume_clerk_retry', 0)

        # if restrict_access_in_bound is True then restrict simultaneous host access
        # as specified in discipline
        self.restrict_access_in_bound = self.keys.get('restrict_access_in_bound', None)
        Trace.trace(self.my_trace_level+2,"reinit:restrict_access_in_bound %s"%(self.restrict_access_in_bound,))
        c_lock = self.lm_lock
        self.lm_lock = self.get_lock()
        if not self.lm_lock:
            self.lm_lock = e_errors.UNLOCKED
        if c_lock != self.lm_lock:
            self.set_lock(self.lm_lock)
            Trace.log(e_errors.INFO,"Library manager state changed to state:%s"%(self.lm_lock,))

    ########################################
    # End client service requests
    ########################################


class LibraryManagerInterface(generic_server.GenericServerInterface):
    """
    Library manager interface.
    Uses only generic commands, inherited from :class:`generic_server.GenericServerInterface`
    """

    def __init__(self):
        # fill in the defaults for possible options
        generic_server.GenericServerInterface.__init__(self)


    library_options = {}

    # define the command line options that are valid
    def valid_dictionaries(self):
        return generic_server.GenericServerInterface.valid_dictionaries(self) \
               + (self.library_options,)

    paramaters = ["library_name"]

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
    """
    Run library manager
    """
    # get an interface
    intf = LibraryManagerInterface()

    # get a library manager
    lm = LibraryManager(intf.name, (intf.config_host, intf.config_port))
    lm.handle_generic_commands(intf)

    Trace.init(lm.log_name, lm.keys.get('include_thread_name', 'yes'))
    #lm._do_print({'levels':range(5, 400)}) # no manage_queue
    #lm._do_print({'levels':range(5, 500)}) # manage_queue

    while True:
        try:
            # check how long mover is in its state and remove from at_mover list if needed
            t_n = 'check_at_movers'
            if thread_is_running(t_n):
                pass
            else:
                Trace.log(e_errors.INFO, "Library Manager %s (re)starting %s"%(intf.name, t_n))
                #lm.run_in_thread(t_n, lm.check)
                dispatching_worker.run_in_thread(t_n, lm.check)

            if hasattr(lm, 'mover_server'):
                # pull mover request from mover_requests and process it
                t_n = 'process_mover_requests'
                if thread_is_running(t_n):
                    pass
                else:
                    Trace.log(e_errors.INFO, "Library Manager %s (re)starting %s"%(intf.name, t_n))
                    #lm.run_in_thread(t_n, lm.mover_requests.serve_forever)
                    dispatching_worker.run_in_thread(t_n, lm.mover_requests.serve_forever)

            # pull client (not mover and not encp) request from client_requests and process it
            t_n = 'process_client_requests'
            if thread_is_running(t_n):
                pass
            else:
                Trace.log(e_errors.INFO, "Library Manager %s (re)starting %s"%(intf.name, t_n))
                #lm.run_in_thread(t_n, lm.client_requests.serve_forever)
                dispatching_worker.run_in_thread(t_n, lm.client_requests.serve_forever)


            if hasattr(lm, 'encp_server'):
                # pull encp request from encp_requests and process it
                t_n = 'process_encp_requests'
                if thread_is_running(t_n):
                    pass
                else:
                    Trace.log(e_errors.INFO, "Library Manager %s (re)starting %s"%(intf.name, t_n))
                    dispatching_worker.run_in_thread(t_n, lm.encp_requests.serve_forever)

            #lm.serve_forever()

        except SystemExit, exit_code:
            sys.exit(exit_code)
        except:
            traceback.print_exc()
            lm.serve_forever_error("library manager")

        time.sleep(10)

    Trace.alarm(e_errors.ALARM,"Library Manager %sfinished (impossible)"%(intf.name,))

if __name__ == "__main__":

    do_work()

    #import profile
    #profile.run('do_work', '/home/enstore/tmp/enstore/profile')
    #do_work()

