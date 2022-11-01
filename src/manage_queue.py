#!/usr/bin/env python
"""
Manage the pending Library Manager work queue
"""
import sys
import string
import threading
import time

import mpq  # binary search / put
import Trace
import e_errors
import hostaddr

MAX_PRI=1000001
TR=400
DELTA_PRI = 1
AGE_TIME = 30 # min
DEBUG=False
MAX_LONG=(2<<64)-1

# Comparison functions

def compare_priority(r1,r2):
    """
    Compare by priority.
    Used to organize the list by priority
    and time queued.
    The result of comparison is so that
    the highes priority request is closer to the beginning of the sorted list
    hence:
    if r1.pri > r2.pri then result = -1 (r1 < r2)

    if r1.pri == r2.pri then

       if r1.queued < r2.queued then result = -1 (r1 is older than r2)

    :type r1: :class:`Request`
    :arg r1: request 1
    :type r2: :class:`Request`
    :arg r2: request 2
    :rtype: :obj:`tuple` - comparison result
    """

    if r1 is r2:
        return 0
    pri1=(r1.pri, -getattr(r1,'queued',0), id(r1))
    pri2=(r2.pri, -getattr(r2,'queued',0), id(r2))
    return -cmp(pri1, pri2)


# used to organize the list by value
# location cookie, for instance
def compare_value(r1,r2):
    """
    Compare by value.
    Used to organize the list by value,
    location cookie, for instance.

    :type r1: :class:`Request`
    :arg r1: request 1
    :type r2: :class:`Request`
    :arg r2: request 2
    :rtype: :obj:`tuple` - comparison result
    """

    return cmp(r1.value,r2.value)

class Request:
    """
    Pending request.
    """
    def __init__(self, priority, value, ticket, timeq=0):
        """

        :type priority: :obj:`int`
        :arg priority: request priority
        :type value: :obj:`int`
        :arg value: request value (location cookie for read reaquests, file size for write requests)
        :type ticket: :obj:`dict`
        :arg ticket: data transfer client request ticket
        :type timeq: :obj:`float`
        :arg timeq: time returned by :meth:`time.time()`
        """

        self.basepri = priority
        self.pri = priority
        self.value = value
        #self.curpri = ticket.get(['encp']['curpri'], priority
        encp = ticket.get('encp','')
        # by default priority grows by 1 every 1/2 hour
        if encp:
            self.adminpri = encp.get('adminpri',-1)
        else:
            self.adminpri = -1

        self.unique_id = ticket.get('unique_id','')
        #self.queued = ticket['times']['job_queued']
        self.queued = time.time()
        if ticket:
            ticket['times']['job_queued'] = self.queued
            ticket['at_the_top'] = 0
            ticket['encp']['curpri'] = self.pri
            #ticket['encp']['agetime'] = self.agetime
            #ticket['encp']['delpri'] = self.delpri
            self.sg = ticket['vc']['storage_group']


        self.work = ticket.get('work','')
        self.host = None
        self.callback = ticket.get('callback_addr', None)
        if self.callback:
            self.host = hostaddr.address_to_name(self.callback[0])
        wrapper = ticket.get('wrapper','')
        if wrapper:
            self.ofn = wrapper.get('pnfsFilename','')
            if self.host == None:
                self.host = wrapper.get("machine", (None, None))[1]
        else:
            self.ofn = ''

        self.ticket = ticket
        if timeq:
            self.time = timeq

    # compare 2 requestst
    def __cmp__(self,other):
        #pri1=(self.pri, getattr(self,'time',0), id(self))
        #pri2=(other.pri, getattr(other,'time',0), id(other))
        pri1=(self.pri, getattr(self,'queued',0), id(self))
        pri2=(other.pri, getattr(other,'queued',0), id(other))
        rc = cmp(pri1,pri2)
        return rc


    def __repr__(self):
        #return "<priority %s value %s ticket %s>" % (self.pri, self.value, self.ticket)
        return "<priority %s value %s id %s work %s %s>" % (self.pri, self.value, self.unique_id, self.work, self.queued)

    def change_pri(self, pri):
        """
        Change request priority.

        :type pri: :obj:`int`
        :arg pri: new priority
        """

        self.pri = pri
        self.ticket['encp']['curpri'] = self.pri

class SortedList:
    """
    Sorted list of requests.
    This class is the lowest in the hierarchy of
    classes in this file and is used in
    all other classes to hold data.
    This class need to be thread safe for putting data and modification of the list.
    Sorting order is set by comparison function.
    So for write requests the comparioson function will be compare_priority with descending order.
    For read "opt" sublist it will be compare_value with ascending order
    """

    def __init__(self, comparison_function, by_pri=0, aging_quantum=60, name=None):
        """
        :type comparison_function: :obj:`callable`
        :arg comparison_function: comparison funstion to organize ordered list (See class description)
        :type by_pri: :obj:`int`
        :arg by_pri: update list by location if 0, otherwise - by location
        :type aging_quantum: :obj:`int`
        :arg aging_quantum: update list every ``aging_quantum`` seconds
        :type name: :obj:`str`
        :arg name: list name (to make debugging easier)
        """

        self.sorted_list = mpq.MPQ(comparison_function)
        self.last_aging_time = 0
        self.aging_quantum = aging_quantum
        self.ids = set()
        self.of_names = set()
        self.keys = set()
        self.update_flag = by_pri # update either by priority or by location
        self.current_index = 0
        self.stop_rolling = 0 # flag for get_next. Stop if 1
        self.my_name = name
        self.last_deleted = None # to be used for read requests
        self.delpri = DELTA_PRI # delta priority is common for all requests in the list
        self.agetime = AGE_TIME #age time is common for all requests in the list
        self.cumulative_delta_pri = 0 # cumulative delta priority
        self.highest_pri = 0 # highest priorioty in the list
        self.highest_pri_id = None # highest priorioty id
        self.updated = False # flags that self.highest_pri has changed, to use in tags list
        self.queued = time.time()
        self.lock = threading.Lock() # to synchronize changes in the list

    def test(self, id):
        """
        Check if request with certain  id is in the list.

        :type id: :obj:`str`
        :arg id: Request id
        :rtype: :obj:`tuple` (id or 0, ``e_errors.OK`` or :obj:`None`)
        """

        if id in self.ids:
            return id,e_errors.OK
        return 0,None

    # find request by its id
    def find(self, id):
        """
        Find request by its id.

        :type id: :obj:`str`
        :arg id: Request id
        :rtype: :obj:`tuple` (id or :obj:`None`, ``e_errors.OK`` or :obj:`None`)
        """

        if not id in self.ids:
            return None, None
	for r in self.sorted_list:
	    if r.unique_id == id:
		return r,e_errors.OK
        return None, None

    # update delta priority
    def update(self, now=None):
        """
        Update delta priority.

        :type now: :obj:`bool`
        :arg now: update if :obj:`True`
        """

        if not self.update_flag: return  # no need to update by_location list
        time_now = time.time()
        #Trace.trace(TR+23, "now %s self.last_aging_time %s self.aging_quantum %s"%(time_now,self.last_aging_time, self.aging_quantum))
        self.updated = False
        if ((time_now - self.last_aging_time >= self.aging_quantum) or
            now):
            if self.agetime > 0:
                deltas = int((time_now - self.queued)/60/self.agetime)
                self.cumulative_delta_pri = self.delpri*deltas
                self.highest_pri = self.highest_pri + self.delpri
                self.updated = True
            self.last_aging_time = time_now


    def put(self, request, key=''):
        """
        Put request into the sorted list.
        In library manager request queue
        there are 2 types of keys:
        volume family - used for write requests and
        external label - used for read requests.

        :type request: :class:`Request`
        :arg request: client request
        :type key: :obj:`str`
        :arg key: list key as described above
        :rtype: :obj:`tuple` (:obj:`int` - 0 was not in list or 1 was in list, ``e_errors.OK`` or :obj:`None`)
        """
        if request.work == 'write_to_hsm':
            output_file_name = request.ofn
        else:
            output_file_name = None
        # check if request is already in the list
        res, stat = self.test(request.unique_id)
        Trace.trace(TR+23,"SortedList.put: test returned res %s stat %s"%
                    (res, stat))
        if not res:
            # put in the list
            Trace.trace(TR+23,"SortedList.put: %s %s"%
                        (request.pri, request.ticket))
            Trace.trace(TR+23,"SortedList.put: %s %s"%
                        (self.my_name, request))
            self.ids.add(request.unique_id)

            if output_file_name:
                self.of_names.add(output_file_name)
            if key and not key in self.keys:
                self.keys.add(key)
            self.lock.acquire()
            try:
                self.sorted_list.insort(request)
                self.updated = False
                # change the highest priority request in the list
                if request.pri > self.highest_pri:
                  self.highest_pri = request.pri
                  self.highest_pri_id = request.unique_id
                  self.updated = True
            except:
                exc, detail, tb = sys.exc_info()
                Trace.handle_error(exc, detail, tb)
                del(tb)
            self.lock.release()

            stat = e_errors.OK
        else:
            # update routing_callback_addr
            try:
                r, err = self.find(request.unique_id)
            except TypeError:
                res = None
                stat = None
                Trace.trace(TR+23, 'manage_queue put: find returned %s'%
                            (self.find(request.unique_id),))
            if (r and r.ticket.has_key('routing_callback_addr') and
                request.ticket.has_key('routing_callback_addr')):
                r.ticket['routing_callback_addr'] = request.ticket['routing_callback_addr']
        Trace.trace(TR+23,"SortedList.put: returning res %s stat %s"%
                    (res, stat))
        return res, stat

    def get(self, pri=0):
        """
        Get request from the list.

        :rtype: :class:`Request` or :obj:`None`
        """

        self.stop_rolling = 0
        if not self.sorted_list:
            self.lock.acquire()
            self.start_index = self.current_index
            self.lock.release()
            Trace.trace(TR+23,"%s:::SortedList.get: c_i %s s_i %s"%
                        (self.my_name, self.current_index, self.start_index))
            return None    # list is empty
        Trace.trace(TR+23,"%s:::SortedList.get: pri %s, u_f %s"%
                    (self.my_name, pri, self.update_flag))
        # update flag is meaningful only for write list
        if self.update_flag and not pri:
            index = 0
            self.lock.acquire()
            self.current_index = index
            self.lock.release()
        else:
            # find request that has "priority" higher than pri
            #Trace.trace(TR+23, '%s:::SortedList.get:sorted_list %s'%(self.my_name,self.sorted_list,))
            rq = Request(pri, pri, {})
            index = self.sorted_list.bisect(rq)
            Trace.trace(TR+23, '%s:::SortedList.get: i %s rq %s rec %s'%(self.my_name, index, rq, self.sorted_list[index]))
            if not self.update_flag:
                # must be a read request
                # if update flag is 0
                # then the list is sorted by location: no need to update sorted list
                # check if request location is less than current
                Trace.trace(TR+23, 'SortedList.get: pri %s value %s last deleted %s'%
                            (pri, self.sorted_list[index].value, self.last_deleted))
                Trace.trace(TR+23, 'SortedList.get: comparing %s %s'%
                            (pri, self.sorted_list[index].value))
                if pri > 0 and cmp(pri, self.sorted_list[index].value) == 1:
                    if self.last_deleted and self.last_deleted.value == pri:
                        Trace.trace(TR+23, 'SortedList.get: setting index to 0 %s'%
                                    (self.sorted_list[0],))
                        # if yes we rolled over the top of the list
                        # go to its beginning
                        index = 0
        if index < len(self.sorted_list) and index >= 0:
            Trace.trace(TR+23,"%s:::SortedList.get: index %s"%(self.my_name, index,))
            record = self.sorted_list[index]
            record.ticket['at_the_top'] = record.ticket['at_the_top']+1
            self.current_index = index
            ret = record
        else:
            ret = None
        if ret:
            ret.ticket['encp']['curpri'] = ret.ticket['encp']['basepri'] + self.cumulative_delta_pri

        self.lock.acquire()
        self.start_index = self.current_index

        self.lock.release()
        Trace.trace(TR+23,"%s:::SortedList.get: at exit c_i %s s_i %s len %s"%
                    (self.my_name, self.current_index, self.start_index, len(self.sorted_list)))

        return ret

    # get next pending request
    def _get_next(self):
        Trace.trace(TR+23, "%s:::SortedList._get_next stop_rolling %s current_index %s start_index %s list len %s"%
                    (self.my_name, self.stop_rolling,self.current_index, self.start_index, len(self.sorted_list)))
        if not self.sorted_list:
            self.start_index = self.current_index
            Trace.trace(TR+23,"%s:::SortedList._get_next: c_i %s s_i %s"%
                        (self.my_name, self.current_index, self.start_index))
            return None    # list is empty
        if self.stop_rolling:
            self.stop_rolling = 0
            return None
        old_current_index = self.current_index
        self.current_index = self.current_index + 1
        if self.current_index >= len(self.sorted_list):
            self.current_index = 0
        Trace.trace(TR+23,"%s:::SortedList._get_next: c_i %s s_i %s list_len %s"%
                    (self.my_name, self.current_index, self.start_index, len(self.sorted_list)))

        if old_current_index == self.current_index: # only one element in the list
            self.start_index = self.current_index
            Trace.trace(TR+33,"%s:::SortedList._get_next:o_i %s c_i %s s_i %s ret %s"%
                        (self.my_name, old_current_index,self.current_index,self.start_index, None))
            self.stop_rolling = 1
            Trace.trace(TR+33,"%s:::SortedList._get_next:stop_rolling for %s"%(self.my_name, self.sorted_list,))
            return self.sorted_list[self.current_index]
            #return None
        try:
            if self.current_index == self.start_index: # returned to the beginning index
                self.stop_rolling = 1
                Trace.trace(TR+33,"%s:::SortedList._get_next stop_rolling %s for %s"%(self.stop_rolling, self.my_name,self.sorted_list,))
                return None  # came back to where it started
        except AttributeError, detail: # how this happens
            self.start_index = self.current_index
            Trace.trace(TR+33, "SortedList._get_next: ATTR ERR %s"%(detail,))
            return None
        Trace.trace(TR+33,"%s:::SortedList._get_next: o_i %s c_i %s s_i %s ret %s"%
                    (self.my_name, old_current_index,self.current_index,
                     self.start_index, self.sorted_list[self.current_index]))
        rq = self.sorted_list[self.current_index]
        rq.ticket['encp']['curpri'] = rq.ticket['encp']['basepri'] + self.cumulative_delta_pri

        return rq

    def get_next(self, disabled_hosts=[]):
        """
        Get next request, considering that there may be hosts,
        requests from which have been alredy rejected by discipline
        in the current selection cycle.

        :type disabled_hosts: :obj:`list`
        :arg disabled_hosts: list of hosts to skip
        :rtype: :class:`Request` or :obj:`None`
        """

        Trace.trace(TR+33,"%s SortedList.get_next: disabled hosts %s"%(self.my_name, disabled_hosts))
        if not hasattr(self,'start_index'): # get was not called in this selection cycle, call it
            Trace.trace(TR+33,"%s SortedList.get_next: will call get"%(self.my_name,))
            return self.get()
        self.lock.acquire()
        rq = self._get_next()
        self.lock.release()
        if len(disabled_hosts) > 0:
            while rq:
                if rq.host in disabled_hosts:
                    # if request came from host
                    # for which discipline disables
                    # tranfers, try to get another one
                    self.lock.acquire()
                    rq = self._get_next()
                    self.lock.release()
                else:
                    break
        return rq

    def rm(self,record,key=''):
        """
        Remove a request from the list (no updates)

        :type record: :class:`Request`
        :arg record: client request
        :type key: :obj:`str`
        :arg key: list key as described in :meth:`SortedList.put`
        """

        Trace.trace(TR+23,"%s:SortedList.rm: %s %s"%(self.my_name, record.pri, record.ticket))
        if record in self.sorted_list:
            self.lock.acquire()
            try:
                self.sorted_list.remove(record)
                max_index = len(self.sorted_list) - 1
                if max_index < 0:
                    # max index can nont be negative
                    max_index = 0
                Trace.trace(TR+23,"%s:SortedList.rm: max_index %s"%(self.my_name, max_index,))
                if self.current_index > max_index:
                    self.current_index = max_index
                if hasattr(self,'start_index') and self.start_index > max_index:
                    self.start_index = max_index
                if record.unique_id in self.ids: self.ids.remove(record.unique_id)
                if record.ofn in self.of_names: self.of_names.remove(record.ofn)
                if key and key in self.keys:
                    self.keys.remove(key)
                self.last_deleted = record
            except:
                exc, detail, tb = sys.exc_info()
                Trace.handle_error(exc, detail, tb)
                del(tb)

                pass
            self.lock.release()


    # same as remove but with updates
    def delete(self, record, key=''):
        """
        Same as :meth:`SortedList.rm`, but with updates.

        :type record: :class:`Request`
        :arg record: client request
        :type key: :obj:`str`
        :arg key: list key as described in :meth:`SortedList.put`
        """
        pri = record.pri

        self.rm(record, key)
        # find the next highest priority request
        Trace.trace(TR+23,"%s:SortedList.delete id %s highest pri id %s"%(self.my_name, record.unique_id, self.highest_pri_id))
        if record.unique_id == self.highest_pri_id:
            req = self.get(pri)
            if req:
              self.highest_pri = req.pri
              self.highest_pri_id = req.unique_id

        # update priority
        self.update()

    # change priority
    def change_pri(self, record, pri):
        """
        Change priorit for specified request.

        :type record: :class:`Request`
        :arg record: client request
        :type pri: :obj:`int`
        :arg pri: priority
        :rtype: :class:`Request` or :obj:`None`
        """

        if pri <= 0: return None
        # We are working with sorted list
        # using binary tree and priority to order it.
        # To update its element we need
        # to pull it out and reinsert at the right
        # place according to priority (comparison function)
        self.rm(record)
        record.change_pri(pri)
        self.put(record)
        return record

    def wprint(self):
        """
        Print :class:`SortedList` to STDO
        """

        print "LIST LENGTH",len(self.sorted_list)
        cnt = 0
        if len(self.sorted_list):
            for rq in self.sorted_list:
                print rq
                cnt = cnt + 1
                if DEBUG and cnt > 100:
                    break

    def sprint(self):
        """
        Print :class:`SortedList` to stream

        :rtype: :obj:`str`
        """

        m = ''
        cnt = 0
        if len(self.sorted_list):
            m = "LIST LENGTH %s rolling %s\n"%(len(self.sorted_list),self.stop_rolling)
            for rq in self.sorted_list:
                m = '%s %s\n'%(m,rq)
                cnt = cnt + 1
                if DEBUG and cnt > 100:
                    break

        return m

    def get_tickets(self):
        """
        Get all tickets, conatined in :class:`SortedList`

        :rtype: :obj:`list` - list of :obj:`dict` tickets
        """

        tickets = []
        for rq in self.sorted_list:
            tickets.append(rq.ticket)
        return tickets

class Queue:
    """
    Request queue.
    Separate queues are created for write and read requests.
    Separate queue entries are created based on volume family (write requests) or
    external label (read requests).
    Each queue entry has 2 sorted lists (:class:`SortedList`):

       one sorted by priority

       and another sorted by file size (write) or by location cookie (read).
    """
    def __init__(self, aging_quantum=60):
        """
        :type aging_quantum: :obj:`int`
        :arg aging_quantum: update queue every aging_quantum seconds
         """

        # dictionary of volumes for read requests
        # or volume families for write requests
        # each entry in this dictionary is a sorted list
        # of requests
        self.queue = {}
        self.aging_quantum = aging_quantum
        self.queue_type =''

    def put(self, priority, ticket):
        """
        Create :class:`Request` from ``ticket`` and put requests into the queue.

        :type priority: :obj:`int`
        :arg priority: request priority
        :type ticket: :obj:`dict`
        :arg ticket: request ticket
        :rtype: :obj:`tuple` (:class:`Request` or :obj:`None`, ``e_errors.OK`` or ``e_errors.WRONGPARAMETER`` or :obj:`None`)
        """

        Trace.trace(TR+21,"Queue.put: %s %s"%(priority, ticket,))
        # set type of the queue
        if not self.queue_type:
            self.queue_type = ticket['work']
        if ticket['work'] == 'write_to_hsm':
            key = ticket['vc']['volume_family']
            val = ticket["wrapper"]["size_bytes"]
        elif ticket['work'] == 'read_from_hsm':
            key = ticket['fc']['external_label']
            val = ticket['fc']['location_cookie']
        else:
            return None, e_errors.WRONGPARAMETER
        # create a request
        rq = Request(priority, val, ticket, ticket['times']['t0'])
        if not self.queue.get(key,''):
            # create opt entry in the list. For writes it is Volume Family
            # for reads - volume label. Create by_priority entry as well
            self.queue[key] = {'opt':SortedList(compare_value, 0,self.aging_quantum, "opt:%s:"%(key,)), # no updates
                               'by_priority':SortedList(compare_priority, 1, self.aging_quantum, "by_priority:%s:"%(key,))
                              }

        # put request into both queues
        try:
            res,stat = self.queue[key]['by_priority'].put(rq)
        except:
            exc, detail, tb = sys.exc_info()
            Trace.handle_error(exc, detail, tb)
            del(tb)
            return None, None
        if res:
            # already in the list
            return None, stat
        try:
            res, stat = self.queue[key]['opt'].put(rq)
        except:
            exc, detail, tb = sys.exc_info()
            Trace.handle_error(exc, detail, tb)
            del(tb)
            # put into opt list failed
            # remove request from by_priority list
            self.queue[key]['by_priority'].delete(rq)
            return None, None

        if res:
            # already in the list
            ret = None, stat
        else:
            ret = rq, stat
        return  ret

    def test(self, ticket):
        """
        Check if ticket is in the :class:`Queue`.

        :type ticket: :obj:`dict`
        :arg ticket: request ticket
        :rtype: :obj:`tuple` (id or 0, ``e_errors.OK`` or :obj:`None`)
        """

        if ticket['work'] == 'write_to_hsm':
            key = ticket['vc']['volume_family']
        elif ticket['work'] == 'read_from_hsm':
            key = ticket['fc']['external_label']
        unique_id = ticket.get('unique_id','')
        if unique_id and self.queue.has_key(key):
            return self.queue[key]['by_priority'].test(unique_id)
        return 0,None


    def get(self,key='',location=''):
        """
        Get returns a record from the queue.
        Key is either volume family for write requests
        or external_label for read requsts>
        "Optional" location parameter for read requests indicates
        what is the current position of the tape (location_cookie).
        For write requests it is a file size.

        :type key: :obj:`str`
        :arg key: key as described above
        :type location: :obj:`str`
        :arg location: location as decribed above.
        :rtype: :class:`Request`
        """

        #Trace.trace(TR+23, 'Queue.get: Queue list %s'% (self.sprint(),))
        if not key:
            return None
        if not self.queue:
            Trace.trace(TR+23, "Queue.get: queue is empty")
            return None
        Trace.trace(TR+23, "Queue.get: queue %s"%(self.queue,))
        Trace.trace(TR+23, "Queue.get: key %s location %s"%(key, location))
        if not self.queue.has_key(key):
            Trace.trace(TR+23,"Queue.get: key %s is not in the queue"%(key,))
            return None
        self.queue[key]['by_priority'].stop_rolling = 0
        self.queue[key]['opt'].stop_rolling = 0
        sublist = self.queue[key]['opt']

        if location:
            # location means location cookie for read request
            # and file size for write request
            record = sublist.get(location)
        else:
            if self.queue_type == 'read_from_hsm':
                record = sublist.get()
                Trace.trace(TR+23,"Queue.get:read_from_hsm %s %s"%(sublist, record))
            else: record = self.queue[key]['by_priority'].get()
        Trace.trace(TR+23,"Queue.get: %s"%(repr(record),))
        return record

    def what_key(self, request):
        """
        Return key depending on request work type.

        :type request: :class:`Request`
        :arg request: request
        :rtype: :obj:`str` or :obj:`None`
        """

        # identify type of the queue
        if request.work == 'read_from_hsm':
            key = request.ticket['fc']['external_label']
        elif request.work == 'write_to_hsm':
            key = request.ticket['vc']['volume_family']
        else: key = None
        return key

    def delete(self, request):
        """
        Remove request from the :obj:`Queue`.

        :type request: :class:`Request`
        :arg request: request
        """

        key = self.what_key(request)
        if not key: return
        if not self.queue.has_key(key):
            #Trace.log(e_errors.INFO,"manage_queue.delete: no such key %s" %(key,))
            return
        # remove opt entry
        Trace.trace(TR+23,"Queue.delete: opt %s %s"%(key, request.ticket))

        self.queue[key]['opt'].delete(request)
        self.queue[key]['by_priority'].delete(request)
        # if list for this queue is empty, remove
        # the dictionary entry
        if len(self.queue[key]['by_priority'].sorted_list) == 0:
            del(self.queue[key])

    def change_pri(self, request, pri):
        """
        Change request priority.

        :type request: :class:`Request`
        :arg request: request
        :type pri: :obj:`int`
        :arg pri: new priority
        :rtype: :class:`Request` or :obj:`None`
        """

        key = self.what_key(request)
        if not key: return None
        Trace.log(e_errors.INFO,
                  "Queue.change_pri: key %s cur_pri %s pri %s"%(key, request.pri, pri))
        ret = self.queue[key]['opt'].change_pri(request, pri)
        if ret:
            ret = self.queue[key]['by_priority'].change_pri(request, pri)
        return ret

    def get_next(self, key='', disabled_hosts = []):
        """
        Get next request, considering that there may be hosts,
        requests from which have been alredy rejected by discipline
        in the current selection cycle.

        :type key: :obj:`str`
        :arg key: key is either a volume label or volume family
        :type disabled_hosts: :obj:`list`
        :arg disabled_hosts: list of hosts to skip
        :rtype: :class:`Request` or :obj:`None`
        """

        Trace.trace(TR+23, 'Queue.get_next: key %s'% (key,))

        if not key: return None
        if not self.queue:
            Trace.trace(TR+23, "Queue.get_next: queue is empty")
            return None
        Trace.trace(TR+23, "Queue.get_next: keys %s"%(self.queue.keys(),))
        if not self.queue.has_key(key):
            Trace.trace(TR+23,"Queue.get_next: key %s is not in the queue"%(key,))
            return None
        if self.queue_type == 'read_from_hsm':
            sublist = self.queue[key]['opt']
        else:
            # write request"
            sublist = self.queue[key]['by_priority']
        #Trace.trace(TR+23,"Queue.get_next: sublist %s"%(sublist.sprint(),))
        rc = sublist.get_next(disabled_hosts=disabled_hosts)
        Trace.trace(TR+23,"Queue.get_next: sublist returns %s"%(rc,))
        return rc


    def wprint(self):
        """
        Print :class:`Queue` to STDO
        """

        print "********************"
        if not self.queue_type:
            print "UKNOWN QUEUE type"
            return
        else:
            print self.queue_type," Queue"
        print "by_val"
        for key in self.queue.keys():
            print "KEY",key
            self.queue[key]['opt'].wprint()
        print "------------------------------"
        print "by_priority"
        for key in self.queue.keys():
            print "KEY",key
            self.queue[key]['by_priority'].wprint()
        print "********************"

    def sprint(self):
        """
        Print :class:`Queue` to stream

        :rtype: :obj:`str`
        """

        m="********************\n"
        if not self.queue_type:
            m='%sUKNOWN QUEUE type\n'%(m,)
            return m
        m='%s%s Queue\nby_val\n'%(m,self.queue_type)
        for key in self.queue.keys():
            m = '%s KEY %s\n%s'%(m, key, self.queue[key]['opt'].sprint())
        m = '%s------------------------------\nby_priority\n'%(m,)
        for key in self.queue.keys():
            m='%s KEY %s\n%s'%(m,key,self.queue[key]['by_priority'].sprint())
        m='%s********************\n'%(m,)
        return m

    def get_queue(self, queue_key='by_priority'):
        """
        Get all tickets, conatined in :class:`Queue`

        :rtype: :obj:`list` - list of :obj:`dict` tickets
        """

        _list = []
        for key in self.queue.keys():
            _list = _list+self.queue[key][queue_key].get_tickets()
        return _list

    # find record in the queue
    def find(self,id):
        """
        Find request in the :class:`Queue`.

        :type id: :obj:`str`
        :arg id: request id
        :rtype: :class:`Request` or :obj:`None`
        """

        # id - request unuque id
        for key in self.queue.keys():
            record, status = self.queue[key]['by_priority'].find(id)
            if record: break
        else:
            record = None
        return record

    # update priority of requests in the queue
    # returns the list if requests with updated priority
    def update_priority(self):
        """
        Update priority.

        :rtype: :obj:`list` of requests with updated priority
        """

        updated_requests = {}
        for key in self.queue.keys():
            Trace.trace(TR+23, 'Queue.update_priority: updating %s'% (key,))
            try:
                self.queue[key]['by_priority'].update()
            except KeyError:
                Trace.trace(TR+23, "KeyError %s"%(self.queue,))
            if self.queue[key]['by_priority'].updated:
                Trace.trace(TR+23, 'Queue.update_priority: updated %s'% (key,))
                self.queue[key]['by_priority'].updated = False
                # priority was updated
                # get the request
                request, status = self.queue[key]['by_priority'].find(self.queue[key]['by_priority'].highest_pri_id)
                if request:
                    request.ticket['encp']['curpri'] = request.ticket['encp']['basepri'] + self.queue[key]['by_priority'].cumulative_delta_pri
                    request.pri = request.ticket['encp']['curpri']
                    updated_requests[key] = request
        return updated_requests

class Atomic_Request_Queue:
    """
    This class is for either regular or for admin queue.
    Each such queue contains 2 subqueues(:class:`Queue`), one for read requests and another for write requests.
    """

    def __init__(self, aging_quantum=60, name='None'):
        """
        :type aging_quantum: :obj:`int`
        :arg aging_quantum: update :class:`Atomic_Request_Queue` every ``aging_quantum seconds``
        :type name: :obj:`str`
        :arg name: queue name (to make debugging easier)
        """

        self.queue_name = name
        self.write_queue = Queue(aging_quantum)
        self.read_queue = Queue(aging_quantum)
        # sorted list of volumes for read requests
        # and volume families for write requests
        # based on this list the highest priority volume
        # or volume family is selected
        # only one request per volume or volume family
        self.tags = SortedList(compare_priority, 1, aging_quantum, "%s:::TAGS"%(name,))
        # volume or file family references for fast search
        self.ref = {}

        self.aging_quantum = aging_quantum
        # this lock is needed to synchronize
        # put, update, and delete methods
        self._lock = threading.Lock()


    # wrap acquire and release for debugging
    def lockacquire(self):
        Trace.trace(TR+23,"ACQUIRE LOCK")
        self._lock.acquire()
    def lockrelease(self):
        Trace.trace(TR+23,"RELEASE LOCK")
        self._lock.release()

    def update(self, request, key):
        """
        Update queue.

        :type request: :class:`Request`
        :arg request: request
        :type key: :obj:`str`
        :arg key: request key
        """

        Trace.trace(TR+23," Atomic_Request_Queue:update:key: %s"%(key,))
        Trace.trace(TR+23," Atomic_Request_Queue:update:tags.keys: %s"%(self.tags.keys,))
        Trace.trace(TR+23," Atomic_Request_Queue:update:refs.keys: %s"%(self.ref.keys(),))
        updated_rq = None

        self.lockacquire()
        try:
            if self.ref.has_key(key):
                # see if priority of current request is higher than
                # request in the queue and if yes remove it
                Trace.trace(TR+23," Atomic_Request_Queue:update:tags1: %s"%(request,))
                if request.pri > self.ref[key].pri:
                    self.tags.rm(self.ref[key], key)
                    self.tags.put(request, key)
                    del(self.ref[key])
                    self.ref[key] = request
                    updated_rq = request
            else:
                # check if corresponding key is already in the tags
                if not key in self.tags.keys:
                    self.tags.put(request, key)
                    self.ref[key] = request
                    updated_rq = request
        except:
            exc, detail, tb = sys.exc_info()
            Trace.handle_error(exc, detail, tb)
            del(tb)
        self.lockrelease()

        if not updated_rq:
            # nothing to update
            return
        #
        # now update all tags and refs if needed
        #
        # get requests with updated priority
        updated_write_requests = self.write_queue.update_priority()
        updated_read_requests = self.read_queue.update_priority()

        # merge them into one dictionary
        updated_requests = updated_write_requests
        updated_requests.update(updated_read_requests)

        tags_toreplace = []
        Trace.trace(TR+23," Atomic_Request_Queue:updated_requests:%s"%(updated_requests,))
        if updated_requests:
            for key in self.tags.keys:
                Trace.trace(TR+23,"key %s"% (key,))
                if key in (updated_requests.keys()):
                    tags_toreplace.append((key, updated_requests[key]))

            Trace.trace(TR+23," Atomic_Request_Queue:update:tags to replace: %s"%(tags_toreplace,))
            for key, request in tags_toreplace:
                Trace.trace(TR+23," Atomic_Request_Queue:update:replacing0 : %s(%s) %s(%s)"%(request, request.pri, updated_rq, updated_rq.pri))
                if request.unique_id != updated_rq.unique_id: # do not update request which has been just updated
                    Trace.trace(TR+23," Atomic_Request_Queue:update:replacing: %s(%s) with %s(%s)"%
                            (self.ref[key], self.ref[key].pri, request, request.pri))
                    self.lockacquire()
                    try:
                        self.tags.rm(self.ref[key], key)
                        self.tags.put(request, key)
                        del(self.ref[key])
                        self.ref[key] = request
                    except:
                        exc, detail, tb = sys.exc_info()
                        Trace.handle_error(exc, detail, tb)
                        del(tb)

                    self.lockrelease()


    def get_tags(self):
        """
        See what keys has tags list.
        Needed for fair share distribution.

        :rtype: :obj:`list`
        """
        return self.tags.keys

    def get_sg(self, tag):
        """
        Get storage group specified by tag

        :type tag: :obj:`str`
        :arg tag: tag
        :rtype: :obj:`str` - storage group
        """
        return self.tags[tag]

    def put(self, priority, ticket):
        """
        Create :class:`Request` from ``ticket`` and put requests into :class:`Atomic_Request_Queue`.

        :type priority: :obj:`int`
        :arg priority: request priority
        :type ticket: :obj:`dict`
        :arg ticket: request ticket
        :rtype: :obj:`tuple` (:class:`Request` or :obj:`None`, ``e_errors.OK`` or ``e_errors.WRONGPARAMETER`` or :obj:`None`)
        """

        Trace.trace(TR+23," Atomic_Request_Queue:put:ticket: %s"%(ticket,))
        if ticket['work'] == 'write_to_hsm':
            # backward compatibility
            if not ticket['vc'].has_key('storage_group'):
                # special treatment for D0 requests
                if string.find(ticket['wrapper']['pnfsFilename'], "/pnfs/sam") == 0:
                    ticket['vc']['storage_group'] = 'D0'
                else:
                    ticket['vc']['storage_group'] = 'unknown'
            # combine volume family
            if ticket['vc']['file_family'] != 'ephemeral':
                key = string.join((ticket['vc']['storage_group'],
                       ticket['vc']['file_family'],
                       ticket['vc']['wrapper']),'.')
            else:
                key = string.join((ticket['vc']['storage_group'],
                                     'ephemeral',ticket['vc']['wrapper']), '.')
            ticket['vc']['volume_family'] = key
        elif ticket['work'] == 'read_from_hsm':
            key = ticket['fc']['external_label']
        else:
            return None, e_errors.WRONGPARAMETER

        rc = None
        self.lockacquire()
        try:
            if ticket['work'] == 'write_to_hsm':
                rq, stat = self.write_queue.put(priority, ticket)
                if not rq:
                    rc = rq, stat
            elif ticket['work'] == 'read_from_hsm':
                rq, stat = self.read_queue.put(priority, ticket)
                if not rq:
                    rc = rq, stat
        except:
            exc, detail, tb = sys.exc_info()
            Trace.handle_error(exc, detail, tb)
            del(tb)
        self.lockrelease()
        if rc == None:
            try:
                # now get the highest priority request from the queue
                # where request went
                self.update(rq,key)
                rc = rq, e_errors.OK
            except:
                exc, detail, tb = sys.exc_info()
                Trace.handle_error(exc, detail, tb)
                del(tb)
        return rc


    def test(self,ticket):
        """
        Check if ticket is in the :obj:`Atomic_Request_Queue`.

        :type ticket: :obj:`dict`
        :arg ticket: request ticket
        :rtype: :obj:`tuple` (id or 0, ``e_errors.OK`` or :obj:`None`)
        """
        if ticket['work'] == 'write_to_hsm':
            if ticket['vc']['file_family'] != 'ephemeral':
                key = string.join((ticket['vc']['storage_group'],
                       ticket['vc']['file_family'],
                       ticket['vc']['wrapper']),'.')
            else: key = string.join((ticket['vc']['storage_group'],
                                     'ephemeral',ticket['vc']['wrapper']), '.')
            ticket['vc']['volume_family'] = key
            q = self.write_queue
        elif ticket['work'] == 'read_from_hsm':
            q = self.read_queue
        else:
            return 0, None
        if q:
            return q.test(ticket)

    def delete(self,record):
        """
        Remove request from the :obj:`Atomic_Request_Queue`.

        :type record: :class:`Request`
        :arg record: request
        """
        if record.work == 'write_to_hsm':
            key = record.ticket['vc']['volume_family']
            queue = self.write_queue
        if record.work == 'read_from_hsm':
            key = record.ticket['fc']['external_label']
            queue = self.read_queue
        hp_rq = None
        self.lockacquire()
        try:
            queue.delete(record)
            self.tags.delete(record, key)
            try:
                if record == self.ref[key]:
                    # if record is in the reference list (and hence in the
                    # tags list) these enrires must be replaced by record
                    # with the same key if exists
                    del(self.ref[key])
                    # find the highest priority request for this
                    # key
                    hp_rq = queue.get(key)
            except KeyError, detail:
                Trace.log(e_errors.ERROR, "error deleting reference %s. Key %s references %s"%
                          (detail, key, self.ref))
        except:
            exc, detail, tb = sys.exc_info()
            Trace.handle_error(exc, detail, tb)
            del(tb)
        self.lockrelease()
        if hp_rq:
            self.update(hp_rq,key)


    def get(self, key='',location='', next=0, active_volumes=[], disabled_hosts=[]):
        """
        Returns a request from the :obj:`Atomic_Request_Queue`.

        :type key: :obj:`str`
        :arg key: volume may be specified to get read request.
                  Volume family may be specified to get write request
        :type location: :obj:`str`
        :arg location: optional location parameter is meaningful only for
                        read requests and indicates what is current position
                        of the tape (location_cookie)
        :type next: :obj:`int`
        :arg next: indicates whether to get a first highest request
                   or keep getting requsts for specified key
        :type active_volumes: :obj:`list`
        :arg active_volumes: list of volumes at movers.
                             This parameter is optional and meaningful only for
                             read requests.
                             If specified, read requests for volumes in this list
                             will be skipped
        :type disabled_hosts: :obj:`list`
        :arg disabled_hosts: list of hsts disabled by discipline.
        :rtype: :class:`Request` or :obj:`None`
        """

        record = None
        Trace.trace(TR+21,'Atomic_Request_Queue:get:key %s location %s next %s active %s disabled_hosts %s'%
                    (key, location, next, active_volumes, disabled_hosts))
        if key:
            # see if key points to write queue
            if key in self.ref.keys():
                if next:
                    Trace.trace(TR+21, "Atomic_Request_Queue:get:GET_NEXT_0")
                    record = self.write_queue.get_next(key, disabled_hosts = disabled_hosts)
                    Trace.trace(TR+21, "Atomic_Request_Queue:get:write_queue.get_next returned %s"%(record,))
                else:
                    Trace.trace(TR+21, "GET_0")
                    record = self.write_queue.get(key, location)
                # see if key points to read queue
                if not record:
                    if next: record = self.read_queue.get_next(key, disabled_hosts = disabled_hosts)
                    else:
                        record = self.read_queue.get(key, location)
                        Trace.trace(TR+21, "Atomic_Request_Queue:get:GET_AA record %s"%(record,))
                        #if not location: record = self.read_queue.get(key)
                        #else: record = self.read_queue.get(key, location)
            else: record = None
            #return record
        else:
            Trace.trace(TR+21, "Atomic_Request_Queue:get:TAGS %s"%(self.tags.sorted_list,))
            Trace.trace(TR+21, "Atomic_Request_Queue:get:TAGS_0 %s"%(self.tags.keys,))
            # key is not specified, get the highest priority from
            # the tags queue
            if next:
                Trace.trace(TR+21, "Atomic_Request_Queue:get:GET_NEXT_1")
                for r in self.tags.sorted_list:
                    Trace.trace(TR+21, "Atomic_Request_Queue:get:TAG %s" % (r.ticket,))
                # go thhrough all tags

                rq = self.tags.get_next()

                while (rq and rq.work == 'read_from_hsm' and rq.ticket['fc']['external_label'] in active_volumes):
                    Trace.trace(TR+21, "Atomic_Request_Queue:get:SKIPPING %s" % (rq.ticket['fc']['external_label'],))
                    rq = self.tags.get_next()

                Trace.trace(TR+21,"Atomic_Request_Queue:get:NEXT %s" % (rq,))
            else:
                Trace.trace(TR+21, "Atomic_Request_Queue:get:GET_1")
                rq = self.tags.get()
                Trace.trace(TR+21,"Atomic_Request_Queue:get:tags_get returned %s"%(rq,))
            if rq:
                # go trough all tags
                while True:
                    Trace.trace(TR+21,"Atomic_Request_Queue:get:NEXT_01 %s" % (rq,))
                    if rq.work == 'write_to_hsm':
                        key = rq.ticket['vc']['volume_family']
                        if next:
                            record = self.write_queue.get_next(key, disabled_hosts = disabled_hosts)
                        else:
                            record = self.write_queue.get(key)
                    if rq.work == 'read_from_hsm':
                        key = rq.ticket['fc']['external_label']
                        if not location:
                            if next:
                                record = self.read_queue.get_next(key, disabled_hosts = disabled_hosts)
                            else:
                                record = self.read_queue.get(key)
                    Trace.trace(TR+21,"Atomic_Request_Queue:get:record %s" % (record,))
                    if not record:
                        if next:
                            rq = self.tags.get_next()
                            Trace.trace(TR+21,"Atomic_Request_Queue:get: tags.get_next %s" % (rq,))
                            if not rq:
                                # all tags were processes
                                record = rq
                                break
                    else:
                        break
            else:
                record = rq
        if record:
            record.ticket['encp']['curpri'] = record.pri
        Trace.trace(TR+21,"Atomic_Request_Queue: get returning %s" % (record,))
        return record

    def get_queue(self):
        """
        Get all tickets, conatined in :class:`Atomic_Request_Queue`

        :rtype: :obj:`tuple` (:obj:`list` - list of :obj:`dict` write tickets,
                            :obj:`list` - list of :obj:`dict` read tickets)
        """

        Trace.trace(TR+21,"get_queue %s"%(self.queue_name))
        return (self.write_queue.get_queue(),
                self.read_queue.get_queue('opt'))

    # find record in the queue
    def find(self,id):
        """
        Find request in the :class:`Atomic_Request_Queue`.

        :type id: :obj:`str`
        :arg id: request id
        :rtype: :class:`Request` or :obj:`None`
        """

        record = self.write_queue.find(id)
        if not record:
            record = self.read_queue.find(id)
        return record

    def change_pri(self, record, pri):
        """
        Change request priority.

        :type record: :class:`Request`
        :arg record: request
        :type pri: :obj:`int`
        :arg pri: new priority
        :rtype: :class:`Request` or :obj:`None`
        """

        if record.work == 'write_to_hsm':
            key = record.ticket['vc']['volume_family']
            queue = self.write_queue
        if record.work == 'read_from_hsm':
            key = record.ticket['fc']['external_label']
            queue = self.read_queue
        ret = queue.change_pri(record, pri)
        if not ret: return ret
        self.update(record,key)
        return ret

    def wprint(self):
        """
        Print :class:`Atomic_Request_Queue` to STDO
        """

        print "NAME", self.queue_name
        if self.tags.keys == []:
          print "EMPTY"
          return
        print "TAGS"
        print "keys", self.tags.keys
        self.tags.wprint()
        print "--------------------------------"
        print "REFERENCES"
        for key in self.ref.keys():
            print "KEY",key
            print "VALUE",self.ref[key]
        print "--------------------------------"
        print "WRITE QUEUE"
        self.write_queue.wprint()
        print "+++++++++++++++++++++++++++++++"
        print "READ QUEUE"
        self.read_queue.wprint()

    def sprint(self):
        """
        Print :class:`Atomic_Request_Queue` to stream

        :rtype: :obj:`str`
        """

        m= 'NAME %s\n TAGS\nkeys%s\n%s\n'%(self.queue_name, self.tags.keys,self.tags.sprint())
        m='%s--------------------------------\nREFERENCES\n'%(m,)
        for key in self.ref.keys():
            m='%s KEY %s\n VALUE %s\n'%(m, key,self.ref[key])
        m='%s--------------------------------\nWRITE QUEUE\n%s'%(m,self.write_queue.sprint())
        m='%s+++++++++++++++++++++++++++++++\nREAD QUEUE\n%s'%(m,self.read_queue.sprint())
        return m


class Request_Queue:
    """
    Complete requests queue.
    Contains queue of regular(:class:`Atomic_Request_Queue`)
    and high priority(:class:`Atomic_Request_Queue`) requests.
    """
    def __init__(self, aging_quantum=60, adm_pri_to=60):
        """
        :type aging_quantum: :obj:`int`
        :arg aging_quantum: update queue of regular requests every aging_quantum seconds
        :type adm_pri_to: :obj:`int`
        :arg adm_pri_to: update queue of high priority requests every adm_pri_to seconds
        """

        self.regular_queue = Atomic_Request_Queue(aging_quantum, "regular_queue")
        self.admin_queue = Atomic_Request_Queue(aging_quantum, "admin_queue")

        # if this time out expires admin priority request
        # can overrirde any request even if it was for a
        # mounted volume
        self.adm_pri_to = adm_pri_to
        self.adm_pri_t0 = 0.
        self.queue_length = 0L
        self.put_into_queue = 0L
        self.deleted = 0L
        self.families = {}
        self.storage_groups = {}
        # this lock is needed to synchronize
        # work with self.families and self.families
        self._lock = threading.Lock()


    def start_cycle(self):
        """
        Start request seletion cycle
        """
        self.process_admin_queue = 1
        # list of requests, processed in this cycle
        self.processed_requests = []
        self.admin_rq_returned = False # initialize this flag in the beginning of each selection cycle


    def get_tags(self):
        """
        See what keys has tags list.
        Needed for fair share distribution.

        :rtype: :obj:`list`
        """
        return self.admin_queue.tags.keys+self.regular_queue.tags.keys

    def get_sg(self, tag):
        """
        Get storage group specified by tag

        :type tag: :obj:`str`
        :arg tag: tag
        :rtype: :obj:`str` - storage group
        """

        if self.admin_queue.ref.has_key(tag):
            return self.admin_queue.ref[tag].sg
        elif self.regular_queue.ref.has_key(tag):
            return self.regular_queue.ref[tag].sg
        else:
            return None

    def put(self,ticket):
        """
        Create :class:`Request` from ``ticket`` and put requests into :class:`Request_Queue`.

        :type priority: :obj:`int`
        :arg priority: request priority
        :type ticket: :obj:`dict`
        :arg ticket: request ticket
        :rtype: :obj:`tuple` (:class:`Request` or :obj:`None`, ``e_errors.OK`` or ``e_errors.WRONGPARAMETER`` or :obj:`None`)
        """

        basepri = ticket['encp']['basepri']
        adm_pri = ticket['encp']['adminpri']
        if basepri > MAX_PRI:
            basepri = MAX_PRI
        if adm_pri > MAX_PRI:
            adm_pri = MAX_PRI
        if adm_pri > -1:
            # for admin request to be put into the right place
            basepri = adm_pri+MAX_PRI+basepri
            queue = self.admin_queue
        else:
            queue = self.regular_queue
        ticket['encp']['basepri'] = basepri
        ticket['encp']['adminpri'] = adm_pri
        with self._lock:
            try:
                rq, stat = queue.put(basepri, ticket)
            except:
                exc, detail, tb = sys.exc_info()
                Trace.handle_error(exc, detail, tb)
                del(tb)
                Trace.trace(TR+23,"More Details %s %s"%(basepri, ticket))
                rq = None
                stat = None
            if rq:
                try:
                    Trace.trace(TR+21, "PUT %s"%(self.queue_length,))
                    if ticket['work'] == 'write_to_hsm' and ticket['vc'].has_key('file_family'):
                        if self.families.has_key(ticket['vc']['file_family']):
                           self.families[ticket['vc']['file_family']] = self.families[ticket['vc']['file_family']] + 1
                        else:
                           self.families[ticket['vc']['file_family']] = 1
                        Trace.trace(TR+21, "PUT. FF %s"%(self.families,))
                    if self.storage_groups.has_key(rq.ticket["vc"]["storage_group"]):
                        self.storage_groups[rq.ticket["vc"]["storage_group"]] = self.storage_groups[rq.ticket["vc"]["storage_group"]] + 1
                    else:
                        self.storage_groups[rq.ticket["vc"]["storage_group"]] = 1
                    self.queue_length = self.queue_length + 1
                    self.put_into_queue = self.put_into_queue + 1
                    if self.put_into_queue == MAX_LONG:
                       self.put_into_queue = 0L # to avoid overflow


                except:
                    exc, detail, tb = sys.exc_info()
                    Trace.handle_error(exc, detail, tb)
                    del(tb)

        return rq, stat

    def test(self, ticket):
        """
        Check if ticket is in the :obj:`Request_Queue`.

        :type ticket: :obj:`dict`
        :arg ticket: request ticket
        :rtype: :obj:`tuple` (id or 0, ``e_errors.OK`` or :obj:`None`)
        """

        if ticket['encp']['adminpri'] > -1:
            queue = self.admin_queue
        else:
            queue = self.regular_queue
        return queue.test(ticket)


    def delete(self,record):
        """
        Remove request from the :obj:`Atomic_Request_Queue`.

        :type record: :class:`Request`
        :arg record: request
        """

        if record.ticket['encp']['adminpri'] > -1:
            queue = self.admin_queue
        else:
            queue = self.regular_queue
        self._lock.acquire()
        try:
            if record.ticket['work'] == 'write_to_hsm' and record.ticket['vc'].has_key('file_family'):
                if self.families.has_key(record.ticket['vc']['file_family']):
                    self.families[record.ticket['vc']['file_family']] = self.families[record.ticket['vc']['file_family']] - 1
                    if self.families[record.ticket['vc']['file_family']] <= 0 :
                        del(self.families[record.ticket['vc']['file_family']])

            if self.storage_groups.has_key(record.ticket["vc"]["storage_group"]):
                self.storage_groups[record.ticket["vc"]["storage_group"]] = self.storage_groups[record.ticket["vc"]["storage_group"]] - 1
                if self.storage_groups[record.ticket["vc"]["storage_group"]] <= 0:
                    del(self.storage_groups[record.ticket["vc"]["storage_group"]])

            queue.delete(record)
        except:
            exc, detail, tb = sys.exc_info()
            Trace.handle_error(exc, detail, tb)
            del(tb)

        self._lock.release()
        if self.queue_length > 0:
           self.queue_length = self.queue_length - 1
           self.deleted = self.deleted + 1
           if self.deleted == MAX_LONG:
               self.deleted = 0L # to avoid overflow
           if self.queue_length == 0:
               # reset counters
               self.deleted = 0L
               self.put_into_queue = self.queue_length

    def get_admin_request(self, key='',location='', next=0, active_volumes=[], disabled_hosts=[]):
        """
        Returns a request with high priority from the :obj:`Request_Queue`.

        :type key: :obj:`str`
        :arg key: volume may be specified to get read request.
                  Volume family may be specified to get write request
        :type location: :obj:`str`
        :arg location: optional location parameter is meaningful only for
                        read requests and indicates what is current position
                        of the tape (location_cookie)
        :type next: :obj:`int`
        :arg next: indicates whether to get a first highest request
                   or keep getting requsts for specified key
        :type active_volumes: :obj:`list`
        :arg active_volumes: list of volumes at movers.
                             This parameter is optional and meaningful only for
                             read requests.
                             If specified, read requests for volumes in this list
                             will be skipped
        :type disabled_hosts: :obj:`list`
        :arg disabled_hosts: list of hsts disabled by discipline.
        :rtype: :class:`Request` or :obj:`None`
        """

        rq = self.admin_queue.get(key=key, location= location, next=next,
                                  active_volumes=active_volumes, disabled_hosts = disabled_hosts)
        if not rq:
            self.process_admin_queue = 0 # all admin queue is processed
        if rq and not (rq in self.processed_requests):
            # allow to remove rq.ticket['fc']['external_label'] only if
            # request has not yet been processed in this cycle
            # othrewise leave this entry as it may be later get used
            # from the library manager postponed requests
            if rq.work == "write_to_hsm" and rq.ticket['fc'].has_key('external_label'):
                # this entry could have been created when selecting requests
                # in the previous cycle and left there because request
                # was not picked up for some reason
                Trace.trace(TR+22, "get_admin_request: delete %s in %s"%(rq.ticket['fc']['external_label'], rq))
                del(rq.ticket['fc']['external_label'])
            self.processed_requests.append(rq)

        return rq

    def get(self, key='',location='', next=0, use_admin_queue=1, active_volumes=[], disabled_hosts=[]):
        """
        Returns a request with high priority from the :obj:`Request_Queue`.

        :type key: :obj:`str`
        :arg key: volume may be specified to get read request.
                  Volume family may be specified to get write request
        :type location: :obj:`str`
        :arg location: optional location parameter is meaningful only for
                        read requests and indicates what is current position
                        of the tape (location_cookie)
        :type next: :obj:`int`
        :arg next: indicates whether to get a first highest request
                   or keep getting requsts for specified key
        :type use_admin_queue: :obj:`int`
        :arg use_admin_queue: select from high priority requests, othrewise from regular requests
        :type active_volumes: :obj:`list`
        :arg active_volumes: list of volumes at movers.
                             This parameter is optional and meaningful only for
                             read requests.
                             If specified, read requests for volumes in this list
                             will be skipped
        :type disabled_hosts: :obj:`list`
        :arg disabled_hosts: list of hsts disabled by discipline.
        :rtype: :class:`Request` or :obj:`None`
        """

        t = time.time()
        # the commented Trace may take a substantial time
        # depending on a queue size
        # uncomment only for debugging
        #Trace.trace(TR+50, "Request_Queue.get: Queue: %s" % (self.sprint(),))
        Trace.trace(TR+22,'Request_Queue.get: key %s location %s next %s use_admin_queue %s active %s hosts %s'%
                    (key, location, next,use_admin_queue, active_volumes, disabled_hosts))
        if key:
            if use_admin_queue and self.process_admin_queue != 0:
                time_to_check = 0
                # get came with key info, hence it is from
                # have bound volume
                # see if there is a time to check hi_pri requests
                # even if they are not for this key
                now = time.time()
                if (now - self.adm_pri_t0 >= self.adm_pri_to):
                    # admin request is highest no matter what
                    self.adm_pri_t0 = now
                    time_to_check = 1

        record = None
        if use_admin_queue and self.process_admin_queue != 0:
            if (not key) or (key and time_to_check):
                # check admin request queue first
                rq = self.admin_queue.get(key, location, next,
                                          active_volumes=active_volumes, disabled_hosts = disabled_hosts)
                if rq:
                    Trace.trace(TR+22, "admin_queue=1 %s time %s"% (rq.ticket['unique_id'], time.time()-t))
                    # get is called in the external loop
                    # self.admin_rq_returned is used to
                    # keep the information about what queue
                    # is selected
                    self.admin_rq_returned = True
                    if not (rq in self.processed_requests):
                        # allow to remove rq.ticket['fc']['external_label'] only if
                        # request has not yet been processed in this cycle
                        # othrewise leave this entry as it may be later get used
                        # from the library manager postponed requests
                        if rq.work == "write_to_hsm" and rq.ticket['fc'].has_key('external_label'):
                            # this entry could have been created when selecting requests
                            # in the previous cycle and left there because request
                            # was not picked up for some reason
                            Trace.trace(TR+22, "get_admin_request: delete %s in %s"%(rq.ticket['fc']['external_label'], rq))
                            del(rq.ticket['fc']['external_label'])
                        self.processed_requests.append(rq)

                    return rq
                else:
                   self.process_admin_queue = 0

        # key is not specified, get the highest priority from
        # the tags queue
        if next and not self.admin_rq_returned:
            Trace.trace(TR+21, "GET_NEXT_2")
            record = self.regular_queue.get(key, location, next=1,
                                            active_volumes=active_volumes,disabled_hosts = disabled_hosts)
        else:
            Trace.trace(TR+21, "GET_2")
            record = self.regular_queue.get(key, location, active_volumes=active_volumes)

        self.admin_rq_returned = False
        Trace.trace(TR+22, "admin_queue=0 time %s"%(time.time()-t,))

        if record and not (record in self.processed_requests):
            # allow to remove rq.ticket['fc']['external_label'] only if
            # request has not yet been processed in this cycle
            # othrewise leave this entry as it may be later get used
            # from the library manager postponed requests
            if record.work == "write_to_hsm" and record.ticket['fc'].has_key('external_label'):
                # this entry could have been created when selecting requests
                # in the previous cycle and left there because request
                # was not picked up for some reason
                Trace.trace(TR+22, "get_admin_request: delete %s in %s"%(record.ticket['fc']['external_label'], record))
                del(record.ticket['fc']['external_label'])
            self.processed_requests.append(record)
        return record

    def get_queue(self):
        """
        Get all tickets, conatined in :class:`Request_Queue`

        :rtype: :obj:`tuple` (:obj:`list` - list of :obj:`dict` high priority write and read tickets,
                            :obj:`list` - list of :obj:`dict` regular priority write tickets,
                            :obj:`list` - list of :obj:`dict` regular priority read tickets)
        """
        aqw, aqr = self.admin_queue.get_queue()
        rqw, rqr = self.regular_queue.get_queue()
        return (aqw+aqr, rqw, rqr)

    def find(self,id):
        """
        Find request in the :class:`Request_Queue`.

        :type id: :obj:`str`
        :arg id: request id
        :rtype: :class:`Request` or :obj:`None`
        """

        record = self.admin_queue.find(id)
        if not record:
            record = self.regular_queue.find(id)
        return record

    def change_pri(self, record, pri):
        """
        Change request priority.

        :type record: :class:`Request`
        :arg record: request
        :type pri: :obj:`int`
        :arg pri: new priority
        :rtype: :class:`Request` or :obj:`None`
        """

        if record.ticket['encp']['adminpri'] > -1:
            # it must be in the admin queue
            return self.admin_queue.change_pri(record, pri)
        else:
            return self.regular_queue.change_pri(record, pri)

    def wprint(self):
        """
        Print :class:`Request_Queue` to STDO
        """

        print "+++++++++++++++++++++++++++++++"
        print "ADMIN QUEUE"
        self.admin_queue.wprint()
        print "==============================="
        print "REGULAR QUEUE"
        self.regular_queue.wprint()

    def sprint(self):
        """
        Print :class:`Request_Queue` to stream

        :rtype: :obj:`str`
        """

        m='+++++++++++++++++++++++++++++++\nADMIN QUEUE\n%s'%(self.admin_queue.sprint())
        m='%s===============================\nREGULAR QUEUE\n%s'%(m,self.regular_queue.sprint())
        return m


def unit_test():
    """
    Begins section of unit tests for different cases
    """

    pending_work = Request_Queue()

    t1={}
    t1["encp"]={}
    t1['fc'] = {}
    t1['vc'] = {}
    t1["times"]={}
    t1["unique_id"]=1
    t1["encp"]["basepri"]=100
    t1["encp"]["adminpri"]=-1
    t1["encp"]["delpri"]=176
    t1["encp"]["agetime"]=1
    t1["times"]["t0"]=time.time()
    t1['work'] = 'read_from_hsm'
    t1['fc']['external_label'] = 'vol1'
    t1['fc']['location_cookie'] = '2'
    t1['vc']['storage_group'] = 'D0'
    print "PUT",t1['work']
    res = pending_work.put(t1)
    time.sleep(.5)
    print "RESULT",res, t1['fc']['external_label']
    pending_work.sprint()

    t2={}
    t2["encp"]={}
    t2['fc'] = {}
    t2['vc'] = {}
    t2["times"]={}
    t2["unique_id"]=2
    t2["encp"]["basepri"]=200
    t2["encp"]["adminpri"]=-1
    t2["encp"]["delpri"]=125
    t2["encp"]["agetime"]=2
    t2["times"]["t0"]=time.time()
    t2['work'] = 'read_from_hsm'
    t2['fc']['external_label'] = 'vol2'
    t2['fc']['location_cookie'] = '1'
    t2['vc']['storage_group'] = 'D0'
    print "PUT", t2['work']
    res = pending_work.put(t2)
    time.sleep(.5)
    print "RESULT",res, t2['fc']['external_label']
    #pending_work.wprint()

    t4={}
    t4["encp"]={}
    t4["times"]={}
    t4['fc'] = {}
    t4['vc'] = {}
    t4["unique_id"]=4
    t4["encp"]["basepri"]=160
    t4["encp"]["adminpri"]=-1
    t4["encp"]["delpri"]=0
    t4["encp"]["agetime"]=0
    t4["times"]["t0"]=time.time()
    t4['work'] = 'read_from_hsm'
    t4['fc']['external_label'] = 'vol3'
    t4['fc']['location_cookie'] = '5'
    t4['vc']['storage_group'] = 'D0'
    print "PUT", t4['work']
    res = pending_work.put(t4)
    time.sleep(.5)
    print "RESULT",res, t4['fc']['external_label']
    #pending_work.wprint()
    """
    t5={}
    t5["encp"]={}
    t5["times"]={}
    t5['fc'] = {}
    t5['vc'] = {}
    t5['wrapper']={}
    t5['wrapper']['size_bytes']=200L
    t5["unique_id"]=5
    t5["encp"]["basepri"]=200
    t5["encp"]["adminpri"]=-1
    t5["encp"]["delpri"]=0
    t5["encp"]["agetime"]=0
    t5["times"]["t0"]=time.time()
    t5['work'] = 'write_to_hsm'
    t5['vc']['file_family'] = 'family2'
    t5['vc']['wrapper'] = 'cpio_odc'
    t5['vc']['storage_group'] = 'D0'
    t5['wrapper']['pnfsFilename']='file2'
    print "PUT", t5['work']
    res = pending_work.put(t5)
    time.sleep(.5)
    print "RESULT",res, t5['vc']['file_family']
    #pending_work.wprint()
    t5_1={}
    t5_1["encp"]={}
    t5_1["times"]={}
    t5_1['fc'] = {}
    t5_1['vc'] = {}
    t5_1['wrapper']={}
    t5_1['wrapper']['size_bytes']=200L
    t5_1["unique_id"]=51
    t5_1["encp"]["basepri"]=200
    t5_1["encp"]["adminpri"]=-1
    t5_1["encp"]["delpri"]=0
    t5_1["encp"]["agetime"]=0
    t5_1["times"]["t0"]=time.time()
    t5_1['work'] = 'write_to_hsm'
    t5_1['vc']['file_family'] = 'family2'
    t5_1['vc']['wrapper'] = 'cpio_odc'
    t5_1['vc']['storage_group'] = 'D0'
    t5_1['wrapper']['pnfsFilename']='file2'
    print "PUT", t5_1['work']
    res = pending_work.put(t5_1)
    time.sleep(.5)
    print "RESULT",res, t5_1['vc']['file_family']
    #pending_work.wprint()

    t6={}
    t6["encp"]={}
    t6["times"]={}
    t6['fc'] = {}
    t6['vc'] = {}
    t6['wrapper']={}
    t6['wrapper']['size_bytes']=300L
    t6["unique_id"]=6
    t6["encp"]["basepri"]=400
    t6["encp"]["adminpri"]=-1
    t6["encp"]["delpri"]=50
    t6["encp"]["agetime"]=0
    t6["times"]["t0"]=time.time()
    t6['work'] = 'write_to_hsm'
    t6['vc']['wrapper'] = 'cpio_odc'
    t6['vc']['file_family'] = 'family2'
    t6['vc']['storage_group'] = 'D0'
    t6['wrapper']['pnfsFilename']='file3'
    print "PUT", t6['work']
    res = pending_work.put(t6)
    time.sleep(.5)
    print "RESULT",res, t6['vc']['file_family']
    #s=pending_work.sprint()
    #print s
    #pending_work.wprint()
    print "GO SLEEP"
    #time.sleep(65)
    print "WAKE UP"
    t7={}
    t7["encp"]={}
    t7["times"]={}
    t7['fc'] = {}
    t7['vc'] = {}
    t7["unique_id"]=7
    t7["encp"]["basepri"]=150
    t7["encp"]["adminpri"]=-1
    t7["encp"]["delpri"]=0
    t7["encp"]["agetime"]=0
    t7["times"]["t0"]=time.time()
    t7['work'] = 'read_from_hsm'
    t7['fc']['external_label'] = 'vol1'
    t7['fc']['location_cookie'] = '3'
    t7['vc']['storage_group'] = 'D0'
    print "PUT", t7['work']

    res = pending_work.put(t7)
    time.sleep(.5)
    print "RESULT",res,t7['fc']['external_label']
    #pending_work.wprint()
    #s=pending_work.sprint()
    #print s

    t8={}
    t8["encp"]={}
    t8['routing_callback_addr'] = ('131.225.215.253', 56728)
    t8["times"]={}
    t8['fc'] = {}
    t8['vc'] = {}
    t8['wrapper']={}
    t8['wrapper']['size_bytes']=150L
    t8["unique_id"]=8
    t8["encp"]["basepri"]=450
    t8["encp"]["adminpri"]=-1
    t8["encp"]["delpri"]=50
    t8["encp"]["agetime"]=0
    t8["times"]["t0"]=time.time()
    t8['work'] = 'write_to_hsm'
    t8['vc']['wrapper'] = 'cpio_odc'
    t8['vc']['storage_group'] = 'D0'
    t8['vc']['file_family'] = 'family2'
    t8['wrapper']['pnfsFilename']='file4'
    print "PUT", t8['work']
    res = pending_work.put(t8)
    time.sleep(.5)
    print "RESULT",res, t8['vc']['file_family']

    #s=pending_work.sprint()
    #print s
    #pending_work.wprint()
    t9={}
    t9["encp"]={}
    t9['routing_callback_addr'] = ('131.225.215.253', 40000)
    t9["times"]={}
    t9['fc'] = {}
    t9['vc'] = {}
    t9['wrapper']={}
    t9['wrapper']['size_bytes']=150L
    t9["unique_id"]=8
    t9["encp"]["basepri"]=450
    t9["encp"]["adminpri"]=-1
    t9["encp"]["delpri"]=50
    t9["encp"]["agetime"]=0
    t9["times"]["t0"]=time.time()
    t9['work'] = 'write_to_hsm'
    t9['vc']['wrapper'] = 'cpio_odc'
    t9['vc']['storage_group'] = 'D0'
    t9['vc']['file_family'] = 'family2'
    t9['wrapper']['pnfsFilename']='file4'
    print "PUT", t9['work']
    res = pending_work.put(t9)
    time.sleep(.5)
    print "RESULT",res, t9['vc']['file_family']
    pending_work.start_cycle()
    pending_work.wprint()
    #print "enter volume: ",
    #vol = raw_input()
    #print "VOL",vol
    #print "enter location: ",
    #loc = raw_input()
    #print "LOC",loc
    #rq = pending_work.get()
    #print "RQ",rq
    """
    """
    while 1:
    if rq:
        print "****************************************"
        print "REQUEST IS",rq
        #pending_work.delete(rq)
        #location = rq.ticket['fc']['location_cookie']
    else:
        print "No requests in the queue"
        break
    #print "QUEUE", pending_work.wprint()
    print "enter volume: ",
    vol = raw_input()
    rq = pending_work.get(next=1)

    os._exit(0)


    print "+++++++++++++++++++++++++++++"

    while 1:
       pending_work.start_cycle()
       print "GET"
       rq = pending_work.get()
       if rq:
          print "RQ", rq
          pending_work.delete(rq)
          print ">>>>>>>>>>>>>>>>>>>>>>>>>>"
          pending_work.wprint()
          print "<<<<<<<<<<<<<<<<<<<<<<<<<<"
       else:
          break

    v = pending_work.get(0,300)
    print v
    #v = pending_work.f_get(t2["encp"]["basepri"])
    v = pending_work.get()
    print v
    v = pending_work.get()
    print v
    """
    pending_work.start_cycle()
    rq = pending_work.get()
    print "RQ", rq
    rq1 = pending_work.get(next=1)
    print "RQ1",rq1
    rq2 = pending_work.get(next=1)
    print "RQ2",rq2

def unit_test_bz_769():
    # unit test for bugzilla ticket 769
    # at least 2 tickets should be returned
    pending_work = Request_Queue()

    t1={}
    t1["encp"]={}
    t1['fc'] = {}
    t1['vc'] = {}
    t1["times"]={}
    t1["unique_id"]=1
    t1["encp"]["basepri"]=100
    t1["encp"]["adminpri"]=-1
    t1["encp"]["delpri"]=176
    t1["encp"]["agetime"]=1
    t1["times"]["t0"]=time.time()
    t1['work'] = 'read_from_hsm'
    t1['fc']['external_label'] = 'vol1'
    t1['fc']['location_cookie'] = '2'
    t1['vc']['storage_group'] = 'D0'
    t1['callback_addr'] = ('131.225.13.129', 7000)
    print "PUT",t1['work']
    res = pending_work.put(t1)
    print "RESULT",res, t1['fc']['external_label']

    t3={}
    t3["encp"]={}
    t3['fc'] = {}
    t3['vc'] = {}
    t3["times"]={}
    t3['wrapper']={}
    t3['wrapper']['size_bytes']=100L
    t3["unique_id"]=3
    t3["encp"]["basepri"]=300
    t3["encp"]["adminpri"]=-1
    t3["encp"]["delpri"]=0
    t3["encp"]["agetime"]=0
    t3["times"]["t0"]=time.time()
    t3['work'] = 'write_to_hsm'
    t3['vc']['file_family'] = 'family1'
    t3['vc']['wrapper'] = 'cpio_odc'
    t3['vc']['storage_group'] = 'D0'
    t3['wrapper']['pnfsFilename']='file1'
    t3['callback_addr'] = ('131.225.84.71', 7000)
    print "PUT", t3['work']
    res = pending_work.put(t3)
    print "RESULT",res,t3['vc']['file_family']

    t31={}
    t31["encp"]={}
    t31['fc'] = {}
    t31['vc'] = {}
    t31["times"]={}
    t31['wrapper']={}
    t31['wrapper']['size_bytes']=100L
    t31["unique_id"]=31
    t31["encp"]["basepri"]=300
    t31["encp"]["adminpri"]=-1
    t31["encp"]["delpri"]=0
    t31["encp"]["agetime"]=0
    t31["times"]["t0"]=time.time()
    t31['work'] = 'write_to_hsm'
    t31['vc']['file_family'] = 'family1'
    t31['vc']['wrapper'] = 'cpio_odc'
    t31['vc']['storage_group'] = 'D0'
    t31['wrapper']['pnfsFilename']='file31'
    t31['callback_addr'] = ('131.225.84.71', 7000)
    print "PUT", t31['work']
    res = pending_work.put(t31)
    print "RESULT",res,t31['vc']['file_family']

    t311={}
    t311["encp"]={}
    t311['fc'] = {}
    t311['vc'] = {}
    t311["times"]={}
    t311['wrapper']={}
    t311['wrapper']['size_bytes']=100L
    t311["unique_id"]=311
    t311["encp"]["basepri"]=300
    t311["encp"]["adminpri"]=-1
    t311["encp"]["delpri"]=0
    t311["encp"]["agetime"]=0
    t311["times"]["t0"]=time.time()
    t311['work'] = 'write_to_hsm'
    t311['vc']['file_family'] = 'family1'
    t311['vc']['wrapper'] = 'cpio_odc'
    t311['vc']['storage_group'] = 'D0'
    t311['wrapper']['pnfsFilename']='file311'
    t311['callback_addr'] = ('131.225.84.71', 7000)
    print "PUT", t311['work']
    res = pending_work.put(t311)
    print "RESULT",res,t311['vc']['file_family']

    t32={}
    t32["encp"]={}
    t32['fc'] = {}
    t32['vc'] = {}
    t32["times"]={}
    t32['wrapper']={}
    t32['wrapper']['size_bytes']=100L
    t32["unique_id"]=32
    t32["encp"]["basepri"]=300
    t32["encp"]["adminpri"]=-1
    t32["encp"]["delpri"]=0
    t32["encp"]["agetime"]=0
    t32["times"]["t0"]=time.time()
    t32['work'] = 'write_to_hsm'
    t32['vc']['file_family'] = 'family32'
    t32['vc']['wrapper'] = 'cpio_odc'
    t32['vc']['storage_group'] = 'D0'
    t32['wrapper']['pnfsFilename']='file32'
    t32['callback_addr'] = ('131.225.84.71', 7000)
    print "PUT", t32['work']
    res = pending_work.put(t32)
    print "RESULT",res,t32['vc']['file_family']

    # Trace.do_print(range(5, 500)) # uncomment this line for debugging
    pending_work.start_cycle()
    print "PR_GET"
    rq = pending_work.get()
    print "RQ", rq
    if rq:
        print "TICKET", rq.ticket
        print "HOST", rq.host
    cnt = 0
    if rq.ticket['work'] == 'write_to_hsm':
        key = rq.ticket['vc']['volume_family']
    else:
        key = rq.ticket['fc']['external_label']
    while rq:
        print "PR_GET1"
        rq = pending_work.get(key, next=1, disabled_hosts=['tundra.fnal.gov'])
        cnt = cnt + 1
        if rq:
            print "RQ%s %s"%(cnt, rq)
            print "TICKET", rq.ticket
        else:
            print "NO KEY"
            rq = pending_work.get(next=1, disabled_hosts=['tundra.fnal.gov'])
            if rq:
                print "NKRQ%s %s"%(cnt, rq)
                print "NKTICKET", rq.ticket

def unit_test_bz_774():
    # unit test for bugzilla ticket 774
    # repeat the pattern leading to LM hang
    # create 3 requests for vol1 and 1 request for vol3 (not actually needed).
    # start selection cycle
    # call get such that the last request for vol1 gets selected
    # delete this tequest
    # start new selection cycle
    # call get without parameters
    # cal get with next=1 and disabled hosts containing host submutted with requests
    # Not patched code loops in SortedList.get_next()
    # Patched code returns.

    pending_work = Request_Queue()

    t1={}
    t1["encp"]={}
    t1['fc'] = {}
    t1['vc'] = {}
    t1["times"]={}
    t1["unique_id"]=1
    t1["encp"]["basepri"]=100
    t1["encp"]["adminpri"]=-1
    t1["encp"]["delpri"]=176
    t1["encp"]["agetime"]=1
    t1["times"]["t0"]=time.time()
    t1['work'] = 'read_from_hsm'
    t1['fc']['external_label'] = 'vol1'
    t1['fc']['location_cookie'] = 5
    t1['vc']['storage_group'] = 'D0'
    t1['callback_addr'] = ('131.225.13.129', 7000)
    print "PUT",t1['work']
    res = pending_work.put(t1)
    print "RESULT",res, t1['fc']['external_label']

    t2={}
    t2["encp"]={}
    t2['fc'] = {}
    t2['vc'] = {}
    t2["times"]={}
    t2["unique_id"]=2
    t2["encp"]["basepri"]=100
    t2["encp"]["adminpri"]=-1
    t2["encp"]["delpri"]=176
    t2["encp"]["agetime"]=1
    t2["times"]["t0"]=time.time()
    t2['work'] = 'read_from_hsm'
    t2['fc']['external_label'] = 'vol1'
    t2['fc']['location_cookie'] = 2
    t2['vc']['storage_group'] = 'D0'
    t2['callback_addr'] = ('131.225.13.129', 7000)
    print "PUT",t2['work']
    res = pending_work.put(t2)
    print "RESULT",res, t2['fc']['external_label']

    t12={}
    t12["encp"]={}
    t12['fc'] = {}
    t12['vc'] = {}
    t12["times"]={}
    t12["unique_id"]=12
    t12["encp"]["basepri"]=100
    t12["encp"]["adminpri"]=-1
    t12["encp"]["delpri"]=176
    t12["encp"]["agetime"]=1
    t12["times"]["t0"]=time.time()
    t12['work'] = 'read_from_hsm'
    t12['fc']['external_label'] = 'vol1'
    t12['fc']['location_cookie'] = 12
    t12['vc']['storage_group'] = 'D0'
    t12['callback_addr'] = ('131.225.13.129', 7000)
    print "PUT",t12['work']
    res = pending_work.put(t12)
    print "RESULT",res, t12['fc']['external_label']

    t3={}
    t3["encp"]={}
    t3['fc'] = {}
    t3['vc'] = {}
    t3["times"]={}
    t3["unique_id"]=3
    t3["encp"]["basepri"]=150
    t3["encp"]["adminpri"]=-1
    t3["encp"]["delpri"]=176
    t3["encp"]["agetime"]=1
    t3["times"]["t0"]=time.time()
    t3['work'] = 'read_from_hsm'
    t3['fc']['external_label'] = 'vol3'
    t3['fc']['location_cookie'] = 3
    t3['vc']['storage_group'] = 'D0'
    t3['callback_addr'] = ('131.225.13.129', 7000)
    print "PUT",t3['work']
    res = pending_work.put(t3)
    print "RESULT",res, t3['fc']['external_label']
    pending_work.wprint()
    Trace.do_print(range(5, 500)) # uncomment this line for debugging
    pending_work.start_cycle()
    print "PR_GET"
    rq = pending_work.get(key='vol1', location=6)
    print "RQ1", rq
    if rq:
        print "TICKET", rq.ticket
        print "HOST", rq.host
        pending_work.delete(rq)
    else:
        print "NONE"


    print "START NEW CYCLE"
    pending_work.start_cycle()

    rq = pending_work.get()
    print "RQ11", rq
    if rq:
        print "TICKET", rq.ticket
        print "HOST", rq.host
    else:
        print "NONE"

    rq = pending_work.get(next=1, disabled_hosts=['gccensrv1.fnal.gov'])
    print "RQ12", rq
    if rq:
        print "TICKET", rq.ticket
        print "HOST", rq.host
    else:
        print "NONE"

    rq = pending_work.get(next=1, disabled_hosts=['gccensrv1.fnal.gov'])
    print "RQ22", rq
    if rq:
        print "TICKET", rq.ticket
        print "HOST", rq.host
    else:
        print "NONE"


def unit_test_bz_924():
    # Unit test for priority update.
    # Check that priority gets updated correctly
    # and requests get picked according to updated priority.
    # For test results eead the 'comment' values of tickets
    # and check the order.
    # The requests in this test are as
    # request   volume priority
    # 1         vol1       1
    # 2         vol2       2
    # 3         vol2       2
    # 4         vol2       2
    # 5         vol2       2
    # 6         vol1       2
    # requests 1 and get into the queue
    # without any delay
    # Then after 62 secongs get is issued which should pull the request 2 (priority 2).
    # When this request is deleted the queue gets updated raising
    # priority of request 1 to 2.
    # Then request 3 gets into the queue.
    # Then after 62 secondg get is issued which should pull request 1 it has now priority 2,
    # same as priority of request 3, but request 1 is older.
    # And so on.

    global AGE_TIME
    AGE_TIME= 1 # 1 minute
    pending_work = Request_Queue()
    waiting_time = 62 # 62 seconds is enough for request priority to grow

    #Trace.do_print(423) # uncomment this line for debugging

    t1={}
    t1["encp"]={}
    t1['fc'] = {}
    t1['vc'] = {}
    t1["times"]={}
    t1["unique_id"]=1
    t1["encp"]["basepri"]=1
    t1['encp']['adminpri'] = -1
    t1["times"]["t0"]=time.time()
    t1['work'] = 'read_from_hsm'
    t1['fc']['external_label'] = 'vol1'
    t1['fc']['location_cookie'] = 5
    t1['vc']['storage_group'] = 'D0'
    t1['callback_addr'] = ('131.225.13.129', 7000)
    t1['comment'] = "Come out order 2"
    print "PUT",t1
    res = pending_work.put(t1)
    #print "RESULT",res, t1['fc']['external_label']

    t2={}
    t2["encp"]={}
    t2['fc'] = {}
    t2['vc'] = {}
    t2["times"]={}
    t2["unique_id"]=2
    t2["encp"]["basepri"]=2
    t2['encp']['adminpri'] = -1
    t2["times"]["t0"]=time.time()
    t2['work'] = 'read_from_hsm'
    t2['fc']['external_label'] = 'vol2'
    t2['fc']['location_cookie'] = 2
    t2['vc']['storage_group'] = 'D0'
    t2['callback_addr'] = ('131.225.13.129', 7000)
    t2['comment'] = "Come out order 1"
    print "PUT",t2
    res = pending_work.put(t2)
    #print "RESULT",res, t2['fc']['external_label']

    print "Waiting %s s"%(waiting_time,)
    time.sleep(waiting_time)
    #pending_work.wprint()
    pending_work.start_cycle()

    rq = pending_work.get()
    pending_work.delete(rq)
    print "RQ1", rq.ticket

    t2={}
    t2["encp"]={}
    t2['fc'] = {}
    t2['vc'] = {}
    t2["times"]={}
    t2["unique_id"]=3
    t2["encp"]["basepri"]=2
    t2['encp']['adminpri'] = -1
    t2["times"]["t0"]=time.time()
    t2['work'] = 'read_from_hsm'
    t2['fc']['external_label'] = 'vol2'
    t2['fc']['location_cookie'] = 2
    t2['vc']['storage_group'] = 'D0'
    t2['callback_addr'] = ('131.225.13.129', 7000)
    t2['comment'] = "Come out order 3"
    print "PUT",t2
    res = pending_work.put(t2)
    #print "RESULT",res, t2['fc']['external_label']

    print "Waiting %s s"%(waiting_time,)
    time.sleep(waiting_time)
    #pending_work.wprint()
    pending_work.start_cycle()

    rq = pending_work.get()
    pending_work.delete(rq)
    print "RQ2", rq.ticket

    t2={}
    t2["encp"]={}
    t2['fc'] = {}
    t2['vc'] = {}
    t2["times"]={}
    t2["unique_id"]=4
    t2["encp"]["basepri"]=2
    t2['encp']['adminpri'] = -1
    t2["times"]["t0"]=time.time()
    t2['work'] = 'read_from_hsm'
    t2['fc']['external_label'] = 'vol2'
    t2['fc']['location_cookie'] = 2
    t2['vc']['storage_group'] = 'D0'
    t2['callback_addr'] = ('131.225.13.129', 7000)
    t2['comment'] = "Come out order 4"
    print "PUT",t2
    res = pending_work.put(t2)
    #print "RESULT",res, t2['fc']['external_label']

    print "Waiting %s s"%(waiting_time,)
    time.sleep(waiting_time)
    #pending_work.wprint()
    pending_work.start_cycle()

    rq = pending_work.get()
    pending_work.delete(rq)
    print "RQ3", rq.ticket

    t2={}
    t2["encp"]={}
    t2['fc'] = {}
    t2['vc'] = {}
    t2["times"]={}
    t2["unique_id"]=5
    t2["encp"]["basepri"]=2
    t2['encp']['adminpri'] = -1
    t2["times"]["t0"]=time.time()
    t2['work'] = 'read_from_hsm'
    t2['fc']['external_label'] = 'vol2'
    t2['fc']['location_cookie'] = 2
    t2['vc']['storage_group'] = 'D0'
    t2['callback_addr'] = ('131.225.13.129', 7000)
    t2['comment'] = "Come out order 5"
    print "PUT",t2
    res = pending_work.put(t2)
    #print "RESULT",res, t2['fc']['external_label']

    print "Waiting %s s"%(waiting_time,)
    time.sleep(waiting_time)
    #pending_work.wprint()
    pending_work.start_cycle()

    rq = pending_work.get()
    pending_work.delete(rq)
    print "RQ4", rq.ticket

    t1={}
    t1["encp"]={}
    t1['fc'] = {}
    t1['vc'] = {}
    t1["times"]={}
    t1["unique_id"]=6
    t1["encp"]["basepri"]=2
    t1['encp']['adminpri'] = -1
    t1["times"]["t0"]=time.time()
    t1['work'] = 'read_from_hsm'
    t1['fc']['external_label'] = 'vol1'
    t1['fc']['location_cookie'] = 5
    t1['vc']['storage_group'] = 'D0'
    t1['callback_addr'] = ('131.225.13.129', 7000)
    t1['comment'] = "Come out order 6"
    print "PUT",t1
    res = pending_work.put(t1)
    #print "RESULT",res, t1['fc']['external_label']

    print "Waiting %s s"%(waiting_time,)
    time.sleep(waiting_time)
    #pending_work.wprint()
    pending_work.start_cycle()

    rq = pending_work.get()
    pending_work.delete(rq)
    print "RQ5", rq.ticket

    #pending_work.wprint()
    rq = pending_work.get()
    pending_work.delete(rq)
    print "RQ6", rq.ticket







def unit_test_bz_975():
    # Unit test for indefinite request selection loop
    # 1. Put request t1 for "vol1" into queue.
    # 2. Get request t1 and deletes it thus setting self.current_index and
    #    self.start_index to -1
    # 3. Put request t22 for "vol2" and request t3 for "vol1" into queue.
    # 4. Gets request from the queue with active_volumes = ["vol1", "vol2"]
    # This creates indefinite loop for old code

    Trace.do_print(range(5, 500)) # uncomment this line for debugging
    pending_work = Request_Queue()
    t1={}
    t1["encp"]={}
    t1['fc'] = {}
    t1['vc'] = {}
    t1["times"]={}
    t1["unique_id"]=1
    t1["encp"]["basepri"]=1
    t1['encp']['adminpri'] = -1
    t1["times"]["t0"]=time.time()
    t1['work'] = 'read_from_hsm'
    t1['fc']['external_label'] = 'vol1'
    t1['fc']['location_cookie'] = 5
    t1['vc']['storage_group'] = 'D0'
    t1['callback_addr'] = ('131.225.13.129', 7000)
    t1['comment'] = "Come out order 2"
    res = pending_work.put(t1)
    t2={}
    t2["encp"]={}
    t2['fc'] = {}
    t2['vc'] = {}
    t2["times"]={}
    t2["unique_id"]=2
    t2["encp"]["basepri"]=1
    t2['encp']['adminpri'] = -1
    t2["times"]["t0"]=time.time()
    t2['work'] = 'read_from_hsm'
    t2['fc']['external_label'] = 'vol2'
    t2['fc']['location_cookie'] = 5
    t2['vc']['storage_group'] = 'D0'
    t2['callback_addr'] = ('131.225.13.129', 7000)
    t2['comment'] = "Come out order 2"

    t3={}
    t3["encp"]={}
    t3['fc'] = {}
    t3['vc'] = {}
    t3["times"]={}
    t3["unique_id"]=3
    t3["encp"]["basepri"]=2
    t3['encp']['adminpri'] = -1
    t3["times"]["t0"]=time.time()
    t3['work'] = 'read_from_hsm'
    t3['fc']['external_label'] = 'vol1'
    t3['fc']['location_cookie'] = 7
    t3['vc']['storage_group'] = 'D0'
    t3['callback_addr'] = ('131.225.13.129', 7000)
    t3['comment'] = "Come out order 2"

    pending_work.start_cycle()

    rq = pending_work.get(key="K1", next=1, active_volumes=['vv3', 'vv4'])
    rq = pending_work.get(next=1, active_volumes=['vv3', 'vv4'])
    print "RQ", rq
    if rq:
        print "Ticket", rq.ticket
        pending_work.delete(rq)

    """
    print "and once more"
    pending_work.start_cycle()
    rq = pending_work.get_admin_request(key=None)
    print "RQ", rq
    if rq:
        print "Ticket", rq.ticket

    """
    print "Once more"
    res = pending_work.put(t2)
    res = pending_work.put(t3)
    pending_work.start_cycle()

    rq = pending_work.get(key="K1", next=1, active_volumes=['vol1','vol2'])
    rq = pending_work.get(next=1, active_volumes=['vol1','vol2'])
    print "TEST FINISHED"


def unit_test_bz_992():
    # Unit test for threading resyncronisation problem (bz 992)
    # the problem
    # It was observed that if one thread meakes put and another delete or update
    # the tags list may have 2 identical entries.
    # If the corresponding request gets selected and deleted later
    # the "orphan" entry for this request stays in tags without reference to
    # the original requests in the queue.
    # This results in the indefinite loop inside if the queue selection.
    # This test has 2 threads:
    # write, which puts requests with different ids into the queue.
    # read, which gets requests from the queue and deletes them.

    Trace.init("MQ", 'yes')
    Trace.do_print(range(5, 500)) # uncomment this line for debugging
    pending_work = Request_Queue()
    tickets = []

    t1={}
    t1["encp"]={}
    t1['fc'] = {}
    t1['vc'] = {}
    t1["times"]={}
    t1["unique_id"]=1
    t1["encp"]["basepri"]=2
    t1['encp']['adminpri'] = 1
    t1["times"]["t0"]=time.time()
    t1['work'] = 'read_from_hsm'
    t1['fc']['external_label'] = 'vol1'
    t1['fc']['location_cookie'] = 5
    t1['vc']['storage_group'] = 'D0'
    t1['callback_addr'] = ('131.225.13.129', 7000)

    t2={}
    t2["encp"]={}
    t2['fc'] = {}
    t2['vc'] = {}
    t2["times"]={}
    t2["unique_id"]=2
    t2["encp"]["basepri"]=1
    t2['encp']['adminpri'] = 1
    t2["times"]["t0"]=time.time()
    t2['work'] = 'write_to_hsm'
    t2['fc']['external_label'] = 'vol2'
    t2['vc']['storage_group'] = 'D0'
    t2['callback_addr'] = ('131.225.13.129', 7000)
    t2['vc']['file_family'] = 'family2'
    t2['wrapper']={}
    t2['wrapper']['size_bytes']=100L
    t2['vc']['wrapper'] = 'cpio_odc'
    tickets.append(t1)
    tickets.append(t2)

    def th1():
        ticket_id = 0L
        while True:
            try:
                for t in tickets:
                    t["unique_id"] = "id_%s"%(ticket_id,)
                    ticket_id = ticket_id + 1
                    res = pending_work.put(t)
            except KeyboardInterrupt:
                raise KeyboardInterrupt
    def th2():
        pending_work.start_cycle()
        while True:
            try:
                rq = pending_work.get(next=1)
                if rq:
                    print "RQ", rq
                    pending_work.delete(rq)
                else:
                    pending_work.start_cycle()
            except KeyboardInterrupt:
                raise KeyboardInterrupt

    thread = threading.Thread(group=None, target=th2,
                              name="receive_thread", args=(), kwargs={})
    thread.start()
    th1()



    print "TEST FINISHED"



def usage(prog_name):
    print "usage: %s arg"%(prog_name,)
    print "where arg is"
    print "0 - main unit test"
    print "1 - unit test for bugzilla ticket 769 (http://www-enstore.fnal.gov/Bugzilla/show_bug.cgi?id=769)"
    print "2 - unit test for bugzilla ticket 774 (http://www-enstore.fnal.gov/Bugzilla/show_bug.cgi?id=774)"
    print "3 - unit test for update priority:  bugzilla ticket 924 (http://www-enstore.fnal.gov/Bugzilla/show_bug.cgi?id=924)"
    print "4 - unit test for indefinite loop in Atomic_Request_Queue.get:  bugzilla ticket 975 (http://www-enstore.fnal.gov/Bugzilla/show_bug.cgi?id=975)"
    print "5 - unit test for threading resynchronization problem:  bugzilla ticket 992 (http://www-enstore.fnal.gov/Bugzilla/show_bug.cgi?id=975)"

if __name__ == "__main__":
    import os
    if len(sys.argv) == 2:
        if int(sys.argv[1]) == 0:
            unit_test()
            os._exit(0)
        elif int(sys.argv[1]) == 1:
            unit_test_bz_769()
            os._exit(0)
        elif int(sys.argv[1]) == 2:
            unit_test_bz_774()
            os._exit(0)
        elif int(sys.argv[1]) == 3:
            unit_test_bz_924()
            os._exit(0)
        elif int(sys.argv[1]) == 4:
            unit_test_bz_975()
            os._exit(0)
        elif int(sys.argv[1]) == 5:
            unit_test_bz_992()
            os._exit(0)

    usage(sys.argv[0])
    os._exit(1)

