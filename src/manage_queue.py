#!/usr/bin/env python
# manage the pending Library Manager work queue
#########################################################################
# $Id$
#########################################################################

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

# comparison functions
# used to organize the list by priority
# and time queued
def compare_priority(r1,r2):
    # the result of comparison should be so that
    # the highes priority request is closer to the beginning of the sorted list
    # hence:
    # if r1.pri > r2.pri then result = -1 (r1 < r2)
    # if r1.pri == r2.pri then
    #    if r1.queued < r2.queued then result = -1 (r1 is older than r2)
    if r1 is r2:
        return 0
    pri1=(r1.pri, -getattr(r1,'queued',0), id(r1))
    pri2=(r2.pri, -getattr(r2,'queued',0), id(r2))
    return -cmp(pri1, pri2)


# used to organize the list by value
# location cookie, for instance
def compare_value(r1,r2):
    return cmp(r1.value,r2.value)

class Request:
    def __init__(self, priority, value, ticket, timeq=0):
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
        self.pri = pri
        self.ticket['encp']['curpri'] = self.pri

class SortedList:
# Sorted list of requests.
# This class is the lowest in the hierarchy of
# classes in this file and is used in 
# all other classes to hold data.
# This class need to be thread safe for putting data and modification of the list.
# Sorting order is set by comparison function.
# So for write requests the comparioson function will be compare_priority with descending order.
# For read "opt" sublist it will be compare_value with ascending order

    def __init__(self, comparison_function, by_pri=0, aging_quantum=60, name=None):
        self.sorted_list = mpq.MPQ(comparison_function)
        self.last_aging_time = 0
        self.aging_quantum = aging_quantum
        self.highest_pri = 0
        self.ids = []
        self.of_names = []
        self.keys = []
        self.update_flag = by_pri # update either by priority or by location
        self.current_index = 0
        self.stop_rolling = 0 # flag for get_next. Stop if 1
        self.my_name = name
        self.last_deleted = None # to be used for read requests
        self.delpri = DELTA_PRI # delta priority is common for all requests in the list 
        self.agetime = AGE_TIME #age time is common for all requests in the list
        self.cummulative_delta_pri = 0 # cummulative delta priority
        self.highest_pri = 0 # highest priorioty in the list
        self.highest_pri_id = None # highest priorioty id
        self.queued = time.time()
        self.lock = threading.Lock() # to synchronize changes in the list
        
    # check if request with certain  id is in the list
    def test(self, id):
        if id in self.ids:
            return id,e_errors.OK 
        return 0,None
    
    # find request by its id
    def find(self, id):
        if not id in self.ids:
            return None, None
	for r in self.sorted_list:
	    if r.unique_id == id:
		return r,e_errors.OK
        return None, None

    # update delta priority
    def update(self, now=None):
        if not self.update_flag: return  # no need to update by_location list
        time_now = time.time()
        #Trace.trace(TR+23, "now %s self.last_aging_time %s self.aging_quantum %s"%(time_now,self.last_aging_time, self.aging_quantum))
        if ((time_now - self.last_aging_time >= self.aging_quantum) or
            now):
            if self.agetime > 0:
                deltas = int((time_now - self.queued)/60/self.agetime)
                self.cummulative_delta_pri = self.delpri*deltas
            self.last_aging_time = time_now
            

    # put record into the sorted list
    def put(self, request, key=''):
        # in library manager request queue
        # there are 2 types of keys:
        # volume family - used for write requests
        # external label - used for read requests
        # returns a tuple
        # in_list - 0: was not in list
        #         - 1: was in list
        # status - e_errors.OK
        #        - None
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
            self.ids.append(request.unique_id)

            if output_file_name:
                self.of_names.append(output_file_name)
            if key and not key in self.keys:
                self.keys.append(key)
            self.lock.acquire()
            try:
                self.sorted_list.insort(request)
                # change the highest priority request in the list
                if request.pri > self.highest_pri:
                  self.highest_pri = request.pri
                  self.highest_pri_id = request.unique_id
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

        return res, stat

    # get a record from the list
    def get(self, pri=0):
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
            ret.ticket['encp']['curpri'] = ret.ticket['encp']['basepri'] + self.cummulative_delta_pri

        self.lock.acquire()
        self.start_index = self.current_index
        
        self.lock.release()
        Trace.trace(TR+23,"%s:::SortedList.get: at exit c_i %s s_i %s"%
                    (self.my_name, self.current_index, self.start_index))

        return ret

    # get next pending request
    def _get_next(self):
        Trace.trace(TR+23, "%s:::SortedList._get_next stop_rolling %s current_index %s"%
                    (self.my_name, self.stop_rolling,self.current_index))
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
        Trace.trace(TR+23,"%s:::SortedList._get_next:?????"%(self.my_name,))
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
        rq.ticket['encp']['curpri'] = rq.ticket['encp']['basepri'] + self.cummulative_delta_pri

        return rq

    # get next request, considering that there may be hosts
    # requests from which have been alredy rejected by discipline
    # in the current selection cycle
    def get_next(self, disabled_hosts=[]):
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
    
    # remove a request from the list (no updates)
    def rm(self,record,key=''):
        Trace.trace(TR+23,"SortedList.rm: %s %s"%(record.pri, record.ticket))
        if record in self.sorted_list:
            self.lock.acquire()
            try:
                self.sorted_list.remove(record)
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
        pri = record.pri
        self.rm(record, key)
        # find the next highest priority request
        if record.unique_id == self.highest_pri_id:
            req = self.get(pri)
            if req:
              self.highest_pri = req.pri
              self.highest_pri_id = req.unique_id
             
        # update priority
        self.update()


    # change priority
    def change_pri(self, record, pri):
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

    # print the list
    def wprint(self):
        print "LIST LENGTH",len(self.sorted_list)
        cnt = 0
        if len(self.sorted_list):
            for rq in self.sorted_list:
                print rq
                cnt = cnt + 1
                if DEBUG and cnt > 100:
                    break

    def sprint(self):
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
        tickets = []
        for rq in self.sorted_list:
            tickets.append(rq.ticket)
        return tickets

class Queue:
# Request queue.
# Consists of lists of requests for particular kind of request
# read or write in our case
    def __init__(self, aging_quantum=60):
        # dictionary of volumes for read requests
        # or volume families for write requests
        # each entry in this dictionary is a sorted list
        # of requests
        self.queue = {}
        self.aging_quantum = aging_quantum
        self.queue_type =''

    # put requests into the queue
    def put(self, priority, ticket):
        # returns a tuple
        # result - request (instance) or None
        # status - None or e_errors.OK or e_errors.WRONGPARAMETER
        # see SortedList.put
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
            self.queue[key] = {'opt':SortedList(compare_value, 0,self.aging_quantum, key), # no updates
                              'by_priority':SortedList(compare_priority, 1, self.aging_quantum, key)
                              #'by_priority':SortedList(compare_priority, 1, self.aging_quantum, key)
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
        if ticket['work'] == 'write_to_hsm':
            key = ticket['vc']['volume_family']
        elif ticket['work'] == 'read_from_hsm':
            key = ticket['fc']['external_label']
        unique_id = ticket.get('unique_id','')
        if unique_id and self.queue.has_key(key):
            return self.queue[key]['by_priority'].test(unique_id)
        return 0,None
            

    # get returns a record from the queue
    # key is either volume family for write requests
    # or external_label for read requsts
    # "optional" location parameter for read requests indicates
    # what is current position of the tape (location_cookie)
    # for write requests it is file size
    def get(self,key='',location=''):
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

    # find key depending on wok type
    def what_key(self, request):
        # identify type of the queue
        if request.work == 'read_from_hsm':
            key = request.ticket['fc']['external_label'] 
        elif request.work == 'write_to_hsm':
            key = request.ticket['vc']['volume_family']
        else: key = None
        return key

    # remove request from the queue
    def delete(self, request):
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

    # change priority
    def change_pri(self, request, pri):
        key = self.what_key(request)
        if not key: return None
        Trace.log(e_errors.INFO,
                  "Queue.change_pri: key %s cur_pri %s pri %s"%(key, request.pri, pri))
        ret = self.queue[key]['opt'].change_pri(request, pri)
        if ret:
            ret = self.queue[key]['by_priority'].change_pri(request, pri)
        return ret
            
    def get_next(self, key='', disabled_hosts = []):
        # key is either a volume label or volume family
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

    # get all entries from the queue
    def get_queue(self, queue_key='by_priority'):
        _list = []
        for key in self.queue.keys():
            _list = _list+self.queue[key][queue_key].get_tickets()
        return _list

    # find record in the queue
    def find(self,id):
        # id - request unuque id
        for key in self.queue.keys():
            record, status = self.queue[key]['by_priority'].find(id)
            if record: break
        else:
            record = None
        return record


class Atomic_Request_Queue:
    # this class is for either regular or for admin queue
    def __init__(self, aging_quantum=60, name='None'):
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

    def update(self, request, key):
        Trace.trace(TR+23," Atomic_Request_Queue:update:key: %s"%(key,))
        Trace.trace(TR+23," Atomic_Request_Queue:update:tags.keys: %s"%(self.tags.keys,))
        Trace.trace(TR+23," Atomic_Request_Queue:update:refs.keys: %s"%(self.ref.keys(),))
        if self.ref.has_key(key):
            # see if priority of current requst is higher than
            # request in the queue and if yes remove it
            Trace.trace(TR+23," Atomic_Request_Queue:update:tags1: %s"%(request,))
            if request.pri > self.ref[key].pri:
                self.tags.rm(self.ref[key], key)
                self.tags.put(request, key)
                del(self.ref[key])
                self.ref[key] = request
        else:
            # check if corresponding key is already in the tags
            if not key in self.tags.keys:
                self.tags.put(request, key)
                self.ref[key] = request
        
    # see how many different keys has tags list
    # needed for fair share distribution
    def get_tags(self):
        return self.tags.keys

    def get_sg(self, tag):
        return self.tags[tag]

    def put(self, priority, ticket):
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

        if ticket['work'] == 'write_to_hsm':
            rq, stat = self.write_queue.put(priority, ticket)
            if not rq:
                return rq, stat
        elif ticket['work'] == 'read_from_hsm':
            rq, stat = self.read_queue.put(priority, ticket)
            if not rq:
                return rq, stat


        # now get the highest priority request from the queue
        # where request went
        #hp_rq = queue.get(key)
        #if hp_rq and rq != hp_rq:
        self.update(rq,key)
        rc = rq, e_errors.OK
        #else: rc = None, e_errors.UNKNOWN
        return rc
    

    def test(self,ticket):
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

    # delete the record
    def delete(self,record):
        if record.work == 'write_to_hsm':
            key = record.ticket['vc']['volume_family']
            queue = self.write_queue
        if record.work == 'read_from_hsm':
            key = record.ticket['fc']['external_label']
            queue = self.read_queue
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
                if hp_rq: self.update(hp_rq,key)
        except KeyError, detail:
            Trace.log(e_errors.ERROR, "error deleting reference %s. Key %s references %s"%
                      (detail, key, self.ref))
            
    # get returns a record from the queue
    # volume may be specified for read queue
    # volume family may be specified for write queue
    # "optional" location parameter is meaningful only for
    # read requests and indicates what is current position
    # of the tape (location_cookie)
    # flag next indicates whether to get a first highest request
    # or keep getting requsts for specified key
    # active_volumes - list of volumes at movers
    # this parameter is optional and meaningful only for
    # read requests
    # if specified, read requests for volumes in this list
    # will be skipped
    # disabled_hosts - list of hsts disabled by discipline.
    
    def get(self, key='',location='', next=0, active_volumes=[], disabled_hosts=[]):
        record = None
        Trace.trace(TR+21,'Atomic_Request_Queue:get:key %s location %s next %s active %s'%
                    (key, location, next, active_volumes))
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
        Trace.trace(TR+21,"Atomic_Request_Queue: get returning %s" % (record,))
        return record

    def get_queue(self):
        Trace.trace(TR+21,"get_queue %s"%(self.queue_name))
        return (self.write_queue.get_queue(),
                self.read_queue.get_queue('opt'))
        
    # find record in the queue
    def find(self,id):
        record = self.write_queue.find(id)
        if not record:
            record = self.read_queue.find(id)
        return record

    # change priority
    def change_pri(self, record, pri):
        if record.work == 'write_to_hsm':
            key = record.ticket['vc']['volume_family']
            queue = self.write_queue
        if record.work == 'read_from_hsm':
            key = record.ticket['fc']['external_label']
            queue = self.read_queue
        ret = queue.change_pri(record, pri)
        if not ret: return ret
        self.update(record,key)
        #self.tags.delete(record)
        """
        if cmp(id(record), id(self.ref[key])) == 0:
            # if record is in the reference list (and hence in the
            # tags list) these enries must be replaced by record
            # with the same key if exists
            del(self.ref[key])
            # find the highest priority request for this
            # key
            hp_rq = queue.get(key)
            if hp_rq: self.update(hp_rq,key)
        """
        return ret

    def wprint(self):
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
        m= 'NAME %s\n TAGS\nkeys%s\n%s\n'%(self.queue_name, self.tags.keys,self.tags.sprint())
        m='%s--------------------------------\nREFERENCES\n'%(m,)
        for key in self.ref.keys():
            m='%s KEY %s\n VALUE %s\n'%(m, key,self.ref[key]) 
        m='%s--------------------------------\nWRITE QUEUE\n%s'%(m,self.write_queue.sprint()) 
        m='%s+++++++++++++++++++++++++++++++\nREAD QUEUE\n%s'%(m,self.read_queue.sprint())
        return m


class Request_Queue:
    def __init__(self, aging_quantum=60, adm_pri_to=60):
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
        self.process_admin_queue = 1
        # list of requests, processed in this cycle
        self.processed_requests = []
        
    # see how many different keys has tags list
    # needed for fair share distribution
    def get_tags(self):
        return self.admin_queue.tags.keys+self.regular_queue.tags.keys

    # get sg for a specified tag
    # tag is:
    # external label for read requests
    # volume family for write requests
    def get_sg(self, tag):
        if self.admin_queue.ref.has_key(tag):
            return self.admin_queue.ref[tag].sg
        elif self.regular_queue.ref.has_key(tag):
            return self.regular_queue.ref[tag].sg
        else:
            return None

    def put(self,ticket):
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
        #self._lock.acquire()
        try:
            rq, stat = queue.put(basepri, ticket)
        except:
            rq = None
            stat = None
        #self._lock.release()
        if rq:
            self._lock.acquire()
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
                print "EXC", detail
                Trace.handle_error(exc, detail, tb)
                del(tb)
        
            self._lock.release()
                
        return rq, stat

    def test(self, ticket):
        if ticket['encp']['adminpri'] > -1:
            queue = self.admin_queue
        else:
            queue = self.regular_queue
        return queue.test(ticket)
    
    
    # delete the record
    def delete(self,record):
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
    
    # get returns a record from the queue
    # volume may be specified for read queue
    # volume family may be specified for write queue
    # "optional" location parameter is meaningful only for
    # read requests and indicates what is current position
    # of the tape (location_cookie)
    # flag next indicates whether to get a first highest request
    # or keep getting requsts for specified key
    # active_volumes - list of volumes at movers
    # this parameter is optional and meaningful only for
    # read requests
    # if specified, read requests for volumes in this list
    # will be skipped
    # disabled_hosts - list of hosts disabled by discipline
    
    def get(self, key='',location='', next=0, use_admin_queue=1, active_volumes=[], disabled_hosts=[]):
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
                    self.admin_rq_returned = 1
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
        if next and self.admin_rq_returned == 0:
            Trace.trace(TR+21, "GET_NEXT_2")
            record = self.regular_queue.get(key, location, next=1,
                                            active_volumes=active_volumes,disabled_hosts = disabled_hosts)
        else:
            Trace.trace(TR+21, "GET_2")
            record = self.regular_queue.get(key, location, active_volumes=active_volumes)

        self.admin_rq_returned = 0
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
        aqw, aqr = self.admin_queue.get_queue()
        rqw, rqr = self.regular_queue.get_queue()
        return (aqw+aqr, rqw, rqr)
        
    # find record in the queue
    def find(self,id):
        record = self.admin_queue.find(id)
        if not record:
            record = self.regular_queue.find(id)
        return record

    # change priority
    def change_pri(self, record, pri):
        if record.ticket['encp']['adminpri'] > -1:
            # it must be in the admin queue
            return self.admin_queue.change_pri(record, pri)
        else:
            return self.regular_queue.change_pri(record, pri)

    def wprint(self):
        print "+++++++++++++++++++++++++++++++"
        print "ADMIN QUEUE"
        self.admin_queue.wprint()
        print "==============================="
        print "REGULAR QUEUE"
        self.regular_queue.wprint()

    def sprint(self):
        m='+++++++++++++++++++++++++++++++\nADMIN QUEUE\n%s'%(self.admin_queue.sprint())
        m='%s===============================\nREGULAR QUEUE\n%s'%(m,self.regular_queue.sprint())
        return m
         

def unit_test():
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
    """
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
    print "PUT", t3['work']
    res = pending_work.put(t3)
    time.sleep(.5)
    print "RESULT",res,t3['vc']['file_family'] 
    #pending_work.wprint()
    """
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
    # at least 2 tickets shoiuld be returned
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
          
def usage(prog_name):
    print "usage: %s arg"%(prog_name,)
    print "where arg is"
    print "0 - main unit test"
    print "1 - unit test for bugzilla ticket 769 (http://www-enstore.fnal.gov/Bugzilla/show_bug.cgi?id=769)"

if __name__ == "__main__":
    import os
    if len(sys.argv) == 2:
        if int(sys.argv[1]) == 0:
            unit_test()
            os._exit(0)
        elif int(sys.argv[1]) == 1:
            unit_test_bz_769()
            os._exit(0)
    usage(sys.argv[0])
    os._exit(1)
    
  
