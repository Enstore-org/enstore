#!/usr/bin/env python
# manage the pending Library Managerwork queue
__rcsid__="$Id"
import string
import mpq  # binary search / put
import time
import Trace
import e_errors


# comparison functions
def compare_priority(r1,r2):
    if r1 is r2:
        return 0
    return -cmp(r1.pri, r2.pri)

def compare_value(r1,r2):
    return cmp(r1.value,r2.value)

class Request:
    def __init__(self, priority, value, ticket, timeq=0):
        self.basepri = priority
        self.pri = priority
        self.value = value
        #self.curpri = ticket.get(['encp']['curpri'], priority
        encp = ticket.get('encp','')
        if encp:
            self.delpri = encp.get('delpri',0)
            self.agetime = encp.get('agetime',0)
            self.adminpri = encp.get('adminpri',-1)
        else:
            self.delpri = 0
            self.agetime = 0
            self.adminpri = -1
            
        self.unique_id = ticket.get('unique_id','')
        #self.queued = ticket['times']['job_queued']
        self.queued = time.time()
        if ticket:
            ticket['times']['job_queued'] = self.queued
            ticket['at_the_top'] = 0
        self.work = ticket.get('work','')
        wrapper = ticket.get('wrapper','')
        if wrapper:
            self.ofn = wrapper.get('pnfsFilename','')
        else:
            self.ofn = ''
        self.ticket = ticket
        if timeq: self.time = timeq
        
    # compare 2 requestst
    def __cmp__(self,other):
        pri1=(self.pri, getattr(self,'time',0), id(self))
        pri2=(other.pri, getattr(other,'time',0), id(other))
        return cmp(pri1,pri2)
    

    def __repr__(self):
        #return "<priority %s value %s ticket %s>" % (self.pri, self.value, self.ticket)
        return "<priority %s value %s >" % (self.pri, self.value)

    # update request priority
    def update_pri(self):
        pri = self.basepri
        now = time.time()
        deltas = 0
        if self.agetime > 0:
            old_pri = self.pri
            deltas = int(now-self.queued)/60/self.agetime
            pri = pri + self.delpri*deltas
        if pri != self.pri:
            self.pri = pri
            self.ticket['encp']['curpri'] = self.pri
        return old_pri, self.pri

    def change_pri(self, pri):
        self.pri = pri
        self.ticket['encp']['curpri'] = self.pri

class SortedList:
# sorted list of requests    
    def __init__(self, comparison_function, by_pri=1, aging_quantum=60):
        self.sorted_list = mpq.MPQ(comparison_function)
        self.last_aging_time = 0
        self.aging_quantum = 60  #one minute
        self.highest_pri = 0
        self.ids = []
        self.of_names = []
        self.keys = []
        self.update_flag = by_pri
        self.current_index = 0
        
    # check if request with certain  id is in the list
    # and if its outputfile name is in the list flag this
    # with e_errors.INPROGRESS (needed for write requests)
    def test(self, id, output_file_name=None):
        if id in self.ids: return 1,e_errors.OK 
        if output_file_name in self.of_names: return 1, e_errors.INPROGRESS
        return 0,None
    
    # this probably is not needed
    def find(self,id,output_file_name=None):
        if not id in self.ids:
            return None, None
	for r in self.sorted_list:
	    if r.unique_id == id:
		return r,e_errors.OK
            if (output_file_name and
                output_file_name == r.ticket["wrapper"]['pnfsFilename']):
                return r,e_errors.INPROGRESS

    # update the sorted request list
    def update(self, now=None):
        if not self.update_flag: return  # no need to update by_location list
        time_now = time.time()
        rescan_list = []
        if ((time_now - self.last_aging_time >= self.aging_quantum) or
            now): 
            # it is time to update priorities
            for record in self.sorted_list:
                if record.agetime > 0:
                    old_pri,new_pri = record.update_pri()
                    if new_pri != old_pri:
                        rescan_list.append(record)
            if rescan_list:
                # temporarily remove records that have changed priorities
                for record in rescan_list:
                    Trace.trace(13,"SortedList.update: delete Pri%s Ticket %s"%
                                (record.pri, record.ticket))
                    self.delete(record)
                # put them pack according to new priority
                for record in rescan_list:
                    Trace.trace(13,"SortedList.update: reinsert Pri%s Ticket %s"%
                                (record.pri, record.ticket))
                    self.put(record)
            self.last_aging_time = time_now

    # put record into the sorted list
    def put(self, request, key=''):
        # udate the list before putting a record
        self.update()
        if request.work == 'write_to_hsm':
                output_file_name = request.ofn
        else: output_file_name = None
        # check if request is already in the list
        res, stat = self.test(request.unique_id,output_file_name)
        Trace.trace(13,"SortedList.put: test returned res %s stat %s"%
                    (res, stat))
        if not res:
            # put in the list
            Trace.trace(13,"SortedList.put: put %s %s"%
                        (request.pri, request.ticket))
            self.ids.append(request.unique_id)
            if output_file_name: self.of_names.append(output_file_name)
            if key and not key in self.keys: self.keys.append(key)
            self.sorted_list.insort(request)
            stat = e_errors.OK
        return res, stat

    # get a record from the list
    def get(self, pri=0):
        if not self.sorted_list: return None    # list is empty
        self.update()
        Trace.trace(13,"SortedList.get: pri %s, u_f %s"%
                    (pri, self.update_flag))
        # update flag is maeaningful only for write list
        if self.update_flag and not pri:
            index = 0
            self.current_index = index
        else:
            # find request that has "priority" higher than pri 
            rq = Request(pri, pri, {})
            index = self.sorted_list.bisect(rq)
        if index < len(self.sorted_list):
            Trace.trace(13,"SortedList.get: index %s"%(index,))
            record = self.sorted_list[index]
            record.ticket['at_the_top'] = record.ticket['at_the_top']+1
            self.current_index = index
            ret = record
        else: ret = None
        self.start_index = self.current_index
        return ret

    def get_next(self):
        if not self.sorted_list: return None    # list is empty
        old_current_index = self.current_index
        self.current_index = self.current_index + 1
        if self.current_index >= len(self.sorted_list):
            self.current_index = 0
        if old_current_index == self.current_index: # only one element in the list
            return None
        if self.current_index == self.start_index: return None  # came back to where it started
        return self.sorted_list[self.current_index]
    
    # remove a request fro m the list (no updates)
    def rm(self,record,key=''):
        Trace.trace(13,"SortedList.rm: %s %s"%(record.pri, record.ticket))
        if record in self.sorted_list:
            self.sorted_list.remove(record)
        if record.unique_id in self.ids: self.ids.remove(record.unique_id)
        if record.ofn in self.of_names: self.of_names.remove(record.ofn)
        record.ticket['times']['in_queue'] = time.time() - \
                                             record.ticket['times']['job_queued']
        if key and key in self.keys:
            self.keys.remove(key)
            
    # same as remove but with updates
    def delete(self, record, key=''):
        self.rm(record, key)
        self.update()

    # change priority
    def change_pri(self, record, pri):
        if pri < 0: return None
        self.rm(record)
        record.change_pri(pri)
        self.put(record)
        return record

    # print the list
    def wprint(self):
        print "LIST LENGTH",len(self.sorted_list)
        if len(self.sorted_list):
            for rq in self.sorted_list:
                print rq
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
    def put(self, ticket, time=0):
        Trace.trace(13,"Queue.put: %s"%(ticket,))
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
        rq = Request(ticket['encp']['basepri'], val, ticket, ticket['times']['t0'])
        if not self.queue.get(key,''):
            # create opt entry in the list. For writes it is Volume Family
            # for reads - volume label. Create by_priority entyr as well
            self.queue[key] = {'opt':SortedList(compare_value, 0,self.aging_quantum), # no updates
                              'by_priority':SortedList(compare_priority, 1, self.aging_quantum)
                              }
                
        # put requst into both queues
        res,stat = self.queue[key]['by_priority'].put(rq)
        if res: return None, stat
            
        res, stat = self.queue[key]['opt'].put(rq)
        if res: ret = None, stat
        else: ret = rq, stat
        return  ret


    # get returns a record from the queue
    # volume may be specified for read queue
    # volume family may be specified for write queue
    # "optional" location parameter for read requests indicates
    # what is current position of the tape (location_cookie)
    # for write requests it is file size
    def get(self,label='',location=''):
        # label is either a volume label or file family
        if not label: return None
        if not self.queue.has_key(label):
            Trace.trace(13,"Queue.get: label %s is not in the queue"%(label,))
            return None
        sublist = self.queue[label]['opt']
       
        if location:
            # location means location cookie for read request
            # and file size for write request
            record = sublist.get(location)
        else:
            if self.queue_type == 'read_from_hsm': record = sublist.get()
            else: record = self.queue[label]['by_priority'].get()
        Trace.trace(13,"Queue.get: %s"%(repr(record),))
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

        # remove opt entry
        Trace.trace(13,"Queue.delete: opt %s %s"%(key, request.ticket))
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
        if ret: ret = self.queue[key]['by_priority'].change_pri(request, pri)
        return ret
            
    def get_next(self, label=''):
        # label is either a volume label or volume family
        if not label: return None
        if not self.queue.has_key(label):
            Trace.trace(13,"Queue.get: label %s is not in the queue"%(label,))
            return None
        sublist = self.queue[label]['opt']
        return sublist.get_next()
        

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

    # get all entries from the queue
    def get_queue(self):
        list = []
        for key in self.queue.keys():
            list = list+self.queue[key]['by_priority'].get_tickets()
        return list

    # find record in the queue
    def find(self,id,output_file_name=None):
        for key in self.queue.keys():
            record, status = self.queue[key]['by_priority'].find(id,output_file_name)
            if record: break
        else:
            record = None
        return record
        
class Request_Queue:
    def __init__(self, aging_quantum=60, adm_pri_to=60):
        self.write_queue = Queue(aging_quantum)
        self.read_queue = Queue(aging_quantum)
        # sorted list of volumes for read requests
        # and volume families for write requests
        # based on this list the highest priority volume
        # or volume family is selected
        # only one request per volume or volume family
        self.tags = SortedList(compare_priority, 1, aging_quantum)
        # volume or file family references for fast search
        self.ref = {}
        # administration priority queue
        self.adm_queue = SortedList(compare_priority, 1, aging_quantum)
        
        self.aging_quantum = aging_quantum
        # if this time out expires admin priority request
        # can overrirde any request even if it was for a
        # mounted volume
        self.adm_pri_to = adm_pri_to
        self.adm_pri_t0 = 0.

    def update(self, request, key):
        if self.ref.has_key(key):
            # see if priority of current requst is higher than
            # request in the queue and if yes remove it
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

    def put(self,ticket,t_time=0):
        if ticket['encp']['adminpri'] > -1:
            # high priority request. Put into admin queue
            rq = Request(ticket['encp']['basepri'], 0, ticket, ticket['times']['t0'])
            if not self.adm_queue.sorted_list:
                self.adm_pri_t0 = time.time()
            res, stat = self.adm_queue.put(rq)
            if res: return None, stat
            else: return rq, stat
        if ticket['work'] == 'write_to_hsm':
            # combine volume family
            if ticket['vc']['file_family'] != 'ephemeral':
                key = string.join((ticket['vc']['storage_group'],
                       ticket['vc']['file_family'],
                       ticket['vc']['wrapper']),'.')
            else: key = string.join((ticket['vc']['storage_group'],'ephemeral','none'), '.')
            ticket['vc']['volume_family'] = key
            rq, stat = self.write_queue.put(ticket,t_time)
            if not rq: return rq, stat
            queue = self.write_queue
        elif ticket['work'] == 'read_from_hsm':
            key = ticket['fc']['external_label']
            rq, stat = self.read_queue.put(ticket,t_time)
            if not rq: return rq, stat
            queue = self.read_queue
        else:
            return 0, e_errors.WRONGPARAMETER

        # now get the highest priority request from the queue
        # where request went
        #hp_rq = queue.get(key)
        #if hp_rq and rq != hp_rq:
        Trace.trace(14,"put: calling update %s"%(rq,)) ## REMOVE THIS
        self.update(rq,key)
        rc = rq, e_errors.OK
        #else: rc = None, e_errors.UNKNOWN
        return rc
    
    # delete the record
    def delete(self,record):
        if record.ticket['encp']['adminpri'] > -1:
            # it must be in the admin queue
            self.adm_queue.delete(record)
            return
        if record.work == 'write_to_hsm':
            label = record.ticket['vc']['volume_family']
            queue = self.write_queue
        if record.work == 'read_from_hsm':
            label = record.ticket['fc']['external_label']
            queue = self.read_queue
        queue.delete(record)
        self.tags.delete(record, label)
        if record == self.ref[label]:
            # if record is in the reference list (and hence in the
            # tags list) these enrires must be replaced by record
            # with the same label if exists
            del(self.ref[label])
            # find the highest priority request for this
            # label
            hp_rq = queue.get(label)
            if hp_rq: self.update(hp_rq,label)

    def get_admin_request(self, next=0):
        if next:
            return self.adm_queue.get_next()
        return self.adm_queue.get()
    
    # get returns a record from the queue
    # volume may be specified for read queue
    # volume family may be specified for write queue
    # "optional" location parameter is meaningful only for
    # read requests and indicates what is current position
    # of the tape (location_cookie)
    # flag next indicates whether to get a first highest request
    # or keep getting requsts for specified label
    def get(self, label='',location='', next=0, use_admin_queue=1):
        if label:
            if use_admin_queue:
                # get came with label info, hence it is from
                # have bound volume
                # see if there is a time to check hi_pri requests
                # even if they are not for this label
                now = time.time()
                if ((now - self.adm_pri_t0 >= self.adm_pri_to) and
                    self.adm_queue.sorted_list):
                    # admin request is highest no matter what
                    if next: rq = self.adm_queue.get_next()
                    else: rq = self.adm_queue.get()
                    self.adm_pri_t0 = now
                    if rq: return rq

            # see if label points to write queue
            if label in self.ref.keys():
                if next: record = self.write_queue.get_next(label)
                else: record = self.write_queue.get(label, location)
                # see if label points to read queue
                if not record:
                    if next: record = self.read_queue.get_next(label)
                    else:
                        record = self.read_queue.get(label, location)
                        #if not location: record = self.read_queue.get(label)
                        #else: record = self.read_queue.get(label, location)
            else: record = None
            #return record
        else:
            if use_admin_queue:
                # check admin request queu first
                rq = self.adm_queue.get()
                if rq: return rq
            
            # label is not specified, get the highest priority from
            # the tags queue
            if next:
                for r in self.tags.sorted_list:
                    Trace.trace(11, "TAG %s" % (r,))
                rq = self.tags.get_next()
                Trace.trace(11,"NEXT %s" % (rq,))
            else: rq = self.tags.get()
            if rq:
                if rq.work == 'write_to_hsm':
                    label = rq.ticket['vc']['volume_family']
                    record = self.write_queue.get(label)
                if rq.work == 'read_from_hsm':
                    label = rq.ticket['fc']['external_label']
                    if not location: record = self.read_queue.get(label)
            else: record = rq
        return record

    def get_queue(self):
        return self.adm_queue.get_tickets() + \
               self.write_queue.get_queue() + \
               self.read_queue.get_queue()
        
    # find record in the queue
    def find(self,id,output_file_name=None):
        record, status = self.adm_queue.find(id,output_file_name)
        if not record:
            record = self.write_queue.find(id,output_file_name)
        if not record:
            record = self.read_queue.find(id,output_file_name)
        return record

    # change priority
    def change_pri(self, record, pri):
        if record.ticket['encp']['adminpri'] > -1:
            # it must be in the admin queue
            return self.adm_queue.change_pri(record, pri)
        if record.work == 'write_to_hsm':
            label = record.ticket['vc']['volume_family']
            queue = self.write_queue
        if record.work == 'read_from_hsm':
            label = record.ticket['fc']['external_label']
            queue = self.read_queue
        ret = queue.change_pri(record, pri)
        if not ret: return ret
        self.update(record,label)
        #self.tags.delete(record)
        """
        if cmp(id(record), id(self.ref[label])) == 0:
            # if record is in the reference list (and hence in the
            # tags list) these enries must be replaced by record
            # with the same label if exists
            del(self.ref[label])
            # find the highest priority request for this
            # label
            hp_rq = queue.get(label)
            if hp_rq: self.update(hp_rq,label)
        """
        return ret

    def wprint(self):
        print "+++++++++++++++++++++++++++++++"
        print "ADMIN QUEUE",self.adm_queue.wprint()
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
         
if __name__ == "__main__":
  import pprint
  import os
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
  res = pending_work.put(t1, t1["times"]["t0"])
  print "RESULT",res
  #pending_work.wprint()
  
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
  res = pending_work.put(t2, t2["times"]["t0"])
  print "RESULT",res
  #pending_work.wprint()
  
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
  res = pending_work.put(t3, t3["times"]["t0"])
  print "RESULT",res
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
  t4['fc']['external_label'] = 'vol1'
  t4['fc']['location_cookie'] = '5'
  res = pending_work.put(t4, t4["times"]["t0"])
  print "RESULT",res
  #pending_work.wprint()

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
  res = pending_work.put(t5, t5["times"]["t0"])
  print "RESULT",res
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
  res = pending_work.put(t6, t6["times"]["t0"])
  print "RESULT",res
  pending_work.wprint()


  #os._exit(0)
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
  res = pending_work.put(t7, t7["times"]["t0"])
  print "RESULT",res
  #pending_work.wprint()
  pending_work.wprint()

  t8={}
  t8["encp"]={}
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
  res = pending_work.put(t8, t8["times"]["t0"])
  print "RESULT",res

  pending_work.wprint()
  #os._exit(0)
  print "enter volume: ",
  vol = raw_input()
  print "VOL",vol
  print "enter location: ",
  loc = raw_input()
  print "LOC",loc
  rq = pending_work.get()
  #print "RQ",rq
  #os._exit(0)
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
  
  """
  print "GGGGGGGGGGGGGGGGGGGGGGGGGGGGG"
  """
  v = pending_work.get(0,300)
  v.pr()
  #v = pending_work.f_get(t2["encp"]["basepri"])
  v = pending_work.get()
  v.pr()
  v = pending_work.get()
  v.pr()
