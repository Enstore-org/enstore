# manage the pending work queue
import time
#import string

import generic_cs

class Queue:
   queue=[]
   queue_ptr=0

   # return the priority of a ticket
   def priority(self,ticket):
      if 0: print self.keys() # lint fix
      if ticket["encp"]["adminpri"] >= 0:
	  return ticket["encp"]["adminpri"]
      p = ticket["encp"]["basepri"]
      deltas=0
      if ticket["encp"]["agetime"] > 0:
         # deltas is the number of delta time periods 
         deltas=int(time.time()-ticket["times"]["job_queued"])/60/ticket["encp"]["agetime"]
         p=p + ticket["encp"]["delpri"]*deltas
      #self.enprint(p+" "+repr(ticket["encp"]["basepri"])+" "+repr(deltas)+\
      #            " "+repr(ticket["encp"]["agetime"])+" "+\
      #            repr(ticket["encp"]["delpri"])+\
      #            " "+repr(time.time())+" "+repr(ticket["times"]["t0"]))
      ticket["encp"]["curpri"]=p
      return p
   
   # A call back for sort, highest priority should be first.
   def compare_priority(self,t1,t2):
      if 0: print self.keys() # lint fix
      if t1["encp"]["curpri"] < t2["encp"]["curpri"]:
         return 1
      if t1["encp"]["curpri"] > t2["encp"]["curpri"]:
         return -1
      # if priority is equal, then time in rules
      if t1["times"]["t0"] > t2["times"]["t0"]:
         return 1
      return -1
   
   # A call back for sort, highest file location should be first.
   def compare_location(self,t1,t2):
       if 0: print self.keys() # lint fix
       if t1["fc"]["external_label"] == t2["fc"]["external_label"]:
	   if t1["fc"]["location_cookie"] > t2["fc"]["location_cookie"]:
	       return 1
	   if t1["fc"]["location_cookie"] < t2["fc"]["location_cookie"]:
	       return -1
       return -1
   
   # Add work to the end of the set of jobs
   def insert_job(self,ticket):
      ticket['times']['job_queued'] = time.time()
      #print "REQ LC:", ticket['fc']['location_cookie']
      ticket['at_the_top'] = 0
      self.queue.append(ticket)
   
   # Remove a ticket 
   def delete_job(self,ticket):
      for w in self.queue:
         if w["unique_id"] == ticket["unique_id"]:
            if ticket['times'].has_key('job_queued'):
                ticket['times']['in_queue'] = time.time() - \
                                              ticket['times']['job_queued']
                del(ticket['times']['job_queued'])
            else:
                ticket['times']['in_queue'] = 0
            self.queue.remove(w)
            return

   # Find a job 
   def find_job(self,id):
       for w in self.queue:
	   if w["unique_id"] == id:
	       return w
       return None

   # change a job priority
   def change_pri(self,id, pri):
       # priority cannot be less than 0
       if pri < 0:
	   return None
       for w in self.queue:
	   if w["unique_id"] == id:
	       w["encp"]["curpri"] = pri
	       w["encp"]["basepri"] = pri
	       return w
       return None

   # Make a prioritized list of the jobs, and return the top one
   # This is done by calculating current priority of each job and sorting
   def get_init(self):
      self.queue_ptr=0
      if len(self.queue) == 0:				# There are no jobs
          return
      for w in self.queue:
         w["encp"]["curpri"] = self.priority(w)
      self.queue.sort(self.compare_priority)		# Sort the jobs by priority
      return self.queue[0]				# Return the top one

   # Sort jobs by location for the given volume and return the top one
   def get_init_by_location(self):
      self.queue_ptr=0
      if len(self.queue) == 0:			    # There are no jobs
          return
      self.queue.sort(self.compare_location)	    # Sort the jobs by location
      return self.queue[0]			    # Return the top one

   # Return the next highest priority job.  get_init must be called first
   def get_next(self):
      self.queue_ptr=self.queue_ptr+1
      if len(self.queue) > self.queue_ptr:
	  self.queue[self.queue_ptr]['at_the_top'] = \
	  self.queue[self.queue_ptr]['at_the_top'] + 1
          return self.queue[self.queue_ptr]
      return

   # Get next job for the given volume, note that get_init_by_location must be
   # before calling this function
   def get_next_for_this_volume(self, v):
      for i in range (0, len(self.queue)):
	  if self.queue[i]["work"] == "read_from_hsm":
	      if self.queue[i]['vc']['external_label'] == \
		 v['vc']["external_label"]:
		  if self.queue[i]['fc']['location_cookie'] > \
		     v['vc']['current_location']:
		      self.queue[i]['at_the_top'] = self.queue[i]['at_the_top']+1
		      return self.queue[i]

      # no match has been found, return first for this volume
      for i in range (0, len(self.queue)):
	  if self.queue[i]['vc']['external_label'] ==v['vc']["external_label"]:
	      self.queue[i]['at_the_top'] = self.queue[i]['at_the_top']+1
	      self.queue[i]['status'] = v['vc']['status']
	      return self.queue[i]
      return

      

   # return the entire sorted queue with current priorities for reporting
   def get_queue(self):
      for w in self.queue:
         w["encp"]["curpri"] = self.priority(w)
      self.queue.sort(self.compare_priority)
      return self.queue

if __name__ == "__main__":
  import manage_queue
  pending_work = manage_queue.Queue()
  t1={}
  t1["encp"]={}
  t1["times"]={}
  t1["unique_id"]=1
  t1["encp"]["basepri"]=100
  t1["encp"]["adminpri"]=-1
  t1["encp"]["delpri"]=100
  t1["encp"]["agetime"]=1
  t1["times"]["t0"]=time.time()
  pending_work.insert_job(t1)

  t2={}
  t2["encp"]={}
  t2["times"]={}
  t2["unique_id"]=2
  t2["encp"]["basepri"]=200
  t2["encp"]["adminpri"]=-1
  t2["encp"]["delpri"]=125
  t2["encp"]["agetime"]=2
  t2["times"]["t0"]=time.time()
  pending_work.insert_job(t2)

  t3={}
  t3["encp"]={}
  t3["times"]={}
  t3["unique_id"]=3
  t3["encp"]["basepri"]=300
  t3["encp"]["adminpri"]=-1
  t3["encp"]["delpri"]=0
  t3["encp"]["agetime"]=0
  t3["times"]["t0"]=time.time()
  pending_work.insert_job(t3)
  while 1:
    print pending_work.get_queue()
    time.sleep(10)
  n=10
  while n:
    w=pending_work.get_init()
    generic_cs.enprint(w["encp"]["pri"])
    while w:
      w=pending_work.get_next()
    time.sleep(30)
    n=n-1
  generic_cs.enprint("delete t1")
  pending_work.delete_job(t1)
# get the whole list of jobs
  generic_cs.enprint("get_queue")
  generic_cs.enprint(pending_work.get_queue(), generic_cs.PRETTY_PRINT)

