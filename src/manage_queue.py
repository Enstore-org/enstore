# manage the pending work queue
import time
import string

class Queue:
   queue=[]
   queue_ptr=0

   # return the priority of a ticket
   def priority(self,ticket):
      if ticket["encp"]["adminpri"] >= 0:
	  return ticket["encp"]["adminpri"]
      p = ticket["encp"]["basepri"]
      deltas=0
      if ticket["encp"]["agetime"] > 0:
         # deltas is the number of delta time periods 
         deltas=int(time.time()-ticket["times"]["t0"])/60/ticket["encp"]["agetime"]
         p=p + ticket["encp"]["delpri"]*deltas
      #print p,ticket["encp"]["basepri"],deltas,ticket["encp"]["agetime"],ticket["encp"]["delpri"],time.time(),ticket["times"]["t0"]
      ticket["encp"]["curpri"]=p
      return p
   
   # A call back for sort, highest priority should be first.
   def compare_priority(self,t1,t2):
      if t1["encp"]["curpri"] > t2["encp"]["curpri"]:
         return 1
      if t1["encp"]["curpri"] < t2["encp"]["curpri"]:
         return -1
      # if priority is equal, then time in rules
      if t1["times"]["t0"] > t2["times"]["t0"]:
         return 1
      return -1
   
   # Add work to the end of the set of jobs
   def insert_job(self,ticket):
      ticket['times']['job_queued'] = time.time()
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

   # Return the next highest priority job.  get_init must be called first
   def get_next(self):
      self.queue_ptr=self.queue_ptr+1
      if len(self.queue) > self.queue_ptr:
          return self.queue[self.queue_ptr]
      return

   # return the entire sorted queue with current priorities for reporting
   def get_queue(self):
      for w in self.queue:
         w["encp"]["curpri"] = self.priority(w)
      self.queue.sort(self.compare_priority)
      return self.queue

if __name__ == "__main__":
  import manage_queue
  import pprint
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

  n=10
  while n:
    w=pending_work.get_init()
    print w["encp"]["pri"]
    while w:
      w=pending_work.get_next()
    time.sleep(30)
    n=n-1
  print "delete t1"
  pending_work.delete_job(t1)
# get the whole list of jobs 
  print "get_queue"
  pprint.pprint(pending_work.get_queue())
