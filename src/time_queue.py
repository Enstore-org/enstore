#!/usr/bin/env python

import time

class time_queue:

    def __init__(self):
        self.times = {} # timestamps for each ob
        self.seq = [] #keeps obs in time-order
        
    def push(self, ob):
        """add a new item to the queue"""
        now = time.time()
        if ob in self.seq:
            self.seq.remove(ob)
        self.seq.append(ob)
        self.times[ob]=now

    def pop(self):
        """ pop the oldest item off the queue"
        if not self.seq:
            return None
        ob = self.seq.pop(0)
        del self.times[ob]
        return ob
    
    def oldest_time(self):
        """ get the time of the oldest item on the queue"""
        if not self.seq:
            return None
        return self.times[self.seq[0]]

    def __repr__(self):
        s='['
        for l in self.seq:
            if s!='[':
                s=s+', '
            s = s+'%s:%s'%(l, self.times[l])
        s=s+']'
        return s
    
if __name__ == "__main__":
    q = time_queue()

    for x in xrange(30):
        q.push(x)
        time.sleep(1)
        print q
        now = time.time()
        while now - q.oldest_time() > 5:
            print "pop", q.pop()
        
