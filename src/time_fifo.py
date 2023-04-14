#!/usr/bin/env python

# $Id$

import time

class time_fifo:

    def __init__(self):
        self.times = {} # timestamps for each item
        self.seq = [] #keeps items in time-order
        
    def update(self, item):
	"""update(item) adds item to the end of the fifo
	   or moves it to the end of the fifo if already present"""
        now = time.time()
        if item in self.seq:
            self.seq.remove(item)
        self.seq.append(item)
        self.times[item]=now

    def get_oldest(self):
        """remove the oldest item from the queue, and return it"""
        if not self.seq:
            return None
        item = self.seq.pop(0)
        del self.times[item]
        return item
    
    def oldest_time(self):
        """ get the time of the oldest item on the fifo"""
        if not self.seq:
            return None
        return self.times[self.seq[0]]

    def __len__(self):
        return len(self.seq)

    def __nonzero__(self):
        return len(self.seq)>0
    
    def __repr__(self):
        s='['
        for l in self.seq:
            if s!='[':
                s=s+', '
            s = s+'%s:%s'%(l, self.times[l])
        s=s+']'
        return s
    
if __name__ == "__main__":
    q = time_fifo()

    for x in xrange(30):
        q.update(x)
        time.sleep(1)
        print q
        now = time.time()
        while now - q.oldest_time() > 5:
            print "get_oldest", q.get_oldest()

    while q:
        print q
        print "get_oldest", q.get_oldest()

