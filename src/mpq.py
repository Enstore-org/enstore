#!/usr/bin/env python
"""A priority queue which supports multiple comparison methods"""


__rcsid__="$Id"


# Bisection algorithms
# Insert item x in list a, and keep it sorted assuming a is sorted
# This code copied from bisect.py but modified to use a comparison
# function rather than the "<" operator, which invokes an object's
# __cmp__ method.  We want to be able to put the *same* object
#into multiple queues, without changing the object's class definition
#to overload __cmp__

def _insort(a, x, compare, lo=0, hi=None):
    if hi is None:
        hi = len(a)
    while lo<hi:
        mid = (lo+hi)/2
        if compare(x, a[mid])<0:
            hi = mid
        else: lo = mid+1
    a.insert(lo, x)


# Find the index where to insert item x in list a, assuming a is sorted

def _bisect(a, x, compare, lo=0, hi=None):
    if hi is None:
        hi = len(a)
    while lo < hi:
        mid = (lo+hi)/2
        if compare(x,a[mid])<0: hi = mid
        else: lo = mid+1
    return lo


class MPQ:

    def __init__(self, comparator):
        self.items = []
        self.comparator = comparator

    def insort(self, item):
        _insort(self.items, item, self.comparator)

    def bisect(self, item):
        return _bisect(self.items, item, self.comparator)
    def remove(self, item):
        print "MPQ_ITEMS",self.items
        print "MPQ_REMOVE",item
        self.items.remove(item)
        print "MPQ_ITEMS AFTER",self.items 

    def __getitem__(self, index):
        return self.items[index]
    
    def __len__(self):
        return len(self.items)

    def __nonzero__(self):
        return len(self)!=0

    def __repr__(self):
        return str(self.items)
        
##test case    
if __name__ == "__main__":

    class Req:
        def __init__(self,size,priority):
            self.size=size
            self.priority=priority

        def __repr__(self):
            return "<size=%s, priority=%s>" % (self.size, self.priority)

    def compare_priority(r1,r2):
        return -cmp(r1.priority, r2.priority)

    def compare_size(r1,r2):
        return cmp(r1.size,r2.size)

            
    r1=Req(size=1,priority=10)
    r2=Req(size=10,priority=1)
    r3=Req(size=5, priority=5)
    r4=Req(size=1, priority=1)

    
    q1=MPQ(compare_priority)
    q2=MPQ(compare_size)

    for r in r1,r2,r3,r4:
        q1.insort(r)
        q2.insort(r)

    print "q1, sorted by priority:", q1
    print "q2, sorted by size:", q2


    print "Now let's find the smallest size element, and delete it from both queues"
    e=q2[0]
    print "smallest is", e
    q1.remove(e)
    q2.remove(e)
    print "Now the queues are:"
    
    print "q1, sorted by priority:", q1
    print "q2, sorted by size:", q2


