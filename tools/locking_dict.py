#!/usr/bin/env python

##  $Id$
        
## Should re-do this using UserDict

DictionaryLockedError = "DictionaryLockedError"

            
class LockingDict:

    def __init__(self):
        self.dict = {}
        self._locked = 0

    def lock(self):
        self._locked = 1

    def unlock(self):
        self._locked = 0

    def locked(self):
        return self._locked
        
    def __getitem__(self,key):
        return self.dict[key]

    def __delitem__(self,key):
        if self._locked:
            raise DictionaryLockedError, ('__delitem__', key)
        else:
            del(self.dict[key])
    
    def __setitem__(self,key,value):
        if self._locked:
            raise DictionaryLockedError, ('__setitem__', key, value)

        self.dict[key]=value

    def __repr__(self):
        return repr(self.dict)

    def __str__(self):
        return str(self.dict)
    
    def __len__(self):
        return  len(self.dict)
    
    def __getattr__(self,attr):
        return getattr(self.dict,attr)

    
        
if __name__ == "__main__":

    d = LockingDict()
    d[1]=2
    d['a']=77
    print len(d)
    print d.keys()
    del d[1]
    d[1]='3'
    print 'locking'
    d.lock()
    print len(d)
    print d.keys()
    print d.locked()
    try:
        del d[0]
    except DictionaryLockedError, detail:
        print "caught exception",detail
    try:    
        d[1]=66
    except DictionaryLockedError, detail:
        print "caught exception",detail
    

       
