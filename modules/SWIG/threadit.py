#!/usr/bin/env python

import sys, string

leadin = 'static PyObject *_wrap_'

fname = 'no function yet'

for line in sys.stdin.readlines():
    if line[-1]=='\n':
        line = line[:-1]
    if line[:len(leadin)]==leadin:
        print line
        fname = line[len(leadin):]
        while fname[-1] != '(':
            fname = fname[:-1]
        fname = fname[:-1]
    elif string.find(line, fname+'(')>0:
        print "Py_BEGIN_ALLOW_THREADS"
        print line
        print "Py_END_ALLOW_THREADS"
    else:
        print line
    

        
        
