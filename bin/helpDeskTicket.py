#!/usr/bin/env python

import string
import sys
import time

# the next line causes the program to traceback and exit if it is not called properly
thefile=sys.argv[1]
f = open(thefile,"r")
o = open(thefile+'.rtf',"w")

start=0
while 1:
    line = f.readline()
    if not line: break
    if not start:
        print line[:6]
        if line[:6] != "{\\rtf1":
            continue
        else:
            start=1
    size=len(line)
    if line[size-2:size-1] == '=':
        o.write("%s"%(line[:size-2],))
    else:
        o.write("%s"%(line,))
o.close()
f.close()

