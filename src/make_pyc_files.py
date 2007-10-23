#!/usr/bin/env python

###############################################################################
# $Id:
#
##############################################################################
import os
import sys


s=sys.argv[0]
a=s.split('/')
b = a[len(a)-1]
myname=b.split('.')[0]
print "my name", myname

dir_list=os.listdir(".")

for f in dir_list:
    a = f.split('.')
    if a[len(a)-1] == 'py':
        #print f
        del a[len(a)-1]
        s=''
        module = s.join(a)
        if myname != module and module.find("cgi")==-1 and module.find("label_tape")==-1:
            print "will import", module
            try:
                __import__(module)
            except:
                pass
            
        
