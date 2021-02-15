#!/usr/bin/env python

###############################################################################
# $Id:
#
##############################################################################
from __future__ import print_function
import os
import sys


s = sys.argv[0]
a = s.split('/')
b = a[len(a) - 1]
myname = b.split('.')[0]
print("my name", myname)

dir_list = os.listdir(".")

for f in dir_list:
    a = f.split('.')
    if a[len(a) - 1] == 'py':
        #print f
        del a[len(a) - 1]
        s = ''
        module = s.join(a)
        if myname != module and module.find("cgi") == -1 and module.find("label_tape") == -1 and \
            module.find("set_lm_noread") == -1 and module.find("weekly_summary_report") == -1 and \
            module.find("acc_daily_summary") == -1 and module.find("tab_flipping_nanny") == -1 and \
                module.find("operation") == -1:
            print("will import", module)
            try:
                __import__(module)

            except BaseException:
                pass
os.system("chmod 755 *.pyc")
