#!/usr/bin/env python

import sys
import os
import string

filelist = sys.argv[1:]

print "Checking files..."

for file in filelist[:]:
    if os.path.isdir(file):
        print file, "is a directory, ignoring"
        filelist.remove(file)
        continue
    
    print file
    cvsinfo = os.popen('cvs status %s' % (file,)).readlines()
    if not cvsinfo or len(cvsinfo)<6:
        print "Cannot parse cvs output", cvsinfo
        sys.exit(1)

    if string.find(cvsinfo[4], "No revision control file\n") > 0:
        print file, "not in CVS, skipping"
        filelist.remove(file)
        continue

    if len(cvsinfo)<8:
        print "Cannot parse cvs output", cvsinfo
        sys.exit(1)
        
    if string.find(cvsinfo[1], 'Status: Up-to-date\n') < 0:
        print "%s is not up to date" % (file,)
        sys.exit(1)

    for line in cvsinfo[5:7]:
        if string.find(line, "(none)\n") < 0:
            print "%s %s" %(file, line)
            sys.exit(1)
        
    if file[-3:] == '.py':
        pipe = os.popen('%s/tools/mylint.py %s' % (os.environ["ENSTORE_DIR"], file), 'r')
        text = pipe.readlines()
        ret = pipe.close()
        if ret:
            for line in text[1:]:
                print line,
            print file, "does not pass lint, exiting"
            sys.exit(1)
        ret = os.system('%s/tools/check_pct.py -w %s' % (os.environ["ENSTORE_DIR"], file))
        if ret:
            print file, "does not pass check_pct, Continuing - but you need to FIX THIS!!!"
        ret = os.system('%s/sbin/pychecker %s' % (os.environ["ENSTORE_DIR"], file))
        if ret:
            print file, "does not pass pychecker, Continuing - but you need to FIX THIS!!!"

    else:
        print "not a python file, continuing"

if filelist:    
    print "Applying tag production..."
else:
    print "No files to tag!"
if 0:
 err = 0
 for file in filelist:
    cmd = "cvs tag -F production %s" % (file,)
    ret = os.system(cmd)
    if ret:
        print cmd, "failed"
        err = 1

print "Done, status=", err
sys.exit(err)

    
