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

    # if the head of the repository and the production version are not only 1 apart, ask
    versions = os.popen("cvs -q log -h %s | sed -e 's/.*production: //p' -e 's/^head: //p' -e d"%(file,)).readlines()
    # remove carriage return. line feed
    head = string.replace(versions[0], "\012", "")
    if len(versions) == 2:
	production = string.replace(versions[1], "\012", "")
    else:
	production = None
    if production:
	ihead = int(string.split(head, '.')[1])
	iproduction = int(string.split(production, '.')[1])
	if ihead - iproduction > 1:
	    # check if the user wants to proceed
	    sys.stdout.write("\n%s has head version %s and production version %s, continue? [y/n def:n] "%(file, head, production))
	    ans = sys.stdin.readline()
	    ans = string.replace(ans, "\012", "")
	    if ans not in ['y', 'Y']:
		sys.exit(1)

    if file[-3:] == '.py':
        print '\nchecking',file,'with mylint'
        pipe = os.popen('%s/tools/mylint.py %s' % (os.environ["ENSTORE_DIR"], file), 'r')
        text = pipe.readlines()
        ret = pipe.close()
        if ret:
            for line in text[1:]:
                print line,
            print file, "does not pass lint, exiting"
            sys.exit(1)
        #print '\nchecking',file,'with check_pct.py'
        #ret = os.system('%s/tools/check_pct.py -w %s' % (os.environ["ENSTORE_DIR"], file))
        #if ret:
        #    print file, "does not pass check_pct, Continuing - but you need to FIX THIS!!!"
        print '\nchecking',file,'with pychecker'
        ret = os.system('%s/sbin/pychecker %s' % (os.environ["ENSTORE_DIR"], file))
        if ret:
            print file, "does not pass pychecker, Continuing - but you need to FIX THIS!!!"

    else:
        print "not a python file, continuing"

if filelist:    
    print "Applying tag production..."
else:
    print "No files to tag!"
err = 0
for file in filelist:
    cmd = "cvs tag -F production %s" % (file,)
    ret = os.system(cmd)
    if ret:
        print cmd, "failed"
        err = 1

print "Done, status=", err
sys.exit(err)

    
