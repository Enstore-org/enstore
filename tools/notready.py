#!/usr/bin/env python

import sys
import os
import string

filelist = sys.argv[1:]

print "Checking files..."

for file in filelist[:]:

    working_revision = repository_revision = production_revision = '(none)'

    if string.find(file, 'CVS/') >= 0:
        print file, "in in /CVS/ directory. Can not possibly check it"
        continue
    
    if os.path.isdir(file):
        print file, "is a directory, ignoring"
        continue
    
    cvsinfo = os.popen('cvs status -v %s 2>/dev/null' % (file,)).readlines()
    L = len(cvsinfo)
    if not cvsinfo or L<6:
        print "A Cannot parse cvs output", file,cvsinfo, L
        sys.exit(1)

    if string.find(cvsinfo[4], "No revision control file\n") > 0:
        print file, "\t not in CVS, skipping"
        continue

    if L<10:
        print "B Cannot parse cvs output", cvsinfo, L
        sys.exit(1)
        
    if string.find(cvsinfo[1], 'Status: Up-to-date\n') < 0:
        out_of_date = cvsinfo[1]
    else:
        out_of_date = ""

    if string.find(cvsinfo[3], '   Working revision') != 0:
        print "Cannot find Working revision", cvsinfo
        sys.exit(1)
    tokens = string.split(cvsinfo[3])
    if len(tokens) <3:
        print "Cannot parse Working revision", cvsinfo[3], len(tokens),
        sys.exit(1)
    working_revision = tokens[2]

    if string.find(cvsinfo[4], '   Repository revision') != 0:
        print "Cannot find Repository revision", cvsinfo
        sys.exit(1)
    tokens = string.split(cvsinfo[4])
    if len(tokens) <3:
        print "Cannot parse Repository revision", cvsinfo
        sys.exit(1)
    repository_revision = tokens[2]
        
    found = 0
    for line in cvsinfo[10:L]:
        if string.find(line, "\tproduction ") >= 0:
            tokens = string.split(line)
            if len(tokens) <3:
                print "Cannot parse production", line
                sys.exit(1)
            production_revision = tokens[2][0:-1] # strip off trailing }
            found = 1
            break

    if repository_revision != production_revision or working_revision != production_revision or out_of_date:
        print '%s\t Working revision=%s  Repository revision=%s  Production revision=%s'%(file,working_revision, repository_revision, production_revision), out_of_date
    else:
        print '%s\t ok'%(file,)

### #!/bin/sh
### 
### 
### cd /home/bakken
### 
### output=`pwd`/repository-production
### rm -f $output
### 
### dirs=/tmp/dirs
### rm -f $dirs
### 
### rm -fr lz >/dev/null 2>&1
### mkdir lz  >/dev/null 2>&1
### cd lz
### 
### export CVSROOT=cvsuser@hppc.fnal.gov:/cvs/hppc
### cvs co enstore2 >/dev/null 2>&1
### 
### date >> $output
### echo "List of files in $CVSROOT without production tag" >> $output
### 
### #find enstore2 -type f -exec /home/bakken/lz/enstore2/tools/notready.py {} \; | egrep -v 'Checking files...|ok$|not in CVS, skipping$|is a directory, ignoring|in in /CVS/ directory. Can not possibly check it' >> $output
### 
### find enstore2 -type d |grep -v CVS > $dirs
### cat $dirs | while read d; do 
###  (cd $d; /home/bakken/lz/enstore2/tools/notready.py *| egrep -v 'Checking files...|ok$|not in CVS, skipping$|is a directory, ignoring|in in /CVS/ directory. Can not possibly check it' >> $output )
### done
### 
### date >> $output
### 
### /usr/bin/Mail -s "Nonproduction files in cvs repository" enstore-auto@fnal.gov <$output

