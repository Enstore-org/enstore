#!/usr/bin/env python

#$Id$

# These 2 files are not in the enstore repository due to privacy concerns
import dev_phones
import dev_shift

import os
import sys
import time

def getNumber(developer):
    if not dev_phones.phones.has_key(developer):
        developer = dev_phones.callme
    return (developer, dev_phones.phones.get(developer))

def getDeveloper(date):
    if not dev_shift.shifts.has_key(date):
        date = dev_shift.unknown
    return dev_shift.shifts.get(date)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        choice = "date"

    if choice == "schedule":
        remlist = '/tmp/ens-dev-shift'
        webdir = '/usr/hppc_home/www/enstore'
        remfile = open(remlist,'w')
        remfile.write("SET Jan 1\n")
        remfile.write("SET Feb 2\n")
        remfile.write("SET Mar 3\n")
        remfile.write("SET Apr 4\n")
        remfile.write("SET May 5\n")
        remfile.write("SET Jun 6\n")
        remfile.write("SET Jul 7\n")
        remfile.write("SET Aug 8\n")
        remfile.write("SET Sep 9\n")
        remfile.write("SET Oct 10\n")
        remfile.write("SET Nov 11\n")
        remfile.write("SET Dec 12\n")
        for date in dev_shift.shifts.keys():
            (primary,backup)= getDeveloper(date)
            # REM May 22      MSG    %"Jon%"
            remfile.write('REM %s MSG %%"%s%%"\n' % (date,primary,))
            remfile.write('REM %s MSG %%"%s%%"\n' % (date,backup, ))
        remfile.close()
        command = "rm -f %s.ps; nice -n 19 remind -p2 %s | rem2ps -se 16 -e -c0 >%s.ps; nice -n 19 convert +append %s.ps %s.gif; cp %s.ps %s.gif %s" %\
                  (remlist,remlist,remlist,remlist,remlist,remlist,remlist,webdir)
        os.popen(command,'r').readlines()
    else:
        today =  time.asctime(time.localtime(time.time()))[0:10]
        date = today[4:]
        (primary,backup)= getDeveloper(date)
        primary_phone = getNumber(primary)
        backup_phone = getNumber(backup)
        print "%s: Primary is %-9s   %s" % (today,primary,primary_phone[1])
        print "%s: Backup  is %-9s   %s" % (today,backup,  backup_phone[1])
