#!/usr/bin/env python

#$Id$

# These 2 files are not in the enstore repository due to privacy concerns
import dev_phones
import dev_shift

import os
import string
import sys
import time

def getNumber(developer):
    if not dev_phones.phones.has_key(developer):
        dev = string.lower(developer)
        if dev_phones.alias.has_key(dev):
            developer = dev
        else:
            developer = dev_phones.callme
    return (developer, dev_phones.phones.get(developer))

def getDeveloper(date):
    if not dev_shift.shifts.has_key(date):
        date = dev_shift.unknown
    return dev_shift.shifts.get(date)

def getMailAddress(developer):
    x,add = getNumber(developer)
    return add[2]

def getInfo(today):
    date = today[4:]
    (primary,backup)= getDeveloper(date)
    primary_phone = getNumber(primary)
    backup_phone = getNumber(backup)
    p = "%s: Primary is %-9s   %s" % (today,primary,primary_phone[1])
    b = "%s: Backup  is %-9s   %s" % (today,backup,  backup_phone[1])
    return (p,b)

if __name__ == "__main__":
    now = time.time()
    today =  time.asctime(time.localtime(now))[0:10]
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        choice = "date"

    fshift = "ens-dev-shift"
    fweek  = "ens-dev-week"
    http   = "http://www-hppc.fnal.gov/enstore"
    
    if choice == "schedule":
        remlist = '/tmp/'+fshift
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

    elif choice == "week":
        remweek = '/tmp/'+fweek
        remfile = open(remweek,'w')
        remfile.write('Enstore developer schedule for the next week\n\n')
        for day in range(1,9):
            today = time.asctime(time.localtime(now+day*86400))[0:10]
            p, b = getInfo(today)
            remfile.write("%s\n" % (p,))
            remfile.write("%s\n\n" % (b,))
        remfile.write("\nThis is the planned schedule only. Check on-call for up-to-date information\n")
        remfile.write("\nA monthly schedule is available at %s/%s.gif\n" % (http,fshift))
        remfile.close()
        users = ""
        for user in dev_phones.phones.keys():
            users = users+" "+getMailAddress(user)
        command = '/usr/bin/Mail -s "%s Enstore Developer Weekly Schedule" %s < %s' % (today, users, remweek)
        os.popen(command,'r').readlines()


    else:
        date = today[4:]
        (primary,backup)= getDeveloper(date)
        primary_phone = getNumber(primary)
        backup_phone = getNumber(backup)
        p, b = getInfo(today)


        if choice == 'mail':
            remmail = '/tmp/ens-dev-mail'
            remfile = open(remmail,'w')
            remfile.write("\n Enstore Developer Schedule \n")
            remfile.write("%s\n" % (p,))
            remfile.write("%s\n\n" % (b,))
            remfile.write("\nA monthly schedule is available at %s/%s.gif\n" % (http,fshift))
            remfile.close()
            users = ""
            for user in (primary,backup):
                users = users+" "+getMailAddress(user)

            command = '/usr/bin/Mail -s "%s You are on enstore developer shift" %s < %s' % (today, users, remmail)
            os.popen(command,'r').readlines()

        else:
            print "%s" % (p,)
            print "%s" % (b,)
