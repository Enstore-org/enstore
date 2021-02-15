#!/usr/bin/env python

from __future__ import print_function
import operation
import time
import sys
import os
import getopt

LIMIT = 10

one_day = 60 * 60 * 24
seven_days = one_day * 7

TMP_FILE = '/tmp/tab_flipping_nanny.tmp'

if __name__ == '__main__':
    # parse command line arguments
    library = None
    output = None
    auto = False
    opts, args = getopt.getopt(sys.argv[1:], "l:o:a", [
                               "library=", "output=", "auto"])
    for o, a in opts:
        if o in ["-l", "--library"]:
            library = a
        elif o in ["-o", "--output"]:
            output = a
        elif o in ["-a", "--auto"]:
            auto = True

    if auto:
        output = os.path.join(
            operation.csc.get("inventory", {}).get(
                "inventory_rcp_dir").split(':')[1],
            "TAB_FLIPPING_WATCH")
        if library is not None:
            output = output + '-' + library

    stdout_save = sys.stdout
    if not output and len(args) > 0:
        output = args[0]
    if output:
        sys.stdout = open(output, "w")

    if library:
        dl = library
    else:
        dl = operation.DEFAULT_LIBRARIES

    print("Recommended tap flipping jobs on %s (%s)" %
          (operation.cluster, dl), time.ctime(time.time()))
    print()
    print("Recommended write protect on jobs:")
    print()
    if library:
        res = operation.recommend_write_protect_job(
            library=library, limit=1000000)
    else:
        res = operation.recommend_write_protect_job(limit=1000000)
    for i in res:
        operation.show_cap(i, res[i])
    total = 0
    oncaps = len(res)
    for i in res.keys():
        total = total + len(res[i])
    print("%d tapes in %d caps" % (total, oncaps))
    print()
    # check last time a ticket was cut
    if library:
        onltt = operation.get_last_write_protect_on_job_time(l=library)
    else:
        onltt = operation.get_last_write_protect_on_job_time()
    if time.time() - onltt > seven_days:
        print(
            "==> Last ticket was cut 7 or more days ago ...",
            time.ctime(onltt))
        print()
    print("Recommended write protect off jobs:")
    print()
    if library:
        res = operation.recommend_write_permit_job(
            library=library, limit=1000000)
    else:
        res = operation.recommend_write_permit_job(limit=1000000)
    for i in res:
        operation.show_cap(i, res[i])
    total = 0
    offcaps = len(res)
    for i in res.keys():
        total = total + len(res[i])
    print("%d tapes in %d caps" % (total, offcaps))
    print()
    # check last time a ticket was cut
    if library:
        offltt = operation.get_last_write_protect_off_job_time(l=library)
    else:
        offltt = operation.get_last_write_protect_off_job_time()
    if time.time() - offltt > seven_days:
        print(
            "==> Last ticket was cut 7 or more days ago ...",
            time.ctime(offltt))
        print()
    sys.stdout = stdout_save
    if output and (oncaps >= LIMIT or offcaps >= LIMIT):
        cmd = 'cat %s' % (sys.argv[1])
        os.system(cmd)
        cmd = '/usr/bin/Mail -s "tab flipping watch" %s < %s ' % (
            os.environ['ENSTORE_MAIL'], sys.argv[1])
        os.system(cmd)

    # should we generate the ticket?
    if output and oncaps and (
            oncaps >= LIMIT or time.time() - onltt > seven_days):
        execmd = ['auto_write_protect_on']
        if library:
            execmd.append(library)
        res = operation.execute(execmd)
        f = open(TMP_FILE, 'w')
        f.write(
            "A write_protection_on ticket is generated for %s at %s\n\n" %
            (operation.cluster, time.ctime(
                time.time())))
        for i in res:
            f.write(i + '\n')
        f.close()
        cmd = '/usr/bin/Mail -s "write protection on job generated" %s < %s ' % (
            os.environ['ENSTORE_MAIL'], TMP_FILE)
        os.system(cmd)
