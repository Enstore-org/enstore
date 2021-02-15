#!/usr/bin/env python
from __future__ import print_function
import sys
import os
import string
import time
import getopt
import traceback

import e_errors
import log_client


def usage():
    print("Usage:", sys.argv[0], "device file_size position")


def check_mover(device, position, f_size):
    import random
    import array

    # block_size is 128K
    block_size = 128 * 1024
    # mount tape
    #import FTT
    import ftt
    import ftt_driver

    FTT = ftt_driver.FTTDriver()
    print("OPENING DEVICE")
    try:
        FTT.open(device, 0)
    except ftt.FTTError as detail:
        print(detail, detail.errno)
        return

    print("REWINDING TAPE")
    # tape is in
    FTT.rewind()

    ret = FTT.ftt.skip_fm(position)
    nb = f_size / block_size
    rest = f_size % block_size
    print("READING DATA")
    buf = block_size * ' '
    #t1 = time.time()
    t = 0.
    for i in range(0, nb):
        print("READING", block_size / 1024, "Kbytes")
        t1 = time.time()
        FTT.read(buf, 0, block_size)
        t = time.time() - t1 + t
        p = i * 1. * 100 * block_size / f_size
        if p % 10 == 0:
            print("%.3g %s done" % (p, "%"))
        #print "READ", len(ret)
    # p=100.
    print("%.3g %s done" % (p, "%"))
    if rest:
        t1 = time.time()
        FTT.read(buf, 0, rest)
        t = time.time() - t1 + t
    t2 = time.time()
    print(
        block_size *
        f_size /
        1024. /
        1024.,
        "Mbytes read in",
        t2 -
        t1,
        "secs")
    #print "READ TIME",t2-t1,"secs"
    #read_t_r = f_size*1./1024./1024./(t2-t1)
    print("READ TIME", t, "secs")
    read_t_r = f_size * 1. / 1024. / 1024. / (t)
    FTT.rewind()

    FTT.close()
    # unload tape
    # FTT.unload()
    #ticket['work'] = 'unloadvol'
    #rt = u.send(ticket,(mcticket['hostip'], mcticket['port']),300,10)
    print("Transfer rate: %.3g MB/S" % (read_t_r))
    return None


if __name__ == "__main__":

    optlist, args = getopt.getopt(sys.argv[1:], '', '')

    if len(args) < 3:
        usage()
        sys.exit(-1)
    device = args[0]
    f_size = long(args[1])
    pos = int(args[2])

    check_mover(device, pos, f_size)
    sys.exit(0)
