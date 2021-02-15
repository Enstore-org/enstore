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
    print("Usage:", sys.argv[0], "device tape [file_size in MB]")


def check_mover(device, test_tape, f_size=250):
    import time
    import random
    import array

    print("GENERATING DATA")
    ran_arr = ''
    # block_size is 128K
    block_size = 128 * 1024
    for i in range(0, block_size):
        ran_arr = ran_arr + chr(random.randint(0, 255))
    # mount tape
    #import FTT
    import ftt
    import ftt_driver

    FTT = ftt_driver.FTTDriver()
    print("OPENING DEVICE")
    try:
        FTT.open(device, 1)
    except ftt.FTTError as detail:
        print(detail, detail.errno)
        return

    print("REWINDING TAPE")
    # tape is in
    FTT.rewind()

    # check if correct tape has been mounted
    try:

        print("READING LABEL")
        label = 80 * ' '
        nb = FTT.read(label, 0, 80)
        print("NB", nb)
        print("LB", label)

        if len(label) != 80:
            print("WRITING LABEL")
            hdr = "VOL1" + test_tape
            hdr = hdr + (79 - len(hdr)) * ' ' + '0'
            # FTT.set_blocksize(80)
            FTT.rewind()
            sts = FTT.write(hdr, 0, 80)
            FTT.writefm()
            FTT.rewind()
            label = 80 * ' '
            nb = FTT.read(label, 0, 80)
        print("LABEL,LEN", label, nb)
        typ = label[:4]
        val = string.split(label[4:])[0]
        print("TYP", label[:4], "VAL", val)
        if typ != "VOL1":
            print("WRITING LABEL")
            hdr = "VOL1" + test_tape
            hdr = hdr + (79 - len(hdr)) * ' ' + '0'
            sts = FTT.write(hdr, 0, 80)
            FTT.writefm()
            FTT.rewind()
            nb = FTT.read(label, 0, 80)
            typ = label[:4]
            val = string.split(label[4:])[0]
            print("TYP", label[:4], "VAL", val)
        if typ == "VOL1" and val != test_tape:
            print("wrong tape is mounted %s" % (val,))
            FTT.rewind()

            # unload tape
            FTT.ftt.unload()
            #ticket['work'] = 'unloadvol'
            #rt = u.send(ticket,(mcticket['hostip'], mcticket['port']),300,10)
            return

    except BaseException:
        traceback.print_exc()
        typ, val = None, None
        return

    # sys.exit(0)

    # do the work
    # write "file". File size is 250M
    bl_in_M = 1024 * 1024 / block_size  # blocks in 1 MB
    #print "FSIZE",f_size
    f_size = f_size * bl_in_M
    # bs=FTT.get_blocksize()
    # FTT.set_blocksize(block_size)
    # bs=FTT.get_blocksize()
    ret = FTT.ftt.skip_fm(1)
    #print "WRITE SKIP",ret
    print("WRITING DATA")
    t1 = time.time()
    print("R_ARR", ran_arr[0])
    for i in range(0, f_size):
        sts = FTT.write(ran_arr, 0, block_size)
        p = i * 1. * 100 / f_size
        if p % 10 == 0:
            print("%.3g %s done" % (p, "%"))
        #print "WRITTEN",sts,"bytes"
    p = 100.
    print("%.3g %s done" % (p, "%"))

    t2 = time.time()
    print("WRITE TIME", t2 - t1, "secs")
    #print block_size/1024./1024.*f_size,"Mbytes written in",t2-t1,"secs"
    FTT.writefm()
    FTT.rewind()
    # FTT.close()

    write_t_r = block_size * 1. * f_size * 1. / 1024. / 1024. / (t2 - t1)

    # read file
    #FTT.open(drive, 'r')
    time.sleep(10)
    # FTT.set_blocksize(block_size)
    # bs=FTT.get_blocksize()
    ret = FTT.ftt.skip_fm(1)
    print("READ SKIP", ret)
    print("READING DATA")
    t1 = time.time()
    buf = block_size * ' '
    for i in range(0, f_size):
        #print "READING",block_size/1024,"Kbytes"
        FTT.read(buf, 0, block_size)
        p = i * 1. * 100 / f_size
        if p % 10 == 0:
            print("%.3g %s done" % (p, "%"))
        #print "READ", len(ret)
    p = 100.
    print("%.3g %s done" % (p, "%"))
    t2 = time.time()
    #print block_size*f_size/1024./1024.,"Mbytes read in",t2-t1,"secs"
    print("READ TIME", t2 - t1, "secs")
    read_t_r = block_size * 1. * f_size * 1. / 1024. / 1024. / (t2 - t1)
    FTT.rewind()

    FTT.close()
    # unload tape
    # FTT.unload()
    #ticket['work'] = 'unloadvol'
    #rt = u.send(ticket,(mcticket['hostip'], mcticket['port']),300,10)
    print(
        "Transfer rates: device %s write %.3g MB/S read %.3g MB/S" %
        (device, write_t_r, read_t_r))
    return None


if __name__ == "__main__":

    optlist, args = getopt.getopt(sys.argv[1:], '', '')

    if len(args) < 2:
        usage()
        sys.exit(-1)
    device, tape = args[0], args[1]
    if len(args) == 3:
        f_size = string.atoi(args[2])
    else:
        f_size = 250  # default 250 MB
    print("FSIZE WILL BE ", f_size, "MB")

    check_mover(device, tape, f_size)
    sys.exit(0)
