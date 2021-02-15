#!/usr/bin/env python
from __future__ import print_function
import sys
import os
import string
import time
import getopt
import traceback

import configuration_client
import Trace
import e_errors
import log_client


def usage():
    print("Usage:", sys.argv[0], "mover [file_size in MB]")


def check_mover(config_client, mover, f_size=250):
    import udp_client
    import random

    # get the media changer
    mv = config_client.get(mover)

    if mv:
        print("MV", mv)
        logc = log_client.LoggerClient(config_client, "DRTST", "log_server")
        Trace.init("DRTST")
        Trace.log(e_errors.INFO, "checking drive %s" % (mv['mc_device'],))
        u = udp_client.UDPClient()
        # communicate with mover
        # set mover to the draining state
        Trace.log(e_errors.INFO, "draining %s" % (mover,))
        rc = u.send({"work": "start_draining"}, (mv['hostip'], mv['port']),
                    300, 10)
        # sleep a while
        time.sleep(10)
        # wait until mover is free
        rc = u.send({"work": "status"}, (mv['hostip'], mv['port']),
                    300, 10)
        print("rc", rc)
        while not (rc['state'] == 'OFFLINE'):
            rc = u.send({"work": "status"}, (mv['hostip'], mv['port']),
                        300, 10)
            print("rc", rc)
            time.sleep(30)

        Trace.log(e_errors.INFO, "mover %s is ready for test" % (mover,))

        # get a volume
        vc = config_client.get('volume_clerk')
        print("VC", vc)
        library = string.split(mv['library'], '.')[0]
        vticket = {'work': 'next_write_volume',
                   'library': library,
                   'min_remaining_bytes': 1,
                   'volume_family': 'TEST_TAPE.TEST_TAPE',
                   'wrapper': 'none',
                   'vol_veto_list': repr([]),
                   'first_found': 1}

        vinfo = u.send(vticket, (vc['hostip'], vc['port']), 300, 10)

        print("VC Returned", vinfo)
        # return

        # comminucate with media changer
        mcticket = config_client.get(mv['media_changer'])
        # load test volume
        ticket = {'work': 'loadvol',
                  'vol_ticket': {'external_label': vinfo['external_label'],
                                 'media_type': vinfo['media_type'],
                             },
                  'drive_id': mv['mc_device'],
                  }
        rt = u.send(ticket, (mcticket['hostip'], mcticket['port']), 300, 10)
        Trace.log(e_errors.INFO, "load returned %s" % (repr(rt),))
        # create array of random numbers
        ran_arr = ''
        # block_size is 128K
        block_size = 128 * 1024
        for i in range(0, block_size):
            ran_arr = ran_arr + chr(random.randint(0, 255))
        # mount tape
        import ftt
        import ftt_driver

        FTT = ftt_driver.FTTDriver()
        print("OPENING DEVICE")
        try:
            FTT.open(mv['device'], 1)
        except ftt.FTTError as detail:
            print(detail, detail.errno)
            return

        print("REWINDING TAPE")
        # tape is in
        FTT.rewind()

        # check if correct tape has been mounted
        try:
            try:
                print("READING LABEL")
                label = 80 * ' '
                nb = FTT.read(label, 0, 80)
                print("NB", nb)
                print("LB", label)
            except BaseException:
                print("EXPT")
                label = ""
            if len(label) != 80:
                print("WRITING LABEL")
                hdr = "VOL1" + vinfo['external_label']
                hdr = hdr + (79 - len(hdr)) * ' ' + '0'
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
                hdr = "VOL1" + vinfo['external_label']
                hdr = hdr + (79 - len(hdr)) * ' ' + '0'
                sts = FTT.write(hdr, 0, 80)
                FTT.writefm()
                FTT.rewind()
                nb = FTT.read(label, 80)
                typ = label[:4]
                val = string.split(label[4:])[0]
                print("TYP", label[:4], "VAL", val)
            if typ == "VOL1" and val != vinfo['external_label']:
                print("wrong tape is mounted %s" % (val,))
                FTT.rewind()

                # unload tape
                FTT.ftt.unload()
                ticket['work'] = 'unloadvol'
                rt = u.send(
                    ticket, (mcticket['hostip'], mcticket['port']), 300, 10)
                # sys.exit(0)

        except BaseException:
            traceback.print_exc()
            typ, val = None, None

        # sys.exit(0)

        # do the work
        # write "file". File size is 250M
        bl_in_M = 1024 * 1024 / block_size  # blocks in 1 MB
        print("FSIZE", f_size)
        f_size = f_size * bl_in_M
        ret = FTT.ftt.skip_fm(1)
        print("WRITE SKIP", ret)
        t1 = time.time()
        for i in range(0, f_size):
            sts = FTT.write(ran_arr, 0, block_size)
            #print "WRITTEN",sts,"bytes"
        t2 = time.time()
        #print block_size/1024./1024.*f_size,"Mbytes written in",t2-t1,"secs"
        FTT.writefm()
        FTT.rewind()

        write_t_r = block_size * 1. * f_size * 1. / 1024. / 1024. / (t2 - t1)

        # read file
        time.sleep(10)
        ret = FTT.ftt.skip_fm(1)
        print("READ SKIP", ret)

        t1 = time.time()
        buf = block_size * ' '
        for i in range(0, f_size):
            #print "READING",block_size/1024,"Kbytes"
            ret = FTT.read(buf, 0, block_size)
            #print "READ", len(ret)
        t2 = time.time()
        #print block_size*f_size/1024./1024.,"Mbytes read in",t2-t1,"secs"
        read_t_r = block_size * 1. * f_size * 1. / 1024. / 1024. / (t2 - t1)
        FTT.rewind()

        # unload tape
        FTT.ftt.unload()
        ticket['work'] = 'unloadvol'
        rt = u.send(ticket, (mcticket['hostip'], mcticket['port']), 300, 10)
        print(
            "Transfer rates: host %s mover %s device %s write %.3g MB/S read %.3g MB/S" %
            (mv['host'], mover, mv['mc_device'], write_t_r, read_t_r))
        Trace.log(
            e_errors.INFO,
            "Device %s tested.Transfer rates:write %.3g MB/S, read %.3g MB/S" %
            (mv['mc_device'],
             write_t_r,
             read_t_r))
        Trace.log(e_errors.INFO, "stop draining %s" % (mover,))
        rc = u.send({"work": "stop_draining"}, (mv['hostip'], mv['port']),
                    300, 10)
    return None


if __name__ == "__main__":

    optlist, args = getopt.getopt(sys.argv[1:], '', '')

    if len(args) == 0:
        usage()
        sys.exit(-1)
    mover = args[0]
    if len(args) == 2:
        f_size = string.atoi(args[1])
    else:
        f_size = 250  # default 250 MB
    print("FSIZE WILL BE ", f_size, "MB")
    port = os.environ.get('ENSTORE_CONFIG_PORT', 0)
    port = string.atoi(port)
    if port:
        # we have a port
        host = os.environ.get('ENSTORE_CONFIG_HOST', 0)
        if host:
            # we have a host
            csc = configuration_client.ConfigurationClient((host, port))
        else:
            print("cannot find config host")
            sys.exit(-1)
    else:
        print("cannot find config port")
        sys.exit(-1)

    check_mover(csc, mover, f_size)
    sys.exit(0)
