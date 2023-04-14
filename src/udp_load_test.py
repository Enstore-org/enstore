#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import sys
import threading
import time
import socket
import select

import udp_client
import e_errors

RUN=True
CYCLES = 100
lock = threading.Lock()
transfers=0L

def usage(cmd):
    print "usage: %s <host> <port> <threads>"%(cmd)
    
def run_in_thread(function, args=()):
    #thread_name = str(threading.activeCount()+1)
    #print "create thread: name %s target %s args %s" % (thread_name, function, args)
    thread = threading.Thread(group=None, target=function,
                              args=args, kwargs={})
    print "starting thread name=%s"%(thread.getName(),)
    try:
        thread.start()
    except:
        exc, detail, tb = sys.exc_info()
        print "Error starting thread: %s" % (detail)
    return 0

def send_message(address, message):
    global RUN, lock, transfers
    
    u = udp_client.UDPClient()
    while RUN:
        try:
            back = u.send(message, address, rcv_timeout = 10)
            lock.acquire()
            transfers = transfers + 1
            lock.release()
        except (socket.error, select.error, e_errors.EnstoreError), detail:
            thread = threading.currentThread()
            print "%s %s"%(thread.getName(), detail)
        

if __name__ == "__main__":
    if len(sys.argv) !=4:
        usage(sys.argv[0])
        sys.exit(1)
    address = (sys.argv[1], int(sys.argv[2]))

    

    ticket = {'vc': {'comment': '', 'declared': 1202335648.0, 'blocksize': 131072, 'sum_rd_access': 22438, 'library': 'null1', 'si_time': [0.0, 1205775137.0], 'user_inhibit': ['none', 'none'], 'storage_group': 'TEST', 'system_inhibit': ['none', 'none'], 'external_label': 'NUL027', 'wrapper': 'null', 'remaining_bytes': 193848147968L, 'sum_mounts': 1518, 'capacity_bytes': 429496729600L, 'media_type': 'null', 'last_access': 1235590910.0, 'status': ('ok', None), 'eod_cookie': '0000_000000000_0031447', 'non_del_files': 31702, 'sum_wr_err': 10, 'sum_wr_access': 31712, 'file_family': 'test', 'address': ('131.225.13.129', 7502), 'volume_family': 'TEST.test.null', 'write_protected': 'n', 'sum_rd_err': 1, 'first_access': 1219247276.0}, 'outfilepath': '/dev/null', 'encp': {'delpri': 0, 'basepri': 1, 'adminpri': -1, 'delayed_dismount': None, 'agetime': 0}, 'file_size': 2097152L, 'wrapper': {'major': 0, 'rminor': 0, 'pnfsFilename': '/pnfs/fs/usr/data1/test/moibenko/NULL/load_test/A/2009-02-09/stkenmvr121a/3/stkenmvr121a_test_3_1233329977_6012.data.24273', 'uid': 13160, 'sanity_size': 65536, 'gname': 'fnalgrid', 'machine': ('Linux', 'd0cs0934.fnal.gov', '2.6.9-67.0.7.ELsmp', '#1 SMP Fri Mar 14 09:24:11 CDT 2008', 'x86_64'), 'uname': 'fnalgrid', 'pstat': (33188, 30396560L, 20L, 1, 1527, 6849, 2097152L, 1234193597, 1234193597, 1234193467), 'mode_octal': '0100744', 'mode': 33252, 'size_bytes': 2097152L, 'rmajor': 0, 'gid': 9767, 'fullname': '/dev/null', 'inode': 30396560L, 'minor': 20}, 'version': 'v3_7d  CVS $Revision$ <frozen>', 'encp_daq': None, 'client_crc': 1, 'infile': '/pnfs/fs/usr/data1/test/moibenko/NULL/load_test/A/2009-02-09/stkenmvr121a/3/.(access)(000100000000000001CFD090)', 'volume': 'NUL027', 'outfile': '/dev/null', 'fc': {'status': ('ok', None), 'size': 2097152L, 'complete_crc': 0L, 'pnfs_name0': '/pnfs/data1/test/moibenko/NULL/load_test/A/2009-02-09/stkenmvr121a/3/stkenmvr121a_test_3_1233329977_6012.data.24273', 'deleted': 'no', 'purge_en': 'n', 'package': 'none', 'external_label': 'NUL027', 'update': '2009-02-09 09:33:16.419967', 'drive': 'gccenmvr1a:/dev/null:0', 'purge': 'none', 'gid': 1527, 'location_cookie': '0000_000000000_0029743', 'pnfsid': '000100000000000001CFD090', 'address': ('131.225.13.129', 7501), 'bfid': 'GCMS123419359400005', 'sanity_cookie': (65536L, 0L), 'uid': 6849}, 'infilepath': '/pnfs/fs/usr/data1/test/moibenko/NULL/load_test/A/2009-02-09/stkenmvr121a/3/stkenmvr121a_test_3_1233329977_6012.data.24273', 'bfid': 'GCMS123419359400005', 'local_inode': 1999L, 'resend': {'max_retry': 3, 'resubmit': 0, 'retry': 0, 'max_resubmits': None}, 'callback_addr': ('131.225.218.217', 49402), 'work': 'read_from_hsm', 'override_ro_mount': 0, 'times': {'encp_start_time': 1235590923.6707029, 't0': 1235590923}, 'user_level': 'admin', 'unique_id': 'd0cs0934.fnal.gov-1235590924-20202-0'}
    
    t = 0
    while t < int(sys.argv[3]):
        tick = ticket
        tick["THREAD"] = "TH_%s"%(t,)
        run_in_thread(send_message, args=(address, tick))
        t = t + 1
    c = 0
    while c < CYCLES:
        try:
            time.sleep(1)
            c = c + 1
        except KeyboardInterrupt:
            RUN = False
            time.sleep(5)
            break
    RUN = False
    time.sleep(5)
    print "TEST FINISHED, Total number of transfers", transfers
    sys.exit(0)
        
