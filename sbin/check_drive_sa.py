#!/usr/bin/env python
import sys
import os
import string
import time
import getopt
import traceback

import e_errors
import log_client

def usage():
    print "Usage:",sys.argv[0], "mover tape media [file_size in MB]"
    

def check_mover(drive, test_tape, media, f_size=250):
    import time
    import whrandom
    import array
    
    print "GENERATING DATA"
    ran_arr = array.array('B')
    # block_size is 128K
    block_size = 128*1024
    for i in range(0,block_size):
        ran_arr.append(whrandom.randint(0,255))
    # mount tape
    import FTT
    print "OPENING DEVICE"
    FTT.open(drive, 'a+' )
    #x = 120				# for now, after ??? ftt_rewind will
    x = 10				# for now, after ??? ftt_rewind will
					# raise exception
    while x:
        try:
            status = FTT.status( 3 )
            if status['ONLINE']: break
              #print "try ",x," status ",status, " ",device
        except FTT.error:
            pass
        # it appears that one needs to close the device and reopen it to get the status
        # to change. This doesn't make any sense, but does work for ftt v2_3.
        # if you don't close/reopen, the status never changes, but a check outside of enstore
        # using the same python calls succeeds right away after enstore reports failure - bakken 3/3/99
        FTT.close()
        time.sleep( 1 )
        FTT.open(drive, 'a+' )
        x = x -1
        pass
    if x == 0:
        print "Mount error",repr(status)
        return
    else:
        print "REWINDING TAPE"
        # tape is in
        FTT.rewind()

    # check if correct tape has been mounted
    try:
        try:
            print "READING LABEL"
            label=FTT.read(80)
        except:
            print "EXPT"
            label = ""
        if len(label)!=80:
            print "WRITING LABEL"
            hdr = "VOL1"+test_tape
            hdr = hdr+ (79-len(hdr))*' ' + '0'
            FTT.set_blocksize(80)
            FTT.rewind()
            sts = FTT.write(hdr)
            FTT.writefm()
            FTT.rewind()
            label=FTT.read(80)
        #print "LABEL,LEN",label,len(label)
        typ=label[:4]
        val=string.split(label[4:])[0]
        #print "TYP",label[:4],"VAL",val
        if typ != "VOL1":
            print "WRITING LABEL"
            hdr = "VOL1"+test_tape
            hdr = hdr+ (79-len(hdr))*' ' + '0'
            FTT.set_blocksize(80)
            sts = FTT.write(hdr)
            FTT.writefm()
            FTT.rewind()
            label=FTT.read(80)
            typ=label[:4]
            val=string.split(label[4:])[0]
            print "TYP",label[:4],"VAL",val
        if typ == "VOL1" and val != test_tape:
            print"wrong tape is mounted %s"%(val,)
            FTT.rewind()

            # unload tape
            #FTT.unload()
            #ticket['work'] = 'unloadvol'
            #rt = u.send(ticket,(mcticket['hostip'], mcticket['port']),300,10)
            #sys.exit(0)
                
    except:
        traceback.print_exc()
        typ,val = None,None

    #sys.exit(0)
        
    # do the work
    # write "file". File size is 250M
    bl_in_M = 1024*1024/block_size # blocks in 1 MB
    #print "FSIZE",f_size
    f_size = f_size*bl_in_M
    bs=FTT.get_blocksize()
    FTT.set_blocksize(block_size)
    bs=FTT.get_blocksize()
    ret = FTT.skip_fm(1)
    #print "WRITE SKIP",ret
    print "WRITING DATA"
    t1 = time.time()
    for i in range (0,f_size):
        sts = FTT.write(ran_arr)
        #print "WRITTEN",sts,"bytes"
    t2 = time.time()
    #print block_size/1024./1024.*f_size,"Mbytes written in",t2-t1,"secs"
    FTT.writefm()
    FTT.rewind()
    FTT.close()

    write_t_r = block_size*f_size/1024./1024./(t2-t1)
        
    # read file
    FTT.open(drive, 'r')
    time.sleep(10)
    FTT.set_blocksize(block_size)
    bs=FTT.get_blocksize()
    ret = FTT.skip_fm(1)
    #print "READ SKIP",ret
    print "READING DATA"
    t1 = time.time()
    for i in range (0,f_size):
        #print "READING",block_size/1024,"Kbytes"
        ret = FTT.read(block_size)
        #print "READ", len(ret)
    t2 = time.time()
    #print block_size*f_size/1024./1024.,"Mbytes read in",t2-t1,"secs"
    read_t_r = block_size*f_size/1024./1024./(t2-t1)
    FTT.rewind()

    # unload tape
    #FTT.unload()
    #ticket['work'] = 'unloadvol'
    #rt = u.send(ticket,(mcticket['hostip'], mcticket['port']),300,10)
    print "Transfer rates: device %s write %.3g MB/S read %.3g MB/S"%(drive,write_t_r,read_t_r)
    return None
        
    
    
if __name__ == "__main__":

    optlist,args=getopt.getopt(sys.argv[1:],'','')
    
    if len(args) < 3:
        usage()
        sys.exit(-1)
    mover,tape,media=args[0],args[1],args[2]
    if len(args) == 4:
        f_size = string.atoi(args[3])
    else:
        f_size = 250 # default 250 MB
    print "FSIZE WILL BE ",f_size,"MB"
    
    check_mover(mover, tape, media, f_size)
    sys.exit(0)
