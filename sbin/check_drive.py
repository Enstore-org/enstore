#!/usr/bin/env python
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
    print "Usage:",sys.argv[0], "mover tape media [file_size in MB]"
    

def check_mover(config_client, mover, test_tape, media, f_size=250):
    import time
    import udp_client
    import whrandom
    import array
    
    # get the media changer
    mv = config_client.get_uncached(mover)
    
    if mv:
        logc = log_client.LoggerClient(config_client, "DRTST", "log_server")
        Trace.init("DRTST")
        Trace.log(e_errors.INFO,"checking drive %s"%(mv['mc_device'],))
        u = udp_client.UDPClient()
        # communicate with mover
        # set mover to the draining state
        Trace.log(e_errors.INFO,"draining %s"%(mover,))
        rc = u.send({"work" : "start_draining"}, (mv['hostip'], mv['port']),
                            300, 10)
        # sleep a while
        time.sleep(10)
        # wait until mover is free
        rc = u.send({"work" : "status"}, (mv['hostip'], mv['port']),
                    300, 10)
        while not (rc['state'] == 'draining' and rc['mode'] =='f'):
            rc = u.send({"work" : "status"}, (mv['hostip'], mv['port']),
                        300, 10)
            time.sleep(30)
        
        Trace.log(e_errors.INFO, "mover %s is ready for test"%(mover,))

        # comminucate with media changer
        mcticket = config_client.get(mv['media_changer'])
        # load test volume
	ticket = {'work' : 'loadvol',
                  'vol_ticket'     : {'external_label': test_tape,
                                      'media_type': media,
                                      },
                  'drive_id'       : mv['mc_device']
                  }
	rt = u.send(ticket,(mcticket['hostip'], mcticket['port']),300,10)
        Trace.log(e_errors.INFO,"load returned %s"%(repr(rt),))
        # create array of random numbers
        ran_arr = array.array('B')
        # block_size is 128K
        block_size = 128*1024
        for i in range(0,block_size):
            ran_arr.append(whrandom.randint(0,255))
        # mount tape
        import FTT
        FTT.open(mv['device'] , 'a+' )
	x = 120				# for now, after ??? ftt_rewind will
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
            FTT.open( mv['device'], 'a+' )
	    x = x -1
	    pass
	if x == 0:
            Trace.log(e_errors.ERROR,"sw_mount error")
        else:
            # tape is in
            FTT.rewind()

        # check if correct tape has been mounted
        Trace.log(e_errors.INFO,"check test tape label")
        try:
            try:
                label=FTT.read(80)
            except:
                label = ""
            if len(label)!=80:
                Trace.log(e_errors.INFO,"label test tape")
                hdr = "VOL1"+test_tape
                hdr = hdr+ (79-len(hdr))*' ' + '0'
                FTT.set_blocksize(80)
                FTT.rewind()
                sts = FTT.write(hdr)
                FTT.writefm()
                Trace.log(e_errors.INFO,"test tape label written")
                FTT.rewind()
                label=FTT.read(80)
            else:
                typ=label[:4]
                val=string.split(label[4:])[0]
                if typ != "VOL1":
                    Trace.log(e_errors.INFO,"label test tape")
                    hdr = "VOL1"+test_tape
                    hdr = hdr+ (79-len(hdr))*' ' + '0'
                    FTT.set_blocksize(80)
                    sts = FTT.write(hdr)
                    FTT.writefm()
                    Trace.log(e_errors.INFO,"test tape label written")
                    FTT.rewind()
                    label=FTT.read(80)
                    typ=label[:4]
                    val=string.split(label[4:])[0]
            if typ == "VOL1" and val != test_tape:
               Trace.log(e_errors.ERROR,"wrong tape is mounted %s"%(val,))
               FTT.rewind()

               # unload tape
               FTT.unload()
               ticket['work'] = 'unloadvol'
               rt = u.send(ticket,(mcticket['hostip'], mcticket['port']),300,10)
               #sys.exit(0)
                
        except:
            traceback.print_exc()
            typ,val = None,None

        #sys.exit(0)
        
        # do the work
        # write "file". File size is 250M
        bl_in_M = 1024*1024/block_size # blocks in 1 MB
        print "FSIZE",f_size
        f_size = f_size*bl_in_M
        bs=FTT.get_blocksize()
        FTT.set_blocksize(block_size)
        bs=FTT.get_blocksize()
        ret = FTT.skip_fm(1)
        print "WRITE SKIP",ret
        t1 = time.time()
        for i in range (0,f_size):
            sts = FTT.write(ran_arr)
            #print "WRITTEN",sts,"bytes"
        t2 = time.time()
        #print block_size/1024./1024.*f_size,"Mbytes written in",t2-t1,"secs"
        FTT.writefm()
        FTT.rewind()
        FTT.close()

        write_t_r = block_size*1.*f_size*1./1024./1024./(t2-t1)
        
        # read file
        FTT.open( mv['device'], 'r' )
        time.sleep(10)
        FTT.set_blocksize(block_size)
        bs=FTT.get_blocksize()
        ret = FTT.skip_fm(1)
        print "READ SKIP",ret
        
        t1 = time.time()
        for i in range (0,f_size):
            #print "READING",block_size/1024,"Kbytes"
            ret = FTT.read(block_size)
            #print "READ", len(ret)
        t2 = time.time()
        #print block_size*f_size/1024./1024.,"Mbytes read in",t2-t1,"secs"
        read_t_r = block_size*1.*f_size*1./1024./1024./(t2-t1)
        FTT.rewind()

        # unload tape
        FTT.unload()
        ticket['work'] = 'unloadvol'
	rt = u.send(ticket,(mcticket['hostip'], mcticket['port']),300,10)
        print "Transfer rates: host %s mover %s device %s write %.3g MB/S read %.3g MB/S"%(mv['host'],mover,mv['mc_device'],write_t_r,read_t_r)
        Trace.log(e_errors.INFO,"Device %s tested.Transfer rates:write %.3g MB/S, read %.3g MB/S"%(mv['mc_device'],write_t_r,read_t_r))
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
    port = os.environ.get('ENSTORE_CONFIG_PORT', 0)
    port = string.atoi(port)
    if port:
        # we have a port
        host = os.environ.get('ENSTORE_CONFIG_HOST', 0)
        if host:
            # we have a host
            csc = configuration_client.ConfigurationClient((host, port))
        else:
            print "cannot find config host"
            sys.exit(-1)
    else:
        print "cannot find config port"
        sys.exit(-1)

    
    check_mover(csc, mover, tape, media, f_size)
    sys.exit(0)
