#!/usr/bin/env python
# $Id$
import sys
import os
import string
import time

OK = 0
FAIL = 1

dir="/home/aik/tape_inventory_010723/"

def generate_volume_list(volume_file):
    f=open(volume_file, 'r')
    lines = f.readlines()
    volume_list = []
    del(lines[0])
    del(lines[0])
    for line in lines:
        label,junk,junk,junk,junk,junk,junk,volume_group=line.split()
        ff = volume_group.split('.')[1]
        #print "DEBUG: '%s', '%s'" % (label, label[0:4])
        if label[0:4] == "null" or label[0:4] == "NULL":
            continue
        if ff != 'none':
            volume_list.append(label)
    f.close()
    return volume_list

def readlayer(fullname,layer,ferr):
    (dir,fname)=os.path.split(fullname)
    fname = "%s/.(use)(%s)(%s)"%(dir,layer,fname)
    try:
        f = open(fname,'r')
        l = f.readlines()
        f.close()
        return (l,OK)
    except :
        exc, msg, tb = sys.exc_info()
        ferr.write ( "Error ERL: Can't read file '%s', errors : exc='%s' msg='%s'\n" %
                     (fname,str(exc),str(msg)) )
        l = []
        return (l,FAIL)
  

def file_stat(kind,fname,comment,ferr):

    try:
        stat_info_vmap=os.stat(fname)
        return 0
    except OSError,detail:
        ferr.write ( "%s: Stat failed for the file '%s' (%s), detail :%s\n" %
                     (kind,fname,comment,detail) )
    except :
        exc, msg, tb = sys.exc_info()
        ferr.write ( "%s: Stat failed for the file '%s' (%s), errors : exc='%s' msg='%s'\n" %
                     (kind,fname,comment,str(exc),str(msg)) )
        ferr.write ( "DEBUG: stat failed ... exc= '%s' msg= '%s'\n" % (exc, msg) )
    return -1

def check_volume(label):
    err    = 0
    warn   = 0
    fcount = 0
    ferrname = label+".err" 
    
    f=open(dir+label,'r')
    ferr = open(ferrname,'w')
    
    lines=f.readlines()
    l = lines[0].split()[1]
    
    if l != label:
        ferr.write ("Error ELBL: Labels do not match, %s -> %s\n" %(label,l))
        err = err +1
        ferr.close()       
        return (-1,err,warn,fcount)
    
    del(lines[0])
    last_acc = lines[0].split()[3]
    
    if last_acc == "Never":
        #print "DEBUG: OK, never accessed"
        ferr.close()
        os.unlink(ferrname)
        return (0,err,warn,fcount)
    
    for i in range(0,6):
        del( lines[0])    

    # make list
    list  = []
    for line in lines:
        field = line.split()
        if len(field) == 6:
            d = {"bf_id":field[1],
                 "size":field[2],
                 "location":field[3],
                 "del_flag":field[4],
                 "fname":field[5]}
            list.append(d)
        elif len(field) == 0:
            break
        else :
            ferr.write( "Error EFL: File record field length is not 6 (%d), record '%s'\n" %(len(field)),field )
            break
        
    # Process list
    add_nl = 0
    for entry in list:
        if entry["del_flag"] == "yes" :
            continue
        if entry["del_flag"] == "unknown" :
            continue
        if entry["fname"] == "unknown" :
            continue
        
        fcount = fcount +1
        if fcount == 100 :
            print "Files in Hundreds :"
            print 0,
            add_nl = 1
        if fcount % 100 == 0 :
            hundr = fcount/100
            last = hundr%10
            print last,
            sys.stdout.flush()
            
            if last == 9 :
                print ""
        
        # Check file is readable
        fname = entry["fname"]
        if file_stat( "Error ESF",fname,"orig_file",ferr ) :
            err = err +1
            continue

        # Get pnfs layer
        # Layer lendth: currently is 10, before oct 2000 was 9.
        # Tenth element is MT devece name
        
        (layer1,OK_rdL1)=readlayer(fname,1,ferr)
        (layer,OK_rdL4) =readlayer(fname,4,ferr)
        if( not OK_rdL1 ) :
            # Can not check current entry anymore, skip to the next file
            continue

        if (len(layer1) != 1) :
            err = err +1
            ferr.write("Error L1: wrong L1 layer length= %d, layer=%s " % (len(layer1),layer1) )
            ferr.write("invt bf_id %s  file '%s'\n" % (entry["bf_id"],entry["fname"]) )
            continue
        
        # Check BF_ID
        #   for Level 1
        bfid_l1  = layer1[0]
        bfid_inv = entry["bf_id"]
        #if bfid_inv[len(bfid_inv)-1] == "L" or  bfid_inv[len(bfid_inv)-1] == "l" :
        #    bfid_inv = bfid_inv[:-1]
        if bfid_l1 != bfid_inv :
            err = err +1
            err_kind = "Error L1"
            same = "(diff L4)"
            if( OK_rdL4 and len(layer)>8 and ( bfid_l1 == layer[8][:-1] ) ) :
                same = " (same as in L4)"
            msg = "Wrong BF_ID pnfs: %s -> inv: %s file '%s' del='%s'%s." % (bfid_l1, bfid_inv, entry["fname"], entry["del_flag"], same)
            ferr.write ( "%s: %s\n" % (err_kind, msg) )

        if( not OK_rdL4 ) :
            # Can not check current entry anymore, skip to the next file
            continue

        if (len(layer) != 9) and (len(layer) != 10) :
            err = err +1
            ferr.write("Error L4: wrong L4 layer length= %d, layer=%s " % (len(layer),layer) )
            ferr.write("invt bf_id %s  file '%s'\n" % (entry["bf_id"],entry["fname"]) )
            continue

        #   for level 4
        if layer[8][:-1] != entry["bf_id"]:
            msg = "Wrong BF_ID pnfs: %s -> inv: %s file '%s' del='%s'." % (bfid_l1, bfid_inv, entry["fname"], entry["del_flag"])
            if layer[0][:-1] == label :
                #err = err +1
                #err_kind = "Error L4"
                warn = warn +1
                err_kind = "Warn. L4"
            else :
                warn = warn +1
                err_kind = "Warn. L4"
                msg = msg + (" Actual pnfs volume %s referenced as %s (inv), loc %s" %
                             (layer[0][:-1], label, layer[1][:-1]))
            ferr.write ( "%s: %s\n" % (err_kind, msg) )
            vol_map_name = layer[5][:-1]

            if file_stat( "Warn. L4",vol_map_name,"Vol_Map",ferr ) :
                #err = err +1
                warn = warn +1
                continue
#--------
    if ( add_nl ) :
         print ""
         add_nl=0
    
    ferr.close()
    if( err == 0 and warn == 0 ):
        os.unlink(ferrname)

    return (0, err, warn,fcount)

#--------------------------------------------------
# Main()
t1 = time.time()
flog = open("AUDIT.log","w")

volume_list=generate_volume_list(dir+"VOLUMES_DEFINED")
#volume_list= ["VO0185",]
#volume_list= ["VO1059","VO1060","VO1061","VO1062","VO1063","VO1064",]
#volume_list= ["VO1002",]
#volume_list= ["VO0242",]

nvols = len(volume_list)

# set VMAX = -1 for normal operation
#
VMAX   = -1
vcount = 0

for volume in volume_list:
    print "Volume %4d of %4d, %s" % (vcount+1, nvols, volume)
    (ret,err,warn,fcount) = check_volume(volume)
    flag = "OK "
    if (ret != 0 or err !=0 or warn !=0 ):
        flag = "BAD"
        
    flog.write( "%s %s  \tfiles=%6d rc=%3d  err=%5d  warn=%5d\n" % (flag,volume,fcount,ret,err,warn))
    vcount = vcount+1
    if( vcount == VMAX ) :
        break
    
t2 = time.time()
suffix = ""
if vcount > 1 :
    suffix = "s"
print "Checked %s volume%s in %s sec" % ( vcount,suffix, (t2-t1))
