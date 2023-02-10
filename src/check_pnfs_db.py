#!/usr/bin/env python
# $Id$
import sys
import os
import string
import time
import getopt

tdir = 'enstore/tape_inventory'
OK = 0
FAIL = 1

def Print(msg,output):
    output.write(msg)
    print msg[:-1]

def CleanJunk(msg):
    table='                                 !"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~                                                                                                                                 '
    return string.strip(string.translate(msg,table," \"'{}"))

def generate_volume_list(volume_file, library):
    host = os.environ.get('ENSTORE_CONFIG_HOST', '')
    url = "http://%s/%s"%(host, volume_file)
    # copy to this file first
    # as wget may intermix the data with diagnistics at stdout
    ofile = "VOLS%s"%(int(time.time()),)
    try:
        os.system("wget -o wget.log -O %s %s"%(ofile, url,))
        f = open(ofile, 'r')
    except:
        exc, msg, tb = sys.exc_info()
        print "exception: %s %s" % (str(exc), str(msg))
        return None
    
    lines = f.readlines()
    f.close()
    os.unlink(ofile)
    volume_list = []
    del(lines[0])
    del(lines[0])
    for line in lines:
        label,junk,junk,junk,junk,junk,lib,volume_group=line.split()
        ff = volume_group.split('.')[1]
        #print "DEBUG: '%s', '%s'" % (label, label[0:4])
        if label[0:4] == "null" or label[0:4] == "NULL" or label[0:3] == "CLN" :
            continue
        if library == lib and ff != 'none':
            volume_list.append(label)
    return volume_list

def readlayer(fullname,layer,ferr):
    (fdir,fname)=os.path.split(fullname)
    fname = "%s/.(use)(%s)(%s)"%(fdir,layer,fname)
    try:
        f = open(fname,'r')
        l = f.readlines()
        f.close()
        return (l,OK)
    except :
        exc, msg, tb = sys.exc_info()
        Print( "Error ERL: Can't read file '%s', errors : exc='%s' msg='%s'\n" %
                     (fname,str(exc),str(msg)),ferr )
        l = []
        return (l,FAIL)
  

def file_stat(kind,fname,comment,ferr):

    try:
        stat_info_vmap=os.stat(fname)
        return 0
    except OSError,detail:
        Print ( "%s: Stat failed for the file '%s' (%s), detail :%s\n" %
                     (kind,fname,comment,detail),ferr )
    except :
        exc, msg, tb = sys.exc_info()
        Print  ( "%s: Stat failed for the file '%s' (%s), errors : exc='%s' msg='%s'\n" %
                     (kind,fname,comment,str(exc),str(msg)), ferr )
        Print ( "DEBUG: stat failed ... exc= '%s' msg= '%s'\n" % (exc, msg),ferr )
    return -1

def change_file_name(file):
    # replace pnfs with pnfs/fs/usr
    orig = string.split(file, "/")
    if 'sam' in orig:
        sam_ind = orig.index('sam')
        next = orig[sam_ind+1]
        
        if next in ('mammoth', 'm2', 'lto'):
            orig.remove(next)
            orig[sam_ind] = 'sam-%s'%(next,)
            
    new=['/pnfs','fs','usr'] # this is not always true, depends on the name of the mount point
    for i in range(2,len(orig)):
        new.append(orig[i])
    new_fn=string.join(new,'/')
    return new_fn

def check_volume(label,selected_library=""):
    err    = 0
    warn   = 0
    fcount = 0
    vinfo={}
    ferrname = label+".err" 
    
    host = os.environ.get('ENSTORE_CONFIG_HOST', '')
    url = "http://%s/%s/%s"%(host, tdir, label)
    # copy to this file first
    # as wget may intermix the data with diagnistics at stdout
    ofile = label
    try:
        os.system("wget -o wget.log -O %s %s"%(ofile, url,))
        f = open(ofile, 'r')
    except:
        exc, msg, tb = sys.exc_info()
        print "exception: %s %s" % (str(exc), str(msg))
        return None
    ferr = open(ferrname,'w')
    
    lines=f.readlines()
    l = lines[0].split()[1]
    
    if l != label:
        Print ("Error ELBL: Labels do not match, %s -> %s\n" %(label,l),ferr)
        err = err +1
        ferr.close()       
        return (-1,err,warn,fcount,vinfo)
    
    jsize = len(lines)
    started=0
    line=jsize-1
    while line >=0:
        if len(lines[line])>1:
            aline=lines[line].split(':')
            vinfo[CleanJunk(aline[0])] = CleanJunk(aline[1])
            started = 1
        elif started:
            break
        line = line-1
        
    if len(selected_library)>0:
        if vinfo['library']!=selected_library:
            return (-2,err,warn,fcount,vinfo)
        
    del(lines[0])
    last_acc = lines[0].split()[3]
    
    if last_acc == "Never":
        #print "DEBUG: OK, never accessed"
        ferr.close()
        os.unlink(ferrname)
        return (0,err,warn,fcount,vinfo)
    
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
            Print( "Error EFL: File record field length is not 6 (%d), record '%s'\n" %(len(field),field),ferr)
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
        fname = change_file_name(entry["fname"])
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
            Print ("Error L1: wrong L1 layer length= %d, layer=%s " % (len(layer1),layer1),ferr )
            Print ("invt bf_id %s  file '%s'\n" % (entry["bf_id"],entry["fname"]),ferr )
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
            Print ( "%s: %s\n" % (err_kind, msg),ferr )

        if( not OK_rdL4 ) :
            # Can not check current entry anymore, skip to the next file
            continue

        if (len(layer) != 9) and (len(layer) != 10) :
            err = err +1
            Print ("Error L4: wrong L4 layer length= %d, layer=%s " % (len(layer),layer),ferr )
            Print ("invt bf_id %s  file '%s'\n" % (entry["bf_id"],entry["fname"]),ferr )
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
            Print ( "%s: %s\n" % (err_kind, msg),ferr )
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

    return (0, err, warn,fcount,vinfo)

#--------------------------------------------------

def usage():
    print "usage %s --library=<library> [VOL1 VOL2 ...]"%(sys.argv[0],)

# Main()
if __name__ == "__main__":   # pragma: no cover
    try:
        options, args = getopt.getopt(sys.argv[1:],'',['library=',])
    except getopt.GetoptError:
        usage()
        sys.exit(-1)
    if options and options[0][0] == '--library':
        library = options[0][1]
    else:
        usage()
        print "Sorry you must specify library"
        sys.exit(-1)
    if args:
        volume_list = args
    else:
       volume_list=generate_volume_list("enstore/tape_inventory/VOLUMES_DEFINED", library) 
    t1 = time.time()
    flog = open("AUDIT.log","w")

    #volume_list= ["VO1059","VO1060","VO1061","VO1062","VO1063","VO1064",]
    #volume_list= ["VO1002",]

    nvols = len(volume_list)

    # set VMAX = -1 for normal operation
    #
    VMAX   = -1
    vcount = 0

    for volume in volume_list:
        print "Volume %4d of %4d, %s" % (vcount+1, nvols, volume)
        (ret,err,warn,fcount,vinfo) = check_volume(volume,library)
        flag = "OK "
        if (ret != 0 or err !=0 or warn !=0 ):
            flag = "BAD"
        if ret==-2:
            flag="????"

        Print( "%s %s %s \tfiles=%6d rc=%3d  err=%5d  warn=%5d\n" % (flag,volume,vinfo['volume_family'],fcount,ret,err,warn),flog)
        vcount = vcount+1
        if( vcount == VMAX ) :
            break

    t2 = time.time()
    suffix = ""
    if vcount > 1 :
        suffix = "s"
    print "Checked %s volume%s in %s sec" % ( vcount,suffix, (t2-t1))
