#!/usr/bin/env python
import os
import sys
import string
import rexec
import file_clerk_client
import configuration_client
import stat

class FileList:
    def __init__(self, input_string):
        self.ncalls = 0
        self.file = None
        self.file_list = None
        s = sys.argv[1].split('/')
        if s[0] == '' and s[1] == 'pnfs':
            # pnfs file
            self.file=s
        else:
            # file contains list of pnfs files
            self.file_list = open(s,'r')

    def get_file(self):
        if self.file:
            if self.ncalls == 0:
                self.ncalls = self.ncalls + 1
                return self.file
            else:
                return None
        return self.file_list.readline()[:-1]
        
def usage():
    print "usage: %s file_name|list_of_files"%(sys.argv[0])

def read_args():
    if len(sys.argv) != 2:
        usage()
        sys.exit()
    return FileList(sys.argv[1])
    
def readlayer(fullname,layer):
    (dir,fname)=os.path.split(fullname)
    fname = "%s/.(use)(%s)(%s)"%(dir,layer,fname)
    try:
        f = open(fname,'r')
    except:
        exc, msg, tb = sys.exc_info()
        print "exception: %s %s" % (str(exc), str(msg))
        return None
        
    l = f.readlines()
    f.close()
    return l

def touch_file(fn, size):
    (dir,fname)=os.path.split(fn)
    fname = "%s/'.(fset)(%s)(size)(%s)'"%(dir,fname, size)
    print fname
    os.system("echo touch %s"%(fname,))

def get_l4(filename):
    l4_raw = readlayer(filename, 4)
    l4 = {}
    if l4_raw:
        l4['external_label'] = l4_raw[0][:-1]  # volume
        l4['location_cookie'] = l4_raw[1][:-1]  # location cookie
        l4['size'] = long(l4_raw[2][:-1]) # file size
        l4['file_family'] = l4_raw[3][:-1] # file family
        l4['pnfs_name0'] = l4_raw[4][:-1]  # file name
        l4['pnfs_mapname'] = l4_raw[5][:-1]  # volmap name
        l4['pnfsid'] = l4_raw[6][:-1]  # pnfs id
        l4['pnfsvid'] = l4_raw[7][:-1]  # pnfs vid
        l4['bfid'] = l4_raw[8][:-1]  # bfid
        #l4['drive'] = l4_raw[9][:-1]  # drive
    return l4

def compare(bfinfo, l4):
    #keys = l4.keys()
    keys=['external_label','location_cookie','size'] 
    for key in keys:
        if bfinfo[key] != l4[key]: break
    else:
        return 1
    return 0


#touch_file('/pnfs/fs/usr/NULL2/jon/tst2',1000)
#sys.exit()

f_list = read_args()
host = os.environ.get('ENSTORE_CONFIG_HOST', 0)
port = os.environ.get('ENSTORE_CONFIG_PORT', 0)
port = int(port)
csc = configuration_client.ConfigurationClient((host,port))
fcc = file_clerk_client.FileClient(csc)
ask=1
fixed_files=0
while 1:
    fix = 0
    fn = f_list.get_file()
    if fn:
        try:
            fsize = os.stat(fn)[stat.ST_SIZE]
        except:
            exc, msg, tb = sys.exc_info()
            print "exception: %s %s" % (str(exc), str(msg))
            continue
            
        if fsize == 0:
            l4 = get_l4(fn)
            if l4:
                #print fn
                # check if L$ is consistent with file db info
                bfinfo = fcc.bfid_info(l4['bfid'])
                if compare(bfinfo, l4):
                    fsize_str = "%s"%(l4['size'])
                    if ask:
                        try:
                            reply = raw_input("fix %s? [y/n/i]"%(fn,))
                        except:
                            sys.exit()
                        if reply == 'y':
                            fix = 1
                        elif reply == 'n':
                            pass
                        else:
                           ask=0
                           fix = 1
                    else:
                        fix = 1
                    if fix:
                        try:
                            touch_file(fn, fsize_str)
                            fixed_files=fixed_files+1
                            print "fixed",fixed_files
                        except:
                            exc, msg, tb = sys.exc_info()
                            print "exception: %s %s" % (str(exc), str(msg))
                            continue

    else: break
