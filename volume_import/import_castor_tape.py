#!/usr/bin/env python
import os
import copy
import time
import file_clerk_client
import volume_clerk_client
import e_errors
import pnfs
import popen2
import stat
import sys
import getopt
import traceback

KB=1024
MB=KB*KB
GB=MB*KB

def castor2enstoreadler32(signed):
    if signed < 0L:
        return 0x100000000+signed
    else:
        return signed
    
def location_to_cookie(location):
    loc = 1+ (int(location)-1)*3
    return '%04d_%09d_%07d' % (0, 0, loc)

class FileEntry:
    def __init__(self, fcc, file_path, report, castor_path):
        self.file_path = file_path
        self.report = report
	self.old_castor_path= castor_path
        self.fd = copy.deepcopy(self.file_entry)
        self.atime = 0.
        self.mtime = 0.
        self.ctime = 0.
        self.fcc = fcc
    
    name_map={'fileid': 'fileid',
              'parent_fileid': 'parent_fileid',
              'name': 'pnfs_name0',
              'owner_uid':'uid',
              'gid': 'gid',
              'filesize': 'size',
              'fseq': 'location_cookie',
              'atime': 'atime',
              'mtime': 'mtime',
              'ctime': 'ctime',
              'vid': 'external_label',
              'checksum': 'complete_crc'
              }

    file_entry = {'complete_crc': 0L,
                  'deleted': 'no',
                  'drive': '',
                  'external_label': '',
                  'gid': 0,
                  'location_cookie': '',
                  'pnfs_name0': '',
                  'pnfsid': '',
                  'sanity_cookie': (None,None),
                  'size': 0L,
                  'uid': 0,
                  'update': ''}

    
    def __repr__(self):
        return "%s"%(self.fd,)
        
    def read_castor_entry(self, f):
        l = f.readline()
        if l:
            if l.find('+--') != -1:
                l = f.readline()
                if not l: return None
                l=l[:-1]
            if "|" in l:
                lst = l.split('|')
            else:
                lst = l.split()
            lst1 = []
            for i in lst:
                if i:
                    lst1.append(i.strip())
            return lst1

    def get_castor_parent(self, fid):
        cmd='mysql -h castorsrv1 -s --user=enstore --password=enstore -e "select parent_fileid,name from Cns_file_metadata where fileid = '+"'"+str(fid)+"';"+'" cns_db'
        f = os.popen(cmd)
        op = f.readlines()
        f.close()
        l = op[0].split('\t')
        
        return { 'pfid': l[0], 'name': l[1].strip('\n') }


    def get_castor_name(self, fid):
        if fid != str(0):
            pea = self.get_castor_parent(fid)
            path = self.get_castor_name(pea['pfid'])
            path = os.path.join(path, pea['name'])
            return path
        

    def create_file_entry(self, src_keys, lst):
        print "LST (2)"
	print self.old_castor_path
        print lst
        if int(lst[src_keys.index('fsec')]) != 1:
            # file is spread across multiple tapes. Enstore can not read this file
            self.report.write("File is spread across multiple tapes. File %s, position %s, full record %s. Will not be imported \n"%(lst[0], lst[4], lst,))
            return 1
        for src_key in src_keys:
            if src_key == 'owner_uid':
                self.fd['uid'] = int(lst[src_keys.index('owner_uid')])
            elif src_key == 'fileid':
                self.fd['fileid'] = int (lst[src_keys.index('fileid')])
            elif src_key == 'parent_fileid':
                self.fd['parent_fileid'] = int (lst[src_keys.index('parent_fileid')])
            elif src_key == 'gid':
                self.fd['gid'] = int(lst[src_keys.index('gid')])
            elif src_key == 'filesize':
                self.fd['size'] = long(lst[src_keys.index('filesize')])
            elif src_key == 'fseq':
                print "FSEQ", src_keys.index('fseq'),lst[src_keys.index('fseq')] 
                self.fd['location_cookie'] = location_to_cookie(lst[src_keys.index('fseq')])
            elif src_key == 'vid':
                self.fd['external_label'] = lst[src_keys.index('vid')]
            elif src_key == 'checksum':
                self.fd['complete_crc'] = castor2enstoreadler32(long(lst[src_keys.index('checksum')]))
            elif src_key == 'mtime':
                self.mtime = float(lst[src_keys.index('mtime')])
                self.fd['update'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.mtime))
            elif src_key == 'atime':
                self.atime = float(lst[src_keys.index('atime')])
            elif src_key == 'ctime':
                self.ctime = float(lst[src_keys.index('ctime')])
            
    
        castor_file_path = self.get_castor_name(self.fd['fileid'])
        print "DEBUG CASTOR PATH FILE: %s"%(castor_file_path)
	print "DEBUG CASTOR PATH PATTERN: %s"%(self.old_castor_path)
	list_path_file = castor_file_path.split("/")
	list_castor_pattern = self.old_castor_path.split("/")
        for i in range(len(list_castor_pattern)):
	    if list_path_file[0]==list_castor_pattern[0]:
		list_castor_pattern.pop(0);
		list_path_file.pop(0)
		print list_path_file
	    else:
		print "Not path match. Can not proceed"
		return 1
        list_path_file.pop(len(list_path_file)-1)
	internal_path="/".join(list_path_file)
        self.fd['pnfs_name0'] = os.path.join(self.file_path,internal_path,lst[src_keys.index('name')])
	print "DEBUG PNFS PATH: %s"%(self.fd['pnfs_name0'])
        ## some translation from the "castor file path" to the
        ## "enstore file path" is needed


        # create bit file entry
        if os.path.exists(self.fd['pnfs_name0']):
            print "File %s exists. Can not proceed"%(self.fd['pnfs_name0'],)
            return 1
        print "FD",self.fd
        ticket = {'fc':self.fd}
        print "Create BFID for %s"%(self.fd['pnfs_name0'],)
        rticket = self.fcc.create_bit_file(ticket['fc'])
        if rticket['status'][0] == e_errors.OK:
            print 'BFID', rticket['fc']['bfid']

            # create pnfs entry
            print "Create file %s"%(self.fd['pnfs_name0'],)
            dir_name = os.path.dirname(self.fd['pnfs_name0'])
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
            print "CREATE FILE %s, %s"%(self.fd['pnfs_name0'], dir_name)
            p = pnfs.Pnfs(self.fd['pnfs_name0'])
            p.creat(mode=0666)
            p.set_file_size(self.fd['size'])
            p.chown(self.fd['uid'], self.fd['gid']) #Uncomment this for a real case
            p.set_bit_file_id(rticket['fc']['bfid'])
            p.set_xreference(self.fd['external_label'],
                             self.fd['location_cookie'],
                             self.fd['size'],
                             "castor",
                             self.fd['pnfs_name0'],
                             "",
                             p.get_id(self.fd['pnfs_name0']),
                             "",
                             rticket['fc']['bfid'],
                             "castor",
                             self.fd['complete_crc'],
                             self.fd['pnfs_name0'])
            ticket['fc']['pnfsid'] = p.get_id(self.fd['pnfs_name0'])
            ticket['fc']['bfid'] = rticket['fc']['bfid']
            rc = self.fcc.set_pnfsid(ticket)
            print "set_pnfsid returned", rc
        
        
        
    def create_entry(self, src_keys, f):
        lst = self.read_castor_entry(f)
        if lst:
            if not src_keys:
                return lst
            else:
               self.create_file_entry(src_keys, lst)
               return []
        else:
            return -1


class CastorTape:

    volume_entry = {'blocksize': 131072,
                    'capacity_bytes': 0L,
                    'comment': '',
                    'declared': 0.0,
                    'eod_cookie': '',
                    'external_label': '',
                    'first_access': 0.0,
                    'last_access': 0.0,
                    'library': '',
                    'media_type': '',
                    'remaining_bytes': 0L,
                    'si_time': [0.0, 0.0],
                    'sum_mounts': 0,
                    'sum_rd_access': 0,
                    'sum_rd_err': 0,
                    'sum_wr_access': 0,
                    'sum_wr_err': 0,
                    'system_inhibit': ['none', 'none'],
                    'user_inhibit': ['none', 'none'],
                    'volume_family': 'castor.castor.cern',
                    'wrapper': 'cern',
                    'write_protected': 'n'}

    def __init__(self, volume_name, library, volume_size, media, file_entry_path, pnfs_path, castor_path, vo_name):
        print "INIT", volume_name, volume_size, media, file_entry_path, pnfs_path, report_path,library
        self.f = os.path.join("/tmp", volume_name);
        print "F", self.f
        self.path = pnfs_path
	self.old_castor_path = castor_path
	self.voname = vo_name
	print "INIT CASTOR: %s"%(self.old_castor_path)
	vol_path = os.path.join(file_entry_path, volume_name)
	if not os.path.exists(vol_path):
	       os.makedirs(vol_path)
        if not os.path.exists(self.f):
            cmd='mysql -h castorsrv1 --user=enstore --password=enstore -e "select fileid,parent_fileid,name,owner_uid,gid,filesize,fseq,atime,mtime,ctime,vid,checksum,fsec from Cns_file_metadata f, Cns_seg_metadata s where f.fileid = s.s_fileid and vid='+"'"+volume_name+"';"+'" cns_db>'+ self.f
            rstat = os.system(cmd)
            if rstat != 0:
                print "Error. check %s"(self.f,)
                self.empty=1
                return
        if os.stat(self.f)[stat.ST_SIZE] == 0:
            print "Castor volume %s is empty"%(self.f,)
            self.empty = 1
            return
        self.empty = 0
        self.report = open(os.path.join("/tmp", "report"), 'w')
        csc = (os.environ['ENSTORE_CONFIG_HOST'],
               int(os.environ['ENSTORE_CONFIG_PORT']))

        self.fcc = file_clerk_client.FileClient(csc, 0, None, 0, 0)
        self.vcc = volume_clerk_client.VolumeClerkClient(csc, None,  0, 0)
        self.vd = copy.deepcopy(self.volume_entry)
        self.vd['external_label'] = volume_name
        self.vd['capacity_bytes'] = volume_size
        self.vd['media_type'] = media
        self.vd['library'] = library
	self.vd['volume_family'] = 'vo-'+vo_name+'.castor.cern',
           
                    
    def create_volume_entry(self):
        print "VOL", self.vd
        rc = self.vcc.add(self.vd['library'],
                          'castor',           # volume family the media is in
                          'vo-'+vo_name,         # storage group for this volume
                          self.vd['media_type'],            # media
                          self.vd['external_label'],        # label as known to the system
                          self.vd['capacity_bytes'],        #
                          eod_cookie  = "none",  # code for seeking to eod
                          user_inhibit  = ["none","readonly"],# 0:"none" | "readonly" | "NOACCESS"
                          error_inhibit = "none",# "none" | "readonly" | "NOACCESS" | "writing"
                          last_access = time.time(),      # last accessed time
                          first_access = time.time(),     # first accessed time
                          declared = -1,         # time volume was declared to system
                          sum_wr_err = 0,        # total number of write errors
                          sum_rd_err = 0,        # total number of read errors
                          sum_wr_access = 0,     # total number of write mounts
                          sum_rd_access = 0,     # total number of read mounts
                          wrapper = "cern",  # kind of wrapper for volume
                          blocksize = self.vd['blocksize'],        # blocksize (-1 =  media type specifies)
                          non_del_files = 0,     # non-deleted files
                          system_inhibit = ["none","readonly"], # 0:"none" | "writing??" | "NOACCESS", "DELETED
                          remaining_bytes = None,
                          timeout=60, retry=1
                          )

        print "CREATE VOL", rc['status']
        print rc['status'][0] 
        if rc['status'][0] != e_errors.OK:
            if rc['status'][0] != e_errors.VOLUME_EXISTS:
                print rc['status'][1], 'can not proceed'
                return 1
        vol_size, file_entries = self.create_file_entries()
        ticket={}
        ticket['external_label'] = self.vd['external_label']
        ticket['remaining_bytes'] = self.vd['capacity_bytes'] - vol_size
        ticket['eod_cookie'] = location_to_cookie(len(file_entries)+1)
        ticket['sum_wr_access'] = len(file_entries)
        self.vcc.modify(ticket)
        
    def create_file_entries(self):
        f=open(self.f, 'r')
        file_entries=[]
        vol_size = 0L
        src_keys = None
        f_entry = None
        while 1:
            last_f_entry = f_entry
            f_entry = FileEntry(self.fcc, self.path, self.report, self.old_castor_path)
            rc = f_entry.create_entry(src_keys, f)
            if rc and rc != -1:
                 src_keys = rc
            elif rc == -1:
                break
            else:
                file_entries.append(f_entry)
                vol_size = vol_size + f_entry.fd['size']
        self.report.close()
        return vol_size,file_entries 
                
            
def vsize(s):
    if s[len(s)-1] == "G":
        return long(s[:len(s)-1])*GB
    else: return 0L
        
def read_volume_entry(f):
    l1 = f.readline()
    l = l1.strip()
    if l:
        if l.find('+--') != -1:
            l = f.readline()
            if not l: return None
            l=l[:-1]
        lst = l.split(' ')
        lst1 = []
        for i in lst:
            if i:
                lst1.append(i.strip())
        return lst1

def create_volume_entry(src_keys, lst, file_entry_path, path, report, castor_path, library=None, vo_name=None):
    for src_key in src_keys:
        if src_key == 'vid':
            vol = lst[src_keys.index('vid')]
        elif src_key == 'density':
            capacity = vsize((lst[src_keys.index('density')]))
        elif src_key == 'model':
            media = lst[src_keys.index('model')]
    work_dir=os.path.join(file_entry_path,vol)
    if not os.path.exists(work_dir):
        os.makedirs(work_dir)
    outf = os.path.join("/tmp", vol+".out")
    saved_stdout = sys.stdout
    sys.stdout = open(outf, "w")
    try:
        s='-'
        if library == None:
            lib = s.join(('castor', media))
        else:
            lib = library
        print "LIBRARY", lib

        tape = CastorTape(vol, lib, capacity, media, file_entry_path, path, castor_path, vo_name)
        if tape.empty:
            print "tape is empty"
        else:
            tape.create_volume_entry()
            print "tape is done, no errors detected"
    except:
        exc, value, tb = sys.exc_info()
        for l in traceback.format_exception( exc, value, tb ):
            print l
    sys.stdout.flush()
    sys.stdout = saved_stdout
            
        
def create_entry(src_keys, f, file_entry_path, path, report, castor_path, library=None, vo_name=None):
    lst = read_volume_entry(f)
    if lst:
        if not src_keys:
            return lst
        else:
            create_volume_entry(src_keys, lst, file_entry_path, path, report, castor_path, library, vo_name)
            return []
    else:
        return -1

def usage():
    print "usage: %s [-h] [-l library] [-p pnfs_path] [-v vo_name]"%(sys.argv[0],)
 
if __name__ == "__main__":
    library = None
    pnfs_path = None
    opts, args = getopt.getopt(sys.argv[1:], "l:p:v:hs",[])
 
    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit(0)
        if opt == '-l':
            library = arg
        if opt == '-p':
            pnfs_path = arg
        if opt == '-v':
            vo_name = arg
    print library
    print pnfs_path
     
    v='/home/enstore/castor_tapes/castor_volumes'
    file_list_path = "/home/enstore/castor_tapes"
    if pnfs_path == None:
     #    pnfs_path = "/pnfs/fs/usr/tape/test/imported_from_castor"
     #   pnfs_path = "/pnfs/fs/usr/tape/test/castor"
        ifile='/home/enstore/castor_tapes/VO_path'
        df = open(ifile, 'r')
        for line in df.readlines():
            line = line.strip()
            if line == "":
                continue
            list = line.split(' ')
            if list[0] != vo_name:
                continue
            else:
                castor_path=list[1]
                pnfs_path=list[2]
             
        print "will use default pnfs path", pnfs_path
    if library == None:
        print "will use default library"
    report_path = "/home/enstore/castor_tapes"
    src_keys = None
    f = open(v, 'r')
    while 1:
        stdout = sys.stdout
        rc = create_entry(src_keys, f, file_list_path, pnfs_path, report_path, castor_path, library, vo_name)
        sys.stdout = stdout
        if rc and rc != -1:
            src_keys = rc
        elif rc == -1:
            break
