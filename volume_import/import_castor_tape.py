
import os
import copy
import time
import file_clerk_client
import volume_clerk_client
import e_errors
import pnfs

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
    def __init__(self, fcc, file_path, report):
        self.file_path = file_path
        self.report = report
        self.fd = copy.deepcopy(self.file_entry)
        self.atime = 0.
        self.mtime = 0.
        self.ctime = 0.
        self.fcc = fcc
    
    name_map={'name': 'pnfs_name0',
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
                  #                  'sanity_cookie': None,
                  'sanity_cookie': (None,None),
                  'size': 0L,
                  'uid': 0,
                  'update': ''}

    
    def __repr__(self):
        return "%s"%(self.fd,)
        
    def read_castor_entry(self, f):
        l = f.readline()
        #print "L %s"%(l,)
        if l:
            if l.find('+--') != -1:
                l = f.readline()
                if not l: return None
                l=l[:-1]
            lst = l.split('|')
            lst1 = []
            for i in lst:
                if i:
                    lst1.append(i.strip())
            return lst1

    def create_file_entry(self, src_keys, lst):
        if int(lst[src_keys.index('fsec')]) != 1:
            # file is spread across multiple tapes. Enstore can not read this file
            self.report.write("%s\n"%(lst,))
            return 1
        for src_key in src_keys:
            #if src_key == 'name':
            #    name = os.path.join(self.file_path, lst.index('name'))
            if src_key == 'owner_uid':
                #print "UID", src_keys.index('owner_uid'), lst[src_keys.index('owner_uid')]
                self.fd['uid'] = int(lst[src_keys.index('owner_uid')])
            elif src_key == 'gid':
                #print "GID", src_keys.index('gid'), lst[src_keys.index('gid')]
                self.fd['gid'] = int(lst[src_keys.index('gid')])
            elif src_key == 'filesize':
                #print "FSIZE", src_keys.index('filesize'), lst[src_keys.index('filesize')]
                self.fd['size'] = long(lst[src_keys.index('filesize')])
            elif src_key == 'fseq':
                #print "FSEQ", src_keys.index('fseq'),lst[src_keys.index('fseq')] 
                self.fd['location_cookie'] = location_to_cookie(lst[src_keys.index('fseq')])
            elif src_key == 'vid':
                #print "VID", src_keys.index('vid'), lst[src_keys.index('vid')]
                self.fd['external_label'] = lst[src_keys.index('vid')]
            elif src_key == 'checksum':
                #print "CHECKSUM",src_keys.index('checksum'), lst[src_keys.index('checksum')] 
                self.fd['complete_crc'] = castor2enstoreadler32(long(lst[src_keys.index('checksum')]))
            elif src_key == 'mtime':
                self.mtime = float(lst[src_keys.index('mtime')])
                self.fd['update'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.mtime))
            elif src_key == 'atime':
                self.atime = float(lst[src_keys.index('atime')])
            elif src_key == 'ctime':
                self.ctime = float(lst[src_keys.index('ctime')])
            
    
        self.fd['pnfs_name0'] = os.path.join(self.file_path,self. fd['external_label'],lst[src_keys.index('name')])
        # create bit file entry
        if os.path.exists(self.fd['pnfs_name0']):
            print "File %s exists. Can not proceed"%(self.fd['pnfs_name0'],)
            return 1
        print "FD",self.fd
        ticket = {'fc':self.fd}
        print "Create BFID for %s"%(self.fd['pnfs_name0'],)
        rticket = self.fcc.create_bit_file(ticket['fc'])
        print 'File clerk returned', rticket
        if rticket['status'][0] == e_errors.OK:
            print 'BFID', rticket['fc']['bfid']

            # create pnfs entry
            print "Create file %s"%(self.fd['pnfs_name0'],)
            dir_name = os.path.dirname(self.fd['pnfs_name0'])
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
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
        #print "LST", lst
        if lst:
            if not src_keys:
                return lst
                #src_keys = []
                #for i in self.name_map.keys():
                #    if i in lst:
                #        src_keys.append(i)
                #    else:
                #        return None
                #return src_keys
            else:
               self.create_file_entry(src_keys, lst)
               #print self.fd
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

    def __init__(self, volume_name, library, volume_size, media, file_entry_path, pnfs_path):
        print "INIT", volume_name, volume_size, media, file_entry_path, pnfs_path, report_path
        self.f = os.path.join(file_entry_path,volume_name, volume_name)
        print "F", self.f
        self.path = pnfs_path
        print "PATH",os.path.join(file_entry_path, volume_name, "report") 
        self.report = open(os.path.join(file_entry_path, volume_name, "report"), 'w')
        csc = (os.environ['ENSTORE_CONFIG_HOST'],
               int(os.environ['ENSTORE_CONFIG_PORT']))

        self.fcc = file_clerk_client.FileClient(csc, 0, None, 0, 0)
        self.vcc = volume_clerk_client.VolumeClerkClient(csc, None,  0, 0)
        self.vd = copy.deepcopy(self.volume_entry)
        self.vd['external_label'] = volume_name
        self.vd['capacity_bytes'] = volume_size
        self.vd['media_type'] = media
        self.vd['library'] = library
           
                    
    def create_volume_entry(self):
        print "VOL", self.vd
        rc = self.vcc.add(self.vd['library'],
                          'castor',           # volume family the media is in
                          'castor',         # storage group for this volume
                          self.vd['media_type'],            # media
                          self.vd['external_label'],        # label as known to the system
                          self.vd['capacity_bytes'],        #
                          eod_cookie  = "none",  # code for seeking to eod
                          user_inhibit  = ["none","readonly"],# 0:"none" | "readonly" | "NOACCESS"
                          error_inhibit = "none",# "none" | "readonly" | "NOACCESS" | "writing"
                          last_access = -1,      # last accessed time
                          first_access = -1,     # first accessed time
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
            f_entry = FileEntry(self.fcc, self.path, self.report)
            rc = f_entry.create_entry(src_keys, f)
            print "RC",rc
            if rc and rc != -1:
                 src_keys = rc
                 #print "SRC_KEYS",src_keys
                 #for k in src_keys:
                 #    print k,src_keys.index(k) 
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
    #print "L %s"%(l,)
    if l:
        if l.find('+--') != -1:
            l = f.readline()
            if not l: return None
            l=l[:-1]
        #lst = l.split('\t')
        lst = l.split(' ')
        lst1 = []
        for i in lst:
            if i:
                lst1.append(i.strip())
        return lst1

def create_volume_entry(src_keys, lst, file_entry_path, path, report):
    print 'SRC_KEYS', src_keys
    for src_key in src_keys:
        if src_key == 'vid':
            vol = lst[src_keys.index('vid')]
        elif src_key == 'density':
            capacity = vsize((lst[src_keys.index('density')]))
        elif src_key == 'model':
            media = lst[src_keys.index('model')]
    print vol,capacity, media
    s='-'
    library = s.join(('castor', media))
    print "LIBRARY", library

    tape = CastorTape(vol, library, capacity, media, file_entry_path, path)
    print "vol",tape.vd
    tape.create_volume_entry()
        
        
def create_entry(src_keys, f, file_entry_path, path, report):
    lst = read_volume_entry(f)
    print "LST", lst
    if lst:
        if not src_keys:
            return lst
        else:
            create_volume_entry(src_keys, lst, file_entry_path, path, report)
            return []
    else:
        return -1

            

if __name__ == "__main__":
    v='/home/enstore/castor_tapes/castor_volumes'
    file_list_path = "/home/enstore/castor_tapes"
    pnfs_path = "/pnfs/fs/usr/tape/test/imported_from_castor"
    report_path = "/home/enstore/castor_tapes"
    src_keys = None
    f = open(v, 'r')
    while 1:
        rc = create_entry(src_keys, f,  file_list_path, pnfs_path, report_path)
        print "RC",rc
        if rc and rc != -1:
            src_keys = rc
            print "SRC_KEYS",src_keys
        elif rc == -1:
            break
    #ct = CastorTape('/home/enstore/PIC/G03040/G03040', "/pnfs/data1/imported_from_castor", "9940B-castor", "9940B", '/home/enstore/PIC/G03040/report')
    #ct.create_file_entries()
    
