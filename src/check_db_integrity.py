#!/usr/bin/env python

# $Id$
import sys
import os
import pwd
import getopt
import string
import errno
import time
import e_errors
import traceback
import db
import pnfs
import bfid_db
import pprint
import volume_family

opts_permitted = ['interactive', 'fix_file_db', 'fix_bfid_db',
                  'fix_all','scan_pnfs', 'fix_pnfs','fix_volmap','vol=']
def parse_command_line(command_line):
    try:
        loptions, args =getopt.getopt(sys.argv[1:], [], opts_permitted)
        print "LOPT",loptions
        options = []
        for opt in loptions:
            options.append(opt[0][2:])
    except:
        args = options = []
    return args, options, loptions

def check_permissions(options):
    mod_req = 0
    for option in options:
        #print "11111111",option
        if string.find(option,'fix') >= 0:
            #print "222222222222"
            mod_req = 1
            break
    if mod_req:
        # modification of some DBs required
        # check if modification is permitted for this user
        user_name = pwd.getpwuid(os.getuid())[0]
        #print "Login name", user_name
        if user_name == 'enstore':
            return 1
        #####################
        ## remove this when debugging complete
        if user_name == 'moibenko':
            hostname = os.uname()[1]
            if string.find(hostname,'happy.fnal.gov') >= 0:
                return 1
        #####################
        
        else: return 0
    return 1

def usage():
    print 'usage: %s [options] DB_DIR JOURNAL_DIR [Report file DIR]' % (sys.argv[0],)
    opt_list = ''
    for opt in opts_permitted:
        opt_list = string.join((opt_list, string.join(("--",opt),'')), ' ')
    print 'options: %s' % (opt_list,)
    
class DBChecker:

    def __init__(self, dbHome, jouHome, options):
        self.options = options
        self.dbHome = dbHome
        self.jouHome = jouHome
        self.vol_db = db.DbTable("volume", self.dbHome, self.jouHome, ['library', 'volume_family'])
        self.file_dict = db.DbTable("file", self.dbHome, self.jouHome)
        self.bfid_db=bfid_db.BfidDb(self.dbHome)

        #rec = self.vol_dict['null00']
        #print "VOL_DICT"
        self.volume_list=[]
        self.vol_db.cursor("open")
        key,value=self.vol_db.cursor("first")
        while key:
            #print "KEY %s VALUE %s" % (key, value)
            # we are not interested in the deleted volumes
            ff = volume_family.extract_file_family(value['volume_family'])
            if ((value['external_label'] not in self.volume_list) and
                (value['system_inhibit'][0] != e_errors.DELETED) and
                ff != 'CleanTapeFileFamily'):
                self.volume_list.append(value['external_label'])
            key,value=self.vol_db.cursor("next")
        self.vol_db.cursor("close")

        self.bad_volumes = {} # here will be any bad

    def check_file_db_record(self, record):
        bad_record={}
        # bad record structures is as follows:
        # volume
        # file_db_record - copy of file database record
        # missing_keys - list of missing keys
        rec_keys = record.keys()
        rec_keys.sort()
        #print "rec_keys",rec_keys
        must_keys = ['bfid', 'complete_crc', 'deleted',
                     'external_label', 'location_cookie', 'pnfs_mapname',
                     'pnfs_name0', 'pnfsid', 'pnfsvid', 'sanity_cookie', 'size']
        must_keys.sort()
        #print "must_keys",must_keys
        missing_keys = []
        # find what key(s) is missing
        for must_key in must_keys:
            if must_key not in rec_keys:
                #print "KEY %s is missing in the file record %s" % (must_key,record)
                missing_keys.append(must_key)
        if not record.has_key('drive'):
            # special case
            # just put there something
            # this may not work because we are running through indices
            # check !!!!!!!!!!!!!!!!
            #print "DRIVE MISSING",record
            record['drive'] = 'unknown'
            if record not in self.records_to_fix:
                if len(self.records_to_fix) < 1:
                    print "adding",record
                    self.records_to_fix.append(record)
        if missing_keys:
            bad_record['file_db_record'] = record
            bad_record['missing_keys'] = missing_keys
        #print "BAD RECORD",bad_record
        return bad_record
        

    def readlayers(self, pnfs_object):
        l=[]
        l.append(None)
        l.append(pnfs_object.readlayer(1))
        l.append(None)
        l.append(None)
        l.append(map(string.strip, pnfs_object.readlayer(4)))
        return l

    def prepare_for_interactive_input(self):
        self.interactive = 0
        if 'interactive' in self.options: self.interactive = 1

    def interactive_input(self, prompt):
        if not self.interactive: return self.fix
        # if interactive then ask user what does he want to do
        answers = '[y/n/a]: '
        while 1:
            try:
                reply = raw_input(string.join((prompt,answers),''))
                if 'y' in reply or 'n' in reply or 'a' in reply:
                    break
                else:
                    print "please type any of the following: %s" % (answers,)
            except SyntaxError:
                print "syntax error"
            
        if 'y' in reply:
            self.fix = 1
        elif 'n' in reply:
            self.fix = 0
        elif 'a' in reply:
            self.interactive = 0
        return self.fix

    def fix_bfid_db(self, external_label, missing_bfids):
        print "Fixing bfid DB for %s records" % (len(missing_bfids),)
        print "volume", external_label
        print "missing_bfids"
        pprint.pprint(missing_bfids)
        records_fixed = 0
        self.fix = 1  # set fix flag
        self.prepare_for_interactive_input()
        fixed_bfids = []
        for bfid in missing_bfids:
            self.fix = self.interactive_input('fix bfid %s?'% (bfid,))
            if self.fix:
                print "Fixing bfid DB: %s" % (bfid,)
                try:
                    self.bfid_db.add_bfid(external_label, bfid)
                except (IOError,IndexError), detail:
                    print "Exception %s" % (str(detail),)
                    print "ERRNO",errno.errorcode[detail.errno]
                    if detail.errno == errno.ENOENT:
                        try:
                            print "trying to initialize dbfile for %s"%(external_label,)
                            self.bfid_db.init_dbfile(external_label)
                            self.bfid_db.add_bfid(external_label, bfid)
                        except (IOError,IndexError), detail:
                            print "Exception %s" % (str(detail),)
                            print "Give up on fixing bfid_db"
                    else:
                        break
                fixed_bfids.append(bfid)
                records_fixed = records_fixed + 1
        for bfid in fixed_bfids:
            missing_bfids.remove(bfid)
        del(fixed_bfids)

        print "Fixed %s bfids" % (records_fixed,)
        return missing_bfids

    def cookie_to_long(self, cookie): # cookie is such a silly term, but I guess we're stuck with it :-(
        if not cookie: return 0L
        if type(cookie) is type(0L):
            return cookie
        if type(cookie) is type(0):
            return long(cookie)
        if type(cookie) != type(''):
            raise TypeError, "expected string or integer, got %s %s" % (cookie, type(cookie))
        if '_' in cookie:
            part, block, file = string.split(cookie, '_')
        else:
            file = cookie
            if file == 'none':
                file = '0'
            
        if file[-1]=='L':
            file = file[:-1]
        return long(file)
    
    def bad_pnfs_layer1(self, file, layer1):
        bad_layer = 0
        if type(layer1) != type([]):
            print "wrong layer 1 type(%s) for %s. Must be %s" % (type(layer1),file['pnfs_name0'], type([]))
            print "layer 1 entires"
            pprint.pprint(layer1)
            bad_layer = 1
        else:
            if len(layer1) != 1:
                print "wrong layer 1 length(%s) for %s. Must be 1" % (len(layer1),file['pnfs_name0'])
                print "layer 1 entires"
                pprint.pprint(layer1)
                bad_layer = 1

            l1_bfid = layer1[0]
            if file['bfid'] != l1_bfid:
                print "L1 did not compare for %s" % (file['pnfs_name0'],)
                print "F BFID %s L1 BFID %s"%(file['bfid'], l1_bfid)
                bad_layer = 1
        return bad_layer

    def bad_pnfs_layer4(self, file, file_family, layer4):
        bad_layer = 0
        # fill the list of layer 4 entries
        if type(layer4) != type([]):
            print "wrong layer 4 type(%s) for %s. Must be %s" % (type(layer4),file['pnfs_name0'], type([]))
            print "layer 1 entires"
            pprint.pprint(layer4)
            bad_layer = 1
        else:
            if len(layer4) != 10:
                print "wrong layer 4 length(%s) for %s. Must be 10" % (len(layer4),file['pnfs_name0'])
                print "layer 4 entires"
                pprint.pprint(layer4)
                bad_layer = 1
            else:
                # fill the list of layer 4 entries
                v,cookie,size,ff,pfname,mapfname,pnfsid,pnfsmid,bid,dev = layer4
                size = bfid_db.safe_atol(size)

                if v != file['external_label']:
                    print "EXT_L",v, file['extrenal_label']
                if cookie != file['location_cookie']:
                    print "COOKIE",cookie, file['location_cookie'] 
                if size != file['size']:
                    print "TYPES %s %s"%(type(size),type(file['size']))
                    print "SIZE",size,file['size']
                if ff != file_family:
                    print "FF",ff, file_family
                if pfname != file['pnfs_name0']:
                    print "FNAME",pfname,file['pnfs_name0']
                if mapfname != file['pnfs_mapname']:
                    print "MAPNAME", mapfname,file['pnfs_mapname']
                if pnfsid != file['pnfsid']:
                    print "PID",pnfsid, ['pnfsid'] 
                if bid != file['bfid']:
                    print "BFID",bid,file['bfid'] 
                if dev != file['drive']:
                    print "DEV",dev,file['drive'] 

                if not (v == file['external_label'] and
                        cookie == file['location_cookie'] and
                        size == file['size'] and
                        ff == file_family and
                        pfname == file['pnfs_name0'] and
                        mapfname == file['pnfs_mapname'] and
                        pnfsid == file['pnfsid'] and
                        bid == file['bfid'] and
                        dev == file['drive']):
                    print "L4 did not compare for %s" % (file['pnfs_name0'],)
                    bad_layer = 1

            return bad_layer
    

    def check_pnfs_entry(self, file, file_family):
        print "================================================"
        print "Checking pnfs entry for %s" % (file['pnfs_name0'],)
        #print "FILE",file
        pinfo = pnfs.Pnfs(file['pnfs_name0'],get_details=0,get_pinfo=0,timeit=0)
        pl = self.readlayers(pinfo)
        #print "PLS",pl
        # see if mapfile exists
        #print "EXISTS",file['pnfs_name0'], pinfo.exists
        #print "PINFO:volume %s, location_cookie %s, size %s, origff %s, origname %s, mapfile %s" % (pinfo.volume, pinfo.location_cookie, pinfo.size, pinfo.origff, pinfo.origname, pinfo.mapfile)

        vmap = pnfs.Pnfs(file['pnfs_mapname'], get_details=0,get_pinfo=0,timeit=0)
        #print "MAPEXISTS",file['pnfs_mapname'], map.exists
        ml = self.readlayers(vmap)
        bad_entries = []
        bad_map_layers = []
        bad_pnfs_layers = []
        
        # check if pnfs entries are correct
        # check layer 1
        if self.bad_pnfs_layer1(file, pl[1]):
          bad_pnfs_layers.append(1)  

        # check layer 4
        if self.bad_pnfs_layer4(file, file_family, pl[4]):
           bad_pnfs_layers.append(4) 

        if 4 in bad_pnfs_layers:
            print "BAD LAYER 4 for %s"%(file['pnfs_name0'],)
            pprint.pprint(pl[4])

        # check if map layesr are correct
        # check layer 1
        if self.bad_pnfs_layer1(file, ml[1]):
          bad_map_layers.append(1)  

        # check layer 4
        if self.bad_pnfs_layer4(file, file_family, ml[4]):
           bad_map_layers.append(4) 

        if 4 in bad_map_layers:
            print "BAD MAP LAYER 4 for %s"%(file['pnfs_name0'],)
            pprint.pprint(ml[4])

        # compare layers
        if not bad_pnfs_layers: 
            for i in range(0,len(pl)):
                if pl[i] != ml[i]:
                    print "layer %s mismatch for %s" % (i, file['pnfs_name0'],)
                    print "Layer",i
                    print "PL",pl[i]
                    print "ML",ml[i]

        if bad_pnfs_layers or bad_map_layers:
            
            return {'bad_pnfs_layers': bad_pnfs_layers,
                    'bad_map_layers': bad_map_layers,
                    'pnfs_layer_1': pl[1],
                    'pnfs_layer_4': pl[4],
                    'map_layer_1': ml[1],
                    'map_layer_4': ml[4],
                    'pnfs': pinfo,
                    'map': vmap
                    }
        return {}
            
    def fix_layer1(self, file, entry):
        print "Fixing pnfs layer 1 for %s" % (file['pnfs_name0'],)
        print "layer 1 before fix"
        pprint.pprint(entry.readlayer(1))
        rc = 1
        self.fix = self.interactive_input('fix layer1 for %s?'% (file['pnfs_name0'],))
        if self.fix:
            print "Fixing layer 1 for %s" % (file['pnfs_name0'],)
        try:
            entry.set_bit_file_id(file['bfid'], file['size'])
        except:
            exc, value, tb = sys.exc_info()
            for l in traceback.format_exception( exc, value, tb ):
                print l[0:len(l)-1]
            rc = 0
        print "layer 1 after fix"
        pprint.pprint(entry.readlayer(1))
        return rc

    def write_record(self, record, file):
        pprint.pprint(record, file)
                                

    def check_file_db_for_vol(self, external_label,report_file):
        bad_volume = {}
        bad_file_db_records = []
        bad_files = {}
        print "+++++++++++++++++++++++++++++++++++++++++"
        print "VOLUME", external_label
        bfid_list = self.bfid_db.get_all_bfids(external_label)
        print "BFID LIST",bfid_list
        files=[]    # files belonging to the volume
        self.records_to_fix=[]
        vol_rec = self.vol_db[external_label]
        file_family = volume_family.extract_file_family(vol_rec['volume_family'])
        eod_cookie = vol_rec['eod_cookie']
        nfiles = self.cookie_to_long(eod_cookie)
        if nfiles != 0: nfiles = nfiles - 1
        tot_files = 0
        may_fix = 1  # if set volume and all related dbs can be fixed
        for bfid in bfid_list:
            record = self.file_dict[bfid]
            # check the consistensy of the file DB record
            bad_record = self.check_file_db_record(record)
            print "RF!!!",self.records_to_fix
            if bad_record:
                bad_file_db_records.append(bad_record)
            else:
                if record['deleted'] != 'yes':
                    # we are not interested in deleted files
                    files.append(record)
            tot_files = tot_files+1 # total files in file db for volume

        # check if number of files in file db corresponds to eod_cookie
        if tot_files != nfiles:
            # eod cookie migh have been set to None resulting in nfiles == 0
            # if so check the number of non deleted files but report this
            if nfiles == 0:
                print "Number of files according eod %s number of non deleted files %s" % (nfiles,vol_rec['non_del_files'])
                nfiles = vol_rec['non_del_files']
            if tot_files != nfiles:
                print "Number of files mismatch for %s" % (external_label,)
                print "%s files in file db %s files in volume db" % (tot_files, nfiles)
                bad_volume['nfiles_msmatch'] = "%s files in file db %s files in volume db" % (tot_files, nfiles)
                may_fix = 0 # very bad corruption, neither db can be fixed

        #fix records in the file db
        if ((('fix_file_db' in self.options) or ('fix_all' in self.options)) and
            self.records_to_fix):
            if may_fix:
                print "Fixing file DB for %s records" % (len(self.records_to_fix),)
                records_fixed = 0
                self.fix = 1  # set fix flag
                recs = []
                self.prepare_for_interactive_input()
                for rec in self.records_to_fix:
                    self.fix = self.interactive_input('fix this record? %s'%(rec,))
                    if self.fix:
                        print "Fixing file DB: %s" % (rec,)
                        self.file_dict[rec['bfid']] = rec
                        recs.append(rec)
                        records_fixed = records_fixed + 1
                print "Fixed %s records" % (records_fixed,)
                for rec in recs: self.records_to_fix.remove(rec)
            else:
                print "Database for %s is severely damaged and cannot be fixed"%(external_label,)

        if self.records_to_fix:
            print "RF2",self.records_to_fix
            bad_volume['file_db_records_to_fix'] = self.records_to_fix
            print "RF3",bad_volume['file_db_records_to_fix']
        ######################
        if bad_file_db_records:
            bad_volume['bad_file_db_records'] =  bad_file_db_records

        # check if all bfids exist in the volume bfid DB
        try:
            bfids = self.bfid_db.get_all_bfids(external_label)
        except (IOError,IndexError), detail:
            print "Exception %s" % (str(detail),)

        #print "VOL BFIDS",bfids
        missing_bfids = []
        for file in files:
            if file['bfid'] not in bfids:
                  missing_bfids.append(file['bfid'])
            if ('scan_pnfs' in self.options) and files:
                print "==========================================="
                # check if pnfs files exist, volmap files exist
                ret = self.check_pnfs_entry(file, file_family)
                # fix pnfs entry if possible
                fixed = 0
                #print "FIX_PNFS",('fix_pnfs' in self.options)
                if ((('fix_pnfs' in self.options) or ('fix_all' in self.options)) and
                    ret):
                    print "!!!!!!!!!!!!!!!!!!!!!!"

                    if may_fix:
                        # fix pnfs layer 1
                        if 1 in ret['bad_pnfs_layers'] and not file['bfid'] in missing_bfids:
                            fixed = self.fix_layer1(file,ret['pnfs'])
                            # fix map layer 1
                        if 1 in ret['bad_map_layers'] and not file['bfid'] in missing_bfids:
                            fixed = self.fix_layer1(file,ret['map'])
                        # if pnfs layer 4 is correct then map layer 4 can be fixed
                        # fix map layer 4
                        if (4 in ret['bad_map_layers']) and not (4 in ret['bad_pnfs_layers']):
                            fixed = self.fix_layer4(file,ret['pnfs']) 


                if not fixed and ret:

                    if not bad_volume.has_key('bad_files'):
                        bad_volume['bad_files'] = {}
                    if not bad_volume['bad_files'].has_key(file['pnfs_name0']):
                        bad_volume['bad_files'][file['pnfs_name0']] = {}
                    bad_volume['bad_files'][file['pnfs_name0']]['pnfs'] = ret



        # fix bfids
        # 
        if ((('fix_bfid_db' in self.options) or ('fix_all' in self.options)) and
            missing_bfids):
            if may_fix:
                missing_bfids = self.fix_bfid_db(external_label, missing_bfids)
            else:
                print "Database for %s is severely damaged and cannot be fixed"%(external_label,)

        if missing_bfids:
            bad_volume['missing_bfids'] = missing_bfids
        if bad_volume:
            print "BAD VOL",bad_volume
            print "BAD VOL PPRINT"
            pprint.pprint(bad_volume)
            report_file.write('VOLUME %s'%(external_label,))
            self.write_record(bad_volume, report_file)
            ret = 1
        else:
            print "VOLUME %s is good" % (external_label,)
            ret = 0
        return ret

    def check_file_db(self):
        # open report file
        tm = time.localtime(time.time())          # get the local time
        FILE_PREFIX = 'DB_CHECK_REPORT-'
        # form the log file name
        fn = '%s%04d-%02d-%02d' % (FILE_PREFIX, tm[0], tm[1], tm[2])
        report_file = open(fn,'w')
        for external_label in self.volume_list:
            self.check_file_db_for_vol(external_label,report_file)                  
        report_file.close()


def report():
    args, options,listopt = parse_command_line(sys.argv[1:])
    print "ARGS",args
    print "OPTS",options
    if len(args) != 2:
        usage()
        os._exit(1)
    perm = check_permissions(options)
    if not perm:
        print "only enstore is allowed to modify DATA BASES"
        os._exit(1)

    #os._exit(0)
    opdb = DBChecker(args[0], args[1], options)
    #os._exit(0)
    if 'vol' in options:
        for (opt, val) in listopt:
            print "OPT %s VAL %s" % (opt,val)
            if opt == '--vol':
                vol = val
        print "Check volume %s" % (vol,) 
        tm = time.localtime(time.time())          # get the local time
        FILE_PREFIX = '_REPORT-'
        # form the log file name
        fn = '%s%s%04d-%02d-%02d' % (vol, FILE_PREFIX, tm[0], tm[1], tm[2])
        report_file = open(fn,'w')
        ret = opdb.check_file_db_for_vol(vol,report_file)                  
        report_file.close()
        if ret == 0:
            # no errors. Delete report file
            os.unlink(fn)
    else:
        opdb.check_file_db()


if __name__=='__main__':
    report()
