#!/usr/bin/env python

# $Id$
import sys
import os
import pwd
import getopt
import string
import errno
import e_errors
import db
import pnfs
import bfid_db
import pprint

opts_permitted = ['interactive', 'fix_file_db', 'fix_bfid_db',
                  'fix_all','scan_pnfs', 'fix_pnfs','fix_volmap']
def parse_command_line(command_line):
    try:
        loptions, args =getopt.getopt(sys.argv[1:], [], opts_permitted)
        options = []
        for opt in loptions:
            options.append(opt[0][2:])
    except:
        args = options = []
    return args, options
def check_permissions(options):
    mod_req = 0
    for option in options:
        print "11111111",option
        if string.find(option,'fix') >= 0:
            print "222222222222"
            mod_req = 1
            break
    if mod_req:
        # modification of some DBs required
        # check if modification is permitted for this user
        user_name = pwd.getpwuid(os.getuid())[0]
        print "Login name", user_name
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
    print 'usage: %s [options] DB_DIR JOURNAL_DIR' % (sys.argv[0],)
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
        self.file_dict = db.DbTable("file", self.dbHome, self.jouHome, ['external_label'])
        self.bfiddb=bfid_db.BfidDb(self.dbHome)

        #rec = self.vol_dict['null00']
        #print "VOL_DICT"
        self.volume_list=[]
        self.vol_db.cursor("open")
        key,value=self.vol_db.cursor("first")
        while key:
            #print "KEY %s VALUE %s" % (key, value)
            # we are not interested in the deleted volumes
            if ((value['external_label'] not in self.volume_list) and
                (value['system_inhibit'][0] != e_errors.DELETED)):
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
                print "KEY %s is missing in the file record %s" % (must_key,record)
                missing_keys.append(must_key)
        if missing_keys:
            bad_record['file_db_record'] = record
            bad_record['missing_keys'] = missing_keys
        if not record.has_key('drive'):
            # special case
            # just put there something
            # this may not work because we are running through indices
            # check !!!!!!!!!!!!!!!!
            print "DRIVE MISSING",record
            record['drive'] = 'uknown'
            self.records_to_fix.append(record)
        #print "BAD RECORD",bad_record
        return bad_record
        

    def readlayers(self, pnfs_object):
        l=[]
        l.append(None)
        l.append(pnfs_object.readlayer(1))
        l.append(None)
        l.append(None)
        l.append(pnfs_object.readlayer(4))
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

    def check_file_db(self):
        for external_label in self.volume_list:
            bad_file_db_records = []
            print "VOLUME", external_label
            c = self.file_dict.inx['external_label'].cursor()
            key, pkey = c.set(external_label)
            files=[]
            self.records_to_fix=[]
            while key:
                #print "KEY %s PKEY %s" % (key, pkey)
                record = self.file_dict[pkey]
                # check the consistensy of the file DB record
                bad_record = self.check_file_db_record(self.file_dict[pkey])
                if bad_record:
                    # add volume name
                    bad_record['volume'] = external_label
                    bad_file_db_records.append(bad_record)
                else:
                    if record['deleted'] != 'yes':
                        # we are not interested in deleted files
                        files.append(record) 
                key,pkey = c.nextDup()
            c.close()
            
            #fix records in the file db
            if ((('fix_file_db' in self.options) or ('fix_all' in self.options)) and
                self.records_to_fix):
                print "Fixing file DB for %s records" % (len(self.records_to_fix),)
                records_fixed = 0
                self.fix = 1  # set fix flag
                self.prepare_for_interactive_input()
                for rec in self.records_to_fix:
                    self.fix = self.interactive_input('fix this record?')
                    if self.fix:
                        print "Fixing file DB: %s" % (rec,)
                        self.file_dict[rec['bfid']] = rec
                        records_fixed = records_fixed + 1
                print "Fixed %s records" % (records_fixed,)

                
            ######################
            if bad_file_db_records:
                if not self.bad_volumes.has_key(external_label):
                    self.bad_volumes[external_label] = {}
                self.bad_volumes[external_label]['bad_file_db_records'] =  bad_file_db_records
            
            # check if all bfids exist in the volume bfid DB
            try:
                bfids = self.bfiddb.get_all_bfids(external_label)
            except (IOError,IndexError), detail:
                print "Exception %s" % (str(detail),)
                
            #print "VOL BFIDS",bfids
            missing_bfids = []
            for file in files:
                if file['bfid'] not in bfids:
                  missing_bfids.append(file['bfid'])


            # fix bfids
            # 
            if ((('fix_bfid_db' in self.options) or ('fix_all' in self.options)) and
                missing_bfids):
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
                            self.bfiddb.add_bfid(external_label, bfid)
                        except (IOError,IndexError), detail:
                            print "Exception %s" % (str(detail),)
                            print "ERRNO",errno.errorcode[detail.errno]
                            if detail.errno == errno.ENOENT:
                                try:
                                    print "trying to initialize dbfile for %s"%(external_label,)
                                    self.bfiddb.init_dbfile(external_label)
                                    self.bfiddb.add_bfid(external_label, bfid)
                                except (IOError,IndexError), detail:
                                    print "Exception %s" % (str(detail),)
                                    print "Give up on fixing bfiddb"
                            else:
                                break
                        fixed_bfids.append(bfid)
                        records_fixed = records_fixed + 1
                for bfid in fixed_bfids:
                    missing_bfids.remove(bfid)
                del(fixed_bfids)
                
                print "Fixed %s bfids" % (records_fixed,)
                    
            if missing_bfids:
                if not self.bad_volumes.has_key(external_label):
                    self.bad_volumes[external_label] = {}
                self.bad_volumes[external_label]['missing_bfids'] = missing_bfids


            if 'scan_pnfs' in self.options:
                # check if pnfs files exist, volmap files exist
                for file in files:
                    pinfo = pnfs.Pnfs(file['pnfs_name0'],get_details=0,get_pinfo=0,timeit=0)
                    pl = self.readlayers(pinfo)
                    #print "PL",pl
                    # see if mapfile exists
                    #print "EXISTS",file['pnfs_name0'], pinfo.exists
                    #print "PINFO:volume %s, location_cookie %s, size %s, origff %s, origname %s, mapfile %s" % (pinfo.volume, pinfo.location_cookie, pinfo.size, pinfo.origff, pinfo.origname, pinfo.mapfile)

                    map = pnfs.Pnfs(file['pnfs_mapname'], get_details=0,get_pinfo=0,timeit=0)
                    ml = self.readlayers(map)

                    # compare layers
                    for i in range(0,len(pl)):
                        if pl[i] != ml[i]:
                            print "layer %s mismatch for %s" % (i, file['pnfs_name0'],)
                            print "Layer",i
                            print "PL",pl[i]
                            print "ML",ml[i]

                    #print "ML",ml
                    #print "EXISTS",file['pnfs_mapname'], pinfo.exists
                    #print "MAP:volume %s, location_cookie %s, size %s, origff %s, origname %s, mapfile %s" % (map.volume, map.location_cookie, map.size, map.origff, map.origname, map.mapfile)
                    """
                    # check if file bfid is in the volume db
                    match = 1
                    if pinfo.bit_file_id not in bfids:
                        print "bit file id %s is missing in the volume database" % (pinfo.bit_file_id)
                        match = 0
                    # pinfo and volume map pinfo must match
                    if pinfo.volume != map.volume:
                        print "volume name %s and volmap volume name %s do not match" % (pinfo.volume, map.volume)
                        match = 0
                    if pinfo.location_cookie != map.location_cookie:
                        print "loc. cookie %s and volmap loc. cookie %s do not match" % (pinfo.location_cookie, map.location_cookie)

                        match = 0
                    if pinfo.size != map.size:
                        print "file size %s and volmap file size %s do not match" % (pinfo.size, map.size)

                        match = 0
                    if pinfo.origff != map.origff:
                        print "file family %s and volmap file family %s do not match" % (pinfo.origff, map.origff)

                        match = 0
                    if pinfo.origname != map.origname:
                        print "file name %s and volmap file name %s do not match" % (pinfo.origname, map.origname)

                        match = 0
                    if pinfo.mapfile != map.mapfile:
                        print "map file %s and volmap map file %s do not match" % (pinfo.mapfile, map.mapfile)

                        match = 0
                    if not match:
                        # try to fix
                        print "trying to fix",pinfo.origname
                        try:
                            p.set_xreference(done_ticket["fc"]["external_label"],
                                             done_ticket["fc"]["location_cookie"],
                                             done_ticket["fc"]["size"],
                                             drive)
                        except:
                            exc,msg,tb=sys.exc_info()

                    """

        if self.bad_volumes:
            print "BAD VOLUMES"
            pprint.pprint(self.bad_volumes)
        else:
            print "NO BAD VOLUMES"


def report():
    args, options = parse_command_line(sys.argv[1:])
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
    opdb.check_file_db()


if __name__=='__main__':
    report()
