#!/usr/bin/env python

###############################################################################
# $Id$
#
# system imports
import sys
import os
import stat
import errno
import string
import re
import traceback
import e_errors
import Trace

MAX_REG_PRIORITY = 100

class PriSelector:

    def read_config(self):
        exists = getattr(self, 'exists', -1)
        if exists == 0: 
            msg = (e_errors.DOESNOTEXIST,"%s " % (self.configfile,))
            return msg
        try:
           mtime = os.stat(self.configfile)[stat.ST_MTIME]
        except OSError, detail:
            msg = (e_errors.DOESNOTEXIST,"%s %s" % (self.configfile, detail))
            Trace.log(e_errors.ERROR, msg[1])
            
            return msg
        if self.mtime == mtime:
            return (e_errors.OK, None)
        self.mtime = mtime
        f = open(self.configfile,'r')
        code = string.join(f.readlines(),'')
        Trace.trace(9, "read_config: loading priority configuration from %s"%
                    (self.configfile,))
        prioritydict={};
        del prioritydict # Lint hack, otherwise lint can't see where prioritydict is defined.
        try:
            exec(code)
            ##I would like to do this in a restricted namespace, but
            ##the dict uses modules like e_errors, which it does not import
        except:
            exc,msg,tb = sys.exc_info()
            fmt =  traceback.format_exception(exc,msg,tb)[2:]
            ##report the name of the config file in the traceback instead of "<string>"
            fmt[0] = string.replace(fmt[0], "<string>", self.configfile)
            msg = string.join(fmt, "")
            Trace.log(e_errors.ERROR,msg)
            return (e_errors.UNKNOWN, None)
        # ok, we read entire file - now set it to real dictionary
        self.prioritydict = prioritydict
        self.pri_keys = prioritydict.keys()
        self.pri_keys.sort()
        self.pri_keys.reverse()

        return (e_errors.OK, None)

    def __init__(self, configfile, max_reg_pri=MAX_REG_PRIORITY):
        self.max_reg_pri = max_reg_pri
        self.mtime = 0
        self.configfile = configfile
        rc = self.read_config()
        if e_errors.OK not in rc:
            self.exists = 0
            return
        self.exists = 1
        
        


    def ticket_match(self, ticket, pri_key, conf_key):
        pattern = "^%s" % (self.prioritydict[pri_key][conf_key],)
        item='%s'%(ticket.get(conf_key, 'Unknown'),)
        if re.search(pattern, item): return 1
        else: return 0
        

    def priority(self, ticket):
        if not self.exists:  # no priority configuration info
            return ticket['encp']['basepri'], ticket['encp']['adminpri']
            
        self.read_config()
        # make a "flat" copy of ticket
        flat_ticket={}
        flat_ticket.update(ticket)
        # before making a ticket remove ['vc']['wrapper'] as it will interfere
        # with 'wrapper' (see ticket structure)
        if flat_ticket['vc'].has_key('wrapper'): del(flat_ticket['vc']['wrapper'])
        for key in flat_ticket.keys():
            if type(flat_ticket[key]) is type({}):
                for k in flat_ticket[key].keys():
                    if k == 'machine': flat_ticket['host'] = flat_ticket[key][k][1]
                    else: flat_ticket[k] = flat_ticket[key][k]
                del(flat_ticket[key])

        cur_pri = flat_ticket['basepri']
        cur_adm_pri = flat_ticket.get('adminpri',-1) 
        for pri_key in self.pri_keys:
            conf_keys = self.prioritydict[pri_key].keys()
            nkeys = len(conf_keys)
            nmatches = 0
            for conf_key in conf_keys:
                # try to match a ticket
                if not self.ticket_match(flat_ticket, pri_key, conf_key): break
                nmatches = nmatches + 1
            if nmatches == nkeys:
                if (pri_key < self.max_reg_pri):
                    if pri_key+cur_pri < self.max_reg_pri:
                        cur_pri = pri_key+cur_pri
                else:
                    cur_pri = pri_key+cur_pri
                break
        if cur_pri >= self.max_reg_pri:
            cur_adm_pri = cur_pri / self.max_reg_pri + cur_adm_pri
            cur_pri = cur_pri % self.max_reg_pri
        return cur_pri, cur_adm_pri

    
if __name__ == "__main__":
    ps = PriSelector('pri_conf.py')
    ps.read_config()
    ticket={'unique_id': 'happy.fnal.gov-959786955.526691-14962', 'at_the_top': 2,
            'encp': {'delayed_dismount': 1, 'basepri': 1, 'adminpri': -1, 'curpri': 1,
                     'agetime': 0, 'delpri': 0}, 'fc': {'address': ('131.225.84.122', 7501),
                                                        'size': 5158L,
                                                        'external_label': 'null02'},
            'vc': {'library': 'happynull', 'file_family_width':2,
                   'volume_family': 'D0.alex.null', 'address': ('131.225.84.122', 7502),
                   'wrapper': 'null', 'file_family': 'alex.null',
                   'at_mover': ('unmounted', 'none'), 'storage_group': 'D0'},
            'times': {'t0': 959786955.184, 'in_queue': 2.62227797508,
                      'job_queued': 959786955.542, 'lm_dequeued': 959786958.164},
            'wrapper': {'minor': 0, 'inode': 0,
                        'fullname': '/home/moibenko/enstore2/src/alarm.py',
                        'size_bytes': 5158L, 'gname': 'hppc', 'mode': 33261,
                        'gid': 5440, 'mtime': 959786955, 'sanity_size': 65536,
                        'machine': ('Linux',
                                    'happy.fnal.gov',
                                    '2.2.15',
                                    '#4 SMP Tue May 30 13:35:20 CDT 2000', 'i686'),
                        'uname': 'moibenko',
                        'pstat': (16893, 70397816L,3, 1, 6849, 5440, 512L, 959704674, 959704674, 959704674),
                        'uid': 6849, 'pnfsFilename': '/pnfs/rip6/happy/NULL/d2/alarm.py',
                        'rminor': 0, 'rmajor': 0, 'type': 'null', 'major': 0},
            'lm': {'address': ('131.225.84.122', 7503)}, 'callback_addr': ('131.225.84.122', 7600),
            'work': 'write_to_hsm', 'retry': 2, 'status': ('ok', None)}
    pri, adm_pri = ps.priority(ticket)
    print "END"
    print "Priority:",pri, adm_pri
    
