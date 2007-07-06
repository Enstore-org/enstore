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
#import pcre deprecated
import copy
import traceback
import pprint


# enstore imports
import e_errors
import Trace
import hostaddr

class Restrictor:

    def read_config(self):
        Trace.log(e_errors.INFO, "(Re)loading discipline")
        self.exists = 0
        disc_dict=self.csc.get('discipline',{})
        if disc_dict['status'][0] == e_errors.OK:
            dict =  disc_dict.get(self.library_manager, {})
        else:
            dict = {}
        if dict:
            self.exists = 1
        self.storage_groups = dict
        return (e_errors.OK, None)

    def __init__(self, csc, library_manager):
        self.csc = csc
        self.library_manager = library_manager
        self.read_config()


    def ticket_match(self, dict, ticket, pri_key, conf_key):
        pattern = "^%s" % (dict[pri_key]['keys'][conf_key],)
        item='%s'%(ticket.get(conf_key, 'Unknown'),)
        try:
            if re.search(pattern, item): return 1
            else: return 0
        except:
            Trace.log(e_errors.ERROR,"parse errorr")
            Trace.handle_error()
            return 0
        #pcre is deprecated
        #except pcre.error, detail:
        #    Trace.log(e_errors.ERROR,"parse errorr %s" % (detail, ))
        #    return 0
        

    def match_found(self, ticket):
        #self.read_config()
        if not self.exists:  # no discipline configuration info
            return 0, None, None, None
        # make a "flat" copy of ticket
        # use deepcopy 
        flat_ticket=copy.deepcopy(ticket)
        if flat_ticket['vc'].has_key('wrapper'): del(flat_ticket['vc']['wrapper'])
        callback = flat_ticket.get('callback_addr', None)
        if callback:
            flat_ticket['host'] = hostaddr.address_to_name(callback[0])
        else:
            flat_ticket['host'] = None 
        for key in flat_ticket.keys():
            if type(flat_ticket[key]) is type({}):
                for k in flat_ticket[key].keys():
                    if k == 'machine':
                        if flat_ticket['host'] == None:
                            flat_ticket['host'] = flat_ticket[key][k][1]
                    else: flat_ticket[k] = flat_ticket[key][k]
                del(flat_ticket[key])

        match = 0, None, None, None
        sg = flat_ticket.get('storage_group', None)
        if not sg:
            return 0, None, None, None
        sg_dict = self.storage_groups.get(sg, None)
        if not sg_dict:
           return match 
        for key in sg_dict.keys():
            conf_keys = sg_dict[key]['keys'].keys()
            nkeys = len(conf_keys)
            nmatches = 0
            for conf_key in conf_keys:
                # try to match a ticket
                if not self.ticket_match(sg_dict, flat_ticket, key, conf_key): break
                nmatches = nmatches + 1
            if nmatches == nkeys:
                # use deep copy to make sure that the original set of arguments is returned
                match = 1,sg_dict[key]['function'],copy.deepcopy(sg_dict[key]['args']),sg_dict[key]['action'] 
                break
        return match

    
if __name__ == "__main__":
    import configuration_client
    def_addr = (os.environ['ENSTORE_CONFIG_HOST'],
                string.atoi(os.environ['ENSTORE_CONFIG_PORT']))
    csc = configuration_client.ConfigurationClient( def_addr )
    r = Restrictor(csc,'mezsilo.library_manager')
    #ps.read_config()
    ticket={'lm': {'address': ('131.225.84.122', 7520)}, 'unique_id': 'happy.fnal.gov-1005321365-0-28872', 'infile': '/pnfs/rip6/happy/mam/aci.py', 'bfid': 'HAMS100471636100000', 'mover': 'MAM01.mover', 'at_the_top': 3, 'client_crc': 1, 'encp_daq': None, 'encp': {'delayed_dismount': None, 'basepri': 1, 'adminpri': -1, 'curpri': 1, 'agetime': 0, 'delpri':0}, 'fc': {'size': 1434L, 'sanity_cookie': (1434L, 657638438L), 'bfid': 'HAMS100471636100000', 'location_cookie': '0000_000000000_0000001', 'address': ('131.225.84.122', 7501), 'pnfsid': '00040000000000000040F2F8', 'pnfs_mapname': '/pnfs/rip6/volmap/alex/MM0001/0000_000000000_0000001', 'drive': 'happy:/dev/rmt/tps0d4n:0060112307', 'external_label': 'MM0001', 'deleted': 'no', 'pnfs_name0': '/pnfs/rip6/happy/mam/aci.py', 'pnfsvid': '00040000000000000040F360', 'complete_crc': 657638438L, 'status': ('ok', None)}, 'file_size': 1434, 'outfile': '/dev/null', 'volume': 'MM0001', 'times': {'t0': 1005321364.951048, 'in_queue': 14.586493015289307, 'job_queued': 1005321365.7764519, 'lm_dequeued': 1005321380.363162}, 'version': 'v2_14  CVS $Revision$ ', 'retry': 0, 'work': 'read_from_hsm', 'callback_addr': ('131.225.84.122', 1463), 'wrapper': {'minor': 5, 'inode': 0, 'fullname': '/dev/null', 'size_bytes': 1434, 'rmajor': 0, 'mode': 33268, 'pstat': (33204, 71365368, 5L, 1, 6849, 5440, 1434, 1004716362, 1004716362, 1004716329), 'gname': 'hppc', 'sanity_size': 65536, 'machine': ('Linux', 'd0mino-g1.fnal.gov', '2.2.17-14', '#1 Mon Feb 5 18:48:50 EST 2001', 'i686'), 'uname': 'moibenko', 'pnfsFilename': '/pnfs/rip6/happy/mam/aci.py', 'uid': 6849, 'gid': 5440, 'rminor': 0, 'major': 0}, 'vc': {'first_access': 1004716170.54972, 'sum_rd_err': 0, 'last_access': 1004741744.274856, 'media_type': '8MM', 'capacity_bytes': 5368709120L, 'declared': 1004474612.7774431, 'remaining_bytes': 20105625600L, 'wrapper': 'cpio_odc', 'external_label': 'MM0001', 'system_inhibit': ['none', 'none'], 'user_inhibit': ['none', 'none'], 'current_location': '0000_000000000_0000001', 'sum_rd_access': 7, 'volume_family': 'D0.alex.cpio_odc', 'address': ('131.225.84.122', 7502), 'file_family': 'alex', 'sum_wr_access': 2, 'library': 'mam', 'sum_wr_err': 1, 'non_del_files': 1, 'blocksize': 131072, 'eod_cookie': '0000_000000000_0000002', 'storage_group': 'D0', 'status': ('ok', None)}, 'status': ('ok', None)}
    ret = r.match_found(ticket)
    print "END"
    print "Access enabled=:",ret
    
