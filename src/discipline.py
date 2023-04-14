#!/usr/bin/env python

###############################################################################
# $Id$
#
# system imports
import os
import re
import copy
import threading


# enstore imports
import e_errors
import Trace
import hostaddr

# Important change in discipline configuration:
# it does not have storage groups anymore
# old style discipline configuration:
#    'LTO3.library_manager':{'gcc':{1:{'keys':{'host':"gccenmvr1a"},
#				      'function':'restrict_host_access',
#				      'args':['auger','gccenmvr1a',3],
#				      'action':'ignore'},
#				   2:{'keys':{'host':"gccenmvr2a"},
#				      'function':'restrict_host_access',
#				      'args':['auger','gccenmvr2a',2],
#				      'action':'ignore'},
#				   },
#			    },
#
# new style discipline configuration
#'LTO3.library_manager':{1: {host:"gccenmvr1a",
#                            'function':'restrict_host_access',
#                            'args':['gccenmvr1a',3],
#                            'action':'ignore'},
#			2:{'host':"gccenmvr2a",
#                           'function':'restrict_host_access',
#                           'args':['gccenmvr2a',2],
#                           'action':'ignore'},
#                        }
# This allows for the faster expression match
# This also allows to set a matching rule for all possible host names:
# "[A-Za-z]+".
# If doing so this should be the last entry in the discipline configuration

class Restrictor:

    def read_config(self):
        Trace.log(e_errors.INFO, "(Re)loading discipline")
        self.exists = 0
        disc_dict=self.csc.get('discipline',{})
        if disc_dict['status'][0] == e_errors.OK:
            ldict =  disc_dict.get(self.library_manager, {})
        else:
            ldict = {}
        if dict:
            self.exists = 1
        self.discipline_dict = ldict
        return (e_errors.OK, None)

    def __init__(self, csc, library_manager):
        self.csc = csc
        self.library_manager = library_manager
        self._lock = threading.Lock()
        self.read_config()


    def ticket_match(self, key, item):
        pattern = "^%s" % (self.discipline_dict[key]['host'],)
        try:
            if re.search(pattern, item):
                return 1
            else:
                return 0
        except:
            Trace.log(e_errors.ERROR,"parse errorr")
            Trace.handle_error()
            return 0

    def match_found(self, ticket):
        # returns a tuple:
        # 1 if match was found, 0 - if not
        # function name to run when match was found, None othrewise
        # list of function arguments, None if fuction was not found
        # what to do with request ("ignore", "reject"), None if fuction was not found
        
        # default match settings
        match = 0, None, None, None
        failed = False
        self._lock.acquire()
        callback = ticket.get('callback_addr', None)
        try:
            if callback:
                host = hostaddr.address_to_name(callback[0])
            else:
                host = ticket['wrapper']['machine'][1]
        except:
            failed = True 
        
        self._lock.release()
        if failed:
            return match

        if not self.exists:  # no discipline configuration info
            return match

        for key in self.discipline_dict.keys():
            # try to match a ticket
            if self.ticket_match(key, host) == 1:
                # use deep copy to make sure that the original set of arguments is returned
                match = 1, self.discipline_dict[key]['function'], copy.deepcopy(self.discipline_dict[key]['args']),self.discipline_dict[key]['action']
                break

        return match

    
if __name__ == "__main__":
    import socket
    import configuration_client
    def_addr = (os.environ['ENSTORE_CONFIG_HOST'],
                int(os.environ['ENSTORE_CONFIG_PORT']))
    csc = configuration_client.ConfigurationClient( def_addr )
    host = socket.gethostname()
    ip = socket.gethostbyname(host)
    r = Restrictor(csc,'null1.library_manager')
    #ps.read_config()
    ticket={'lm': {'address': (ip, 7520)}, 'unique_id': '%s-1005321365-0-28872'%(host,), 'infile': '/pnfs/rip6/happy/mam/aci.py',
            'bfid': 'HAMS100471636100000', 'mover': 'MAM01.mover', 'at_the_top': 3, 'client_crc': 1, 'encp_daq': None,
            'encp': {'delayed_dismount': None, 'basepri': 1, 'adminpri': -1, 'curpri': 1, 'agetime': 0, 'delpri':0},
            'fc': {'size': 1434L, 'sanity_cookie': (1434L, 657638438L), 'bfid': 'HAMS100471636100000', 'location_cookie':
                   '0000_000000000_0000001', 'address': ('131.225.84.122', 7501), 'pnfsid': '00040000000000000040F2F8',
                   'pnfs_mapname': '/pnfs/rip6/volmap/alex/MM0001/0000_000000000_0000001', 'drive':
                   'happy:/dev/rmt/tps0d4n:0060112307', 'external_label': 'MM0001', 'deleted': 'no', 'pnfs_name0':
                   '/pnfs/rip6/happy/mam/aci.py', 'pnfsvid': '00040000000000000040F360', 'complete_crc': 657638438L,
                   'status': ('ok', None)},
            'file_size': 1434, 'outfile': '/dev/null', 'volume': 'MM0001',
            'times': {'t0': 1005321364.951048,'in_queue': 14.586493015289307, 'job_queued': 1005321365.7764519, 'lm_dequeued': 1005321380.363162},
            'version': 'v2_14  CVS $Revision$ ', 'retry': 0, 'work': 'read_from_hsm', 'callback_addr': ('131.225.13.132', 1463),
            'wrapper': {'minor': 5, 'inode': 0, 'fullname': '/dev/null', 'size_bytes': 1434, 'rmajor': 0, 'mode': 33268,
                        'pstat': (33204, 71365368, 5L, 1, 6849, 5440, 1434, 1004716362, 1004716362, 1004716329), 'gname': 'hppc',
                        'sanity_size': 65536, 'machine': ('Linux', 'gccensrv2.fnal.gov', '2.2.17-14', '#1 Mon Feb 5 18:48:50 EST 2001', 'i686'),
                        'uname': 'moibenko', 'pnfsFilename': '/pnfs/rip6/happy/mam/aci.py', 'uid': 6849, 'gid': 5440, 'rminor': 0, 'major': 0},
            'vc': {'first_access': 1004716170.54972, 'sum_rd_err': 0, 'last_access': 1004741744.274856, 'media_type': '8MM',
                   'capacity_bytes': 5368709120L, 'declared': 1004474612.7774431, 'remaining_bytes': 20105625600L,
                   'wrapper': 'cpio_odc', 'external_label': 'MM0001', 'system_inhibit': ['none', 'none'],
                   'user_inhibit': ['none', 'none'], 'current_location': '0000_000000000_0000001', 'sum_rd_access': 7,
                   'volume_family': 'D0.alex.cpio_odc', 'address': ('131.225.84.122', 7502), 'file_family': 'alex',
                   'sum_wr_access': 2, 'library': 'mam', 'sum_wr_err': 1, 'non_del_files': 1, 'blocksize': 131072,
                   'eod_cookie': '0000_000000000_0000002', 'storage_group': 'D0', 'status': ('ok', None)},
            'status': ('ok', None)
            }
    ret = r.match_found(ticket)
    print "END"
    print "Access enabled=:",ret
    
