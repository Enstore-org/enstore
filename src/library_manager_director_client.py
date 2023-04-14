#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import sys
import string
import copy

#enstore imports
import generic_client
import configuration_client
import enstore_functions2
import e_errors

MY_NAME = ".LMDC"
RCV_TIMEOUT = 20
RCV_TRIES = 3

# Note that library manager director is not a regular enstore server.
# It is not guarateed that all methods pertinent to generic enstore server
# are implemented in library manager director.
# This means that not all generic client methods will work.

class LibraryManagerDirectorClient(generic_client.GenericClient) :
    def __init__(self, csc, server_name="", flags=0, logc=None, alarmc=None,
                 rcv_timeout = RCV_TIMEOUT, rcv_tries = RCV_TRIES,
                 server_address = None):
        # csc - configuration server client
        # name - name of the Library Manager Director in configuration dictionary 
        self.name = server_name  
        self.library_manager = self.name
        self.conf = csc.get(server_name)
        if server_address == None:
            server_address = (self.conf['hostip'], self.conf['udp_port'])

        self.log_name = "C_"+string.upper(string.replace(server_name,
                                                         ".LMD",
                                                         MY_NAME))
        
        generic_client.GenericClient.__init__(self, csc, self.log_name,
                                              flags = flags, logc = logc,
                                              alarmc = alarmc,
                                              rcv_timeout = rcv_timeout,
                                              rcv_tries = rcv_tries,
                                              server_name = server_name,
                                              server_address = server_address)
        self.send_to = rcv_timeout
        self.send_tries = rcv_tries

    def get_library_manager(self, ticket) :
        if ticket['work'] != "write_to_hsm":
           ticket['status'] = (e_errors.OK, None)
           return ticket
        # save original work
        saved_work  = ticket['work']
        # The new work is "get_library_manager".
        # This is needed because request can be sent
        # using udp_proxy server or directly to lm_director,
        # depending on the configuration.
        ticket['work'] = 'get_library_manager'
        ticket = self.send(ticket)
        ticket['work'] = saved_work
        return ticket

class LibraryManagerDirectorClientInterface(generic_client.GenericClientInterface) :
    def __init__(self, args=sys.argv, user_mode=1) :
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options,)

    parameters = ["<LibraryManager>.library_manager"]
        
    def parse_options(self):
        generic_client.GenericClientInterface.parse_options(self)

        if (getattr(self, "help", 0) or getattr(self, "usage", 0)):
            self.print_help()
        elif len(self.argv) <= 1: #if only "enstore library" is specified.
            self.print_help()
        elif len(self.args) < 1: #if a valid switch doesn't have the LM.
            self.print_usage("expected library director parameter")
        else:
            try:
                self.name = self.args[0]
                del self.args[0]
            except KeyError:
                self.name = ""
                
        self.name = self.complete_server_name(self.name, "LMD")

# unit test
def unit_test(intf):
    # intf - LibraryManagerDirectorClientInterface instance
    if len(intf.argv) != 2:
        print "Usage: %s <LibraryManager.library_manager>" % (intf.argv[0],)
        return
    ticket={'lm': {'address': ("thehost", 7520)}, 'unique_id': '%s-1005321365-0-28872'%("thehost",), 'infile': '/pnfs/rip6/happy/mam/aci.py',
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
            'version': 'v2_14  CVS $Revision$ ', 'retry': 0, 'work': 'read_from_hsm',
            #'callback_addr': ('131.225.204.196', 1463),
            'callback_addr': ('131.225.207.112', 1463),
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
    
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,
                                                    config_port))
    csc.dump_and_save()
    lm = csc.get(intf.argv[1], None)
    if lm:
        lmdname = lm.get('use_LMD', None) # this is the keyword, that must be defined in LM configuration to refer to library manager director
        if lmdname:
            print "LMD name:", lmdname
            lmdc = LibraryManagerDirectorClient(csc, lmdname)
        else:
            print "No LMD is defined for %s" % (lmdname,)
            sys.exit(1)
    else:
        print "%s is not in configuration" % (lmdname,)
        sys.exit(1)

    print "CLIENT ADDRESS",lmdc.server_address  
    t = lmdc.get_library_manager(ticket)

    print t
    print "REPLY", t['vc']
    print "SENT", ticket['vc']

    for key in t.keys():
        print "'%s': %s" % (key, t[key])

    print "%s" % (t['fc']['location_cookie'],)   
    

if __name__ == "__main__":
    intf = LibraryManagerDirectorClientInterface(user_mode=0)
    unit_test(intf)
    
 
    
            
 
