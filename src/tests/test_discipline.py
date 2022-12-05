import unittest
import ast
import discipline
import socket
import configuration_client
import mock
import os
import re
import copy
import threading
import e_errors
import Trace
import hostaddr
class TestRestrictor(unittest.TestCase):

    def setUp(self):
        self.csc = configuration_client.ConfigurationClient()
        this_dir = os.path.dirname(os.path.abspath(__file__))
        fixture_dir = os.path.join(this_dir, 'fixtures')
        init_file = os.path.join(fixture_dir, 'csc.prod.dump')

        with open(init_file,'r') as fd:
            data = fd.read()
        
        self.full_dict = ast.literal_eval(data)
        self.csc.saved_dict = self.full_dict['dump']
        self.res = discipline.Restrictor(self.csc.saved_dict, 'CD-DiskSF3.library_manager')
        host = socket.gethostname()
        ip = socket.gethostbyname(host)
        self.ticket={'lm': {'address': (ip, 7520)}, 'unique_id': '%s-1005321365-0-28872'%(host,), 'infile': '/pnfs/rip6/happy/mam/aci.py',
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
            'wrapper': {'minor': 5, 'inode': 0, 'fullname': '/dev/null', 'size_bytes': 1434, 'rmajor': 0, 'mode': 33268,
                        'pstat': (33204, 71365368, 5L, 1, 6849, 5440, 1434, 1004716362, 1004716362, 1004716329), 'gname': 'hppc',
                        'sanity_size': 65536, 'machine': ('Linux', 'fdmtest.fnal.gov', '2.2.17-14', '#1 Mon Feb 5 18:48:50 EST 2001', 'i686'),
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
       
    def test___init__(self):
        self.assertTrue(isinstance(self.res, discipline.Restrictor))

    def test_read_config(self):
        self.assertEqual(self.res.exists, 1)


    def test_match_found(self):
        #import pdb; pdb.set_trace()
        ret = self.res.match_found(self.ticket)
        print "ret=",ret

    def test_ticket_match(self):
        pass
if __name__ == "__main__":
    unittest.main()
