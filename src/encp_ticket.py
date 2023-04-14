# $Id$

def write_request_ok(ticket):
    ticket_keys = {'callback_addr': ('', 0),
                   'encp':{},
                   'fc':{},
                   'file_size':0L,
                   'infile':'',
                   'outfile':'',
                   'retry':0,
                   'times':{},
                   'unique_id':'',
                   'vc':{},
                   'version':'',
                   'work':'write_to_hsm',
                   'wrapper':{},
                   }
                   
    encp_keys = {'adminpri': -1,
                 'agetime': 0,
                 'basepri': 1,
                 'delpri': 0}
    
    fc_keys = {'address': ('', 0),
              }

    vc_keys = {'address': ('', 0),
               #'file_family': '',
               'file_family_width': 0,
               'library': '',
               'storage_group': '',
               }

    fsize_type = 0L
    # hack as in read_request_ok
    if type(ticket['file_size']) == type(0):  
        fsize_type = 0
    elif type(ticket['file_size']) == type(None):
        fsize_type = None

    inode_type = 0L
    if type(ticket['wrapper']['inode']) == type(0):
        inode_type = 0
    elif type(ticket['wrapper']['inode']) == type(None):
        inode_type = None

    wrapper_keys = {'fullname': '',
                    'gid': 0,
                    'gname': '',
                    'inode': inode_type,
                    'machine': ('','','','',''),
                    'major': 0,
                    'minor': 0,
                    'mode': 0,
                    'mtime': 0,
                    'pnfsFilename': '',
                    'sanity_size': 0,
                    'size_bytes': fsize_type,
                    'type': '',
                    'uid': 0,
                    'uname': ''}
    
    for key in ticket_keys:
        if not ticket.has_key(key):
            return key
    
    for key in encp_keys:
        if not ticket['encp'].has_key(key):
            return key
        
    for key in fc_keys:
        if not ticket['fc'].has_key(key):
            return key
        
    for key in vc_keys:
        if not ticket['vc'].has_key(key):
            return key

    for key in wrapper_keys:
        if not ticket['wrapper'].has_key(key):
            return key
        else:
            if type(ticket['wrapper'][key]) != type( wrapper_keys[key]):
                #if key == 'inode' and (type(ticket['wrapper'][key]) == type(0) or type(ticket['wrapper'][key]) == type(0L)):
                #    continue
                return key
        
    return None

def read_request_ok(ticket):
    ticket_keys = {#'callback_addr': ('', 0),
                   'encp':{},
                   'fc':{},
                   'file_size':0L,
                   'infile':'',
                   #'lm':{},
                   'outfile':'',
                   #'retry':0,
                   'times':{},
                   'unique_id':'',
                   'vc':{},
                   'version':'',
                   'work':'read_from_hsm',
                   'wrapper':{},
                   }
                   
    encp_keys = {'adminpri': -1,
                 'agetime': 0,
                 'basepri': 1,
                 'delpri': 0}
    
    fc_keys = {'address': ('', 0),
               'bfid':'',
               'complete_crc':0L,
               'deleted':'',
               'external_label': '',
               'location_cookie': '',
               'pnfs_name0': '',
               'pnfsid': '',
               'sanity_cookie': (0L, 0L),
               'size': 0L}

    #lm_keys = {'address': ('', 0)}

    vc_keys = {'address': ('', 0),
               'blocksize': 0,
               'capacity_bytes': 0L,
               'external_label': '',
               'file_family': '',
               'library': '',
               'media_type': '',
               'non_del_files': 0,
               'remaining_bytes': 0L,
               'storage_group': '',
               'sum_mounts': 0,
               'sum_rd_access': 0,
               'sum_rd_err': 0,
               'sum_wr_access': 0,
               'sum_wr_err': 0,
               'system_inhibit': ['', ''],
               'user_inhibit': ['', ''],
               'volume_family': '',
               }

    fsize_type = 0L
    # hack to make get happy, as it gives None for the file size
    if type(ticket['file_size']) == type(0):  
        fsize_type = 0
    elif type(ticket['file_size']) == type(None):
        fsize_type = None

    inode_type = 0L
    if type(ticket['wrapper']['inode']) == type(0):
        inode_type = 0
    elif type(ticket['wrapper']['inode']) == type(None):
        inode_type = None

    wrapper_keys = {'fullname': '',
                    'gid': 0,
                    'gname': '',
                    'inode': inode_type,
                    'machine': ('','','','',''),
                    'major': 0,
                    'minor': 0,
                    'mode': 0,
                    #'mtime': 0,
                    'pnfsFilename': '',
                    'sanity_size': 0,
                    'size_bytes': fsize_type,
                    #'type': '',
                    'uid': 0,
                    'uname': ''}
    
    for key in ticket_keys:
        if not ticket.has_key(key):
            return key
        
    for key in encp_keys:
        if not ticket['encp'].has_key(key):
            return key
        
    for key in fc_keys:
        if not ticket['fc'].has_key(key):
            return key
        
    #for key in lm_keys:
    #    if not ticket['lm'].has_key(key):
    #        return key
    
    for key in vc_keys:
        if not ticket['vc'].has_key(key):
            return key
        
    for key in wrapper_keys:
        if not ticket['wrapper'].has_key(key):
            return key
        else:
            if type(ticket['wrapper'][key]) != type(wrapper_keys[key]):
                #if key == 'inode' and (type(ticket['wrapper'][key]) == type(0) or type(ticket['wrapper'][key]) == type(0L)):
                #    continue
                return key
        
    return None

    
if __name__ == "__main__":
    
    w_ticket = {'status': ('ok', None), 'routing_callback_addr': ('131.225.202.12', 32970),
              'vc': {'file_family': 'lqcd', 'library': 'CD-9940B', 'address': ('131.225.13.59', 7502),
                     'volume_family': 'lqcd.lqcd.cpio_odc','file_family_width': 1,
                     'external_label': 'VO5154', 'storage_group': 'lqcd', 'wrapper': 'cpio_odc'},
              'callback_addr': ('131.225.202.12', 54874), 'file_size': 36864036L,
              'lm': {'address': ('131.225.13.4', 7522)},
              'unique_id': 'dellquad2.fnal.gov-1075133530-841-0',
              'fc': {'size': 36864036L, 'external_label': 'VO5154', 'address': ('131.225.13.59', 7501)},
              'route_selection': 0,
              'outfile': '/pnfs/lqcd/FNAL/l2064f21b679m020m050/m0.012-t32/qf_stag_d_d_m0.0120_nt32_501032',
              'version': 'v2_20  CVS $Revision$ <frozen>',
              'encp':{'agetime': 0, 'curpri': 1, 'adminpri': -1, 'delpri': 0, 'basepri': 1,
                      'delayed_dismount': None}, 'retry': 0, 'encp_daq': None, 'client_crc': 1,
              'at_the_top': 6,
              'mover': {'status': ('ok', None), 'check_written_file': 100, 'max_buffer': 1363148800L,
                        'vendor_id': 'STK', 'max_failures': 5, 'library': 'CD-9940B.library_manager',
                        'local_mover': 0, 'host': 'stkenmvr16a', 'max_consecutive_failures': 3,
                        'device': 'stkenmvr16a:/dev/rmt/tps2d0n', 'norestart': 'INQ',
                        'driver': 'FTTDriver', 'port': 7578, 'compression': 0, 'send_stats': 1,
                        'mc_device': '0,0,10,18', 'name': '9940B16.mover',
                        'statistics_path': '/tmp/enstore/enstore/DBT16MV.stat',
                        'mover_address': ('131.225.13.28', 7578), 'mount_delay': 15,
                        'serial_num': '4790000143', 'callback_addr': ('131.225.13.28', 46791),
                        'data_ip': 'stkenmvr16a', 'logname': 'DBT16MV',
                        'media_changer': 'stk.media_changer', 'max_rate': 28311552.0,
                        'hostip': '131.225.13.28', 'do_cleaning': 'No','update_interval': 5,
                        'product_id': 'T9940B', 'syslog_entry': 'st[0-9]'},
              'work':'write_to_hsm',
              'infile': '/data/raid3/l2064f21b679m020m050/m0.012-t32/qf_stag_d_d_m0.0120_nt32_501032',
              'wrapper': {'major': 0, 'rminor': 0,
                          'pnfsFilename': '/pnfs/lqcd/FNAL/l2064f21b679m020m050/m0.012-t32/qf_stag_d_d_m0.0120_nt32_501032',
                          'uid': 2937, 'uname': 'simone', 'gname': 'g038', 'type': 'cpio_odc', 'gid':1540,
                          'mtime': 1075133530, 'machine': ('Linux', 'lqcd.fnal.gov', '2.4.19-perfctr',
                                                           '#4 SMP Fri May 30 12:21:54 CDT 2003', 'i686'),
                          'sanity_size': 65536, 'pstat': (16877, 145747256, 9L, 1, 2937, 1540, 512,
                                                          1075133530, 1075133530, 1075123605),
                          'mode': 32782, 'rmajor': 0, 'size_bytes': 36864036L,
                          'fullname': '/data/raid3/l2064f21b679m020m050/m0.012-t32/qf_stag_d_d_m0.0120_nt32_501032',
                          'inode': None, 'minor': 0},
              'times': {'lm_dequeued': 1075133535.3374989,'encp_start_time': 1075133530.6074851,
                        'job_queued': 1075133531.4478171,
                        't0': 1075133530, 'in_queue': 3.8894848823547363}}

    r_ticket = {'status': ('ok', None),
                'vc': {'status': ('ok', None), 'comment': '', 'storage_group': 'selex',
                       'si_time': [993676719.0, 997734795.0], 'capacity_bytes': 20401094656L,
                       'blocksize': 131072,'non_del_files': 99,
                       'current_location': '0000_000000000_0000097', 'declared': 993676719.0,
                       'library': 'eagle', 'sum_wr_err': 0, 'sum_wr_access': 99, 'file_family': 'selex',
                       'eod_cookie': '0000_000000000_0000100', 'address': ('131.225.13.59', 7502),
                       'volume_family': 'selex.selex.cpio_odc','user_inhibit': ['none', 'none'],
                       'system_inhibit': ['none', 'full'], 'external_label': 'VO1533',
                       'wrapper': 'cpio_odc', 'remaining_bytes': 364952576L, 'sum_mounts': 231,
                       'sum_rd_access': 713,'media_type': '9840', 'last_access': 1075146934.0,
                       'sum_rd_err': 0, 'first_access': 997729056.0},
                'route_selection': 0, 'volume': 'VO1533',
                'outfile': '/spool03/scratch/jurgen/strip-123/a.tmp',
                'fc':{'status': ('ok', None), 'complete_crc': 3020422051L,
                      'pnfs_name0': '/pnfs/selex/ph001/out4/09/ph001_charm_run009784_001.out4',
                      'deleted': 'no', 'external_label': 'VO1533',
                      'drive': 'stkenmvr3a:/dev/rmt/tps3d1n:3310000364',
                      'location_cookie': '0000_000000000_0000097', 'pnfsid': '000B000000000000000CCDA8',
                      'address': ('131.225.13.59', 7501), 'bfid': '99773456400000',
                      'sanity_cookie': (65536L, 2090556793L), 'size': 80907408L},
                'encp': {'agetime': 0, 'curpri': 1, 'adminpri': -1, 'delpri': 0, 'basepri': 1,
                         'delayed_dismount': None},
                'file_size': 80907408L, 'at_the_top': 3,
                'wrapper': {'major': 0, 'rminor': 0,
                            'pnfsFilename': '/pnfs/selex/ph001/out4/09/ph001_charm_run009784_001.out4',
                            'uid': 2696, 'sanity_size': 65536, 'rmajor': 0,
                            'machine': ('IRIX64', 'fn781a', '6.5', '04131233', 'IP19'),
                            'uname': 'jurgen', 'pstat': (33188, 185388456L, 1310720L, 1, 1727, 1747,
                                                         80907408L, 1059340564, 997734565, 997734522),
                            'gid': 1747, 'mode': 33252, 'gname': 'e781', 'size_bytes': 80907408L,
                            'fullname': '/spool03/scratch/jurgen/strip-123/a.tmp', 'inode': 0,
                            'minor': 0},
                'times': {'lm_dequeued': 1075147989.9954801, 'encp_start_time': 1075147985.4076791,
                          'job_queued': 1075147987.235173, 't0': 1075147985,
                          'in_queue': 2.7601099014282227},
                'work': 'read_from_hsm', 'version': 'v2_20  CVS $Revision$ <frozen>',
                'encp_daq': None, 'client_crc': 1,
                'infile': '/pnfs/selex/ph001/out4/09/ph001_charm_run009784_001.out4',
                'unique_id': 'fn781a.fnal.gov-1075147986-3251221-0',
                'routing_callback_addr': ('131.225.110.36', 65481)}
    
    import pprint

    key=write_request_ok(w_ticket)
    if key == None:
        print "correct write request"
    else:
        pprint.pprint(w_ticket)
        print "wrong write ticket format. key %s is not present"%(key,)
    print "+++++++++++++++++++++++++++++++++++++++++++++++"

    key=read_request_ok(r_ticket)
    if key == None:
        print "correct read request"
    else:
        pprint.pprint(r_ticket)
        print "wrong read ticket format. key %s is not present"%(key,)
        
