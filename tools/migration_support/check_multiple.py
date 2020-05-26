#!/usr/bin/env python
import sys
import os
import errno
import configuration_client
import info_client
import file_clerk_client
import e_errors
import namespace
import chimera
import find_pnfs_file

csc = configuration_client.ConfigurationClient(('stkensrv10n', 7500))
ic_conf = csc.get("info_server")
infoc = info_client.infoClient(csc,
                               server_address=(ic_conf['host'],
                                               ic_conf['port']))
fc_conf = csc.get("file_clerk")
fcc = file_clerk_client.FileClient(csc, bfid=0,
                                   server_address=(fc_conf['host'],
                                                   fc_conf['port']))

ns = namespace.StorageFS('/pnfs/fs', '/pnfs/fs')

with open(sys.argv[1], 'r') as f:
    check_these = eval(f.readline())
    check_these_dict ={}
    for f_i in check_these:
        if f_i[1] not in check_these_dict:
            check_these_dict[f_i[1]] = []
        check_these_dict[f_i[1]].append(f_i)
    print('Will check %s'%(check_these_dict,))
    #sys.exit(0)
    for f in check_these_dict:
        file_exists = True
        for f_i in check_these_dict[f]:
            if not os.path.exists(f_i[2]):
                print('File does not exist',f_i)
                file_exists = False
                if not(os.path.dirname(f_i[2])):
                    print('directory does not exist', os.path.dirname(f_i[2]))
                else:
                    print('directory exists', os.path.dirname(f_i[2]))
                    if f_i[4] == 'no':
                        bfid_info = infoc.bfid_info(f_i[0])
                        if bfid_info['deleted'] != 'yes':
                            print('Will mark deleted', f_i[0]) 
                            res = fcc.set_deleted('yes', bfid = f_i[0])
                            if not e_errors.is_ok(res):
                                print('set_deleted returned', res)
                        
            else:
                print('File exists',f_i[2])
                break
        if file_exists:
            try:
                fn = ns.get_nameof(f)
            except Exception as e:
                if e[0] == errno.ENOENT:
                    print('No action will be taken')



