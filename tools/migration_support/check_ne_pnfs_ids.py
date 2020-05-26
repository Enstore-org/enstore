#!/usr/bin/env python
import sys
import errno
import configuration_client
import info_client
import file_clerk_client
import e_errors
import namespace
import chimera
import checksum

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

with open('not_found_pnfs_ids_2', 'r') as f:
    check_these = []
    while True:
        l = f.readline()
        if not l:
            break
        nsid = l[:-1]
        f_info = infoc.find_file_by_pnfsid(nsid)
        #print("FINFO", f_info)
        if not e_errors.is_ok(f_info):
            if f_info['status'][0] == e_errors.TOO_MANY_FILES:
                print('Skipping, but check')
                print('>>>>>>>>>>>>>>')
                for f_i in f_info['file_list']:
                    print(f_i['bfid'], f_i['pnfsid'], f_i['pnfs_name0'], f_i['file_family'], f_i['deleted'])
                    check_these.append((f_i['bfid'], f_i['pnfsid'], f_i['pnfs_name0'], f_i['file_family'], f_i['deleted']))
                print('<<<<<<<<<<<<<<')
                continue
            else:
                break
        print('orig_file', 'bfid', f_info['bfid'], 
              'pnfsid', f_info['pnfsid'],
              'deleted', f_info['deleted'])
       
        try:
            f = ns.get_nameof(f_info['pnfsid'])
        except Exception as e:
            if e[0] == errno.ENOENT:
                # read layer 4 info
                l4 =  chimera.get_layer_4(f_info['pnfs_name0'])
                bfid_info = infoc.bfid_info(l4['bfid'])
                if e_errors.is_ok(bfid_info):
                    print('found', 'bfid', 
                          bfid_info['bfid'], 
                          'pnfsid', bfid_info['pnfsid'],
                          'deleted', bfid_info['deleted'])
                    if bfid_info['size'] == f_info['size']:
                        dst_crc = bfid_info['complete_crc']
                        src_crc = f_info['complete_crc']
                        print('CRCs', src_crc, dst_crc)
                    if  src_crc != dst_crc:
                        print('Sizes match, CRCs do not match. Different files. Please check', f_info['bfid'], bfid_info['bfid'])
                        continue
                    if bfid_info['deleted'] == 'yes':
                        print('file exists in pnfs, but marked deleted. Check',bfid_info['bfid'])
                        continue
                    if f_info['deleted'] == 'no':
                        print('will set deleted', f_info['bfid'])
                        res = fcc.set_deleted('yes', bfid = f_info['bfid'])
                        if not e_errors.is_ok(res):
                            print('set_deleted returned', res)

if check_these:
    print('Check the following %s'%('check_multiple_list',))
    with open('check_multiple_list', 'w') as f:
        f.write('%s\n'%(check_these,))
