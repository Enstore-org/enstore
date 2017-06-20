#!/usr/bin/env python
import os
import sys
import pprint

import configuration_client
import info_client
import  enstore_functions2
import en_eval
PACK_BFIDS='files_to_check'
LOG_FILES='/srv3/moibenko'


def identify_SFA_bfids_in_package(infoc, package_bfid, sf_bfids):
    packages = {}
    not_active_sfa_files=[]
    for bfid in sf_bfids:
        bfidinfo = infoc.bfid_info(bfid)
        if bfidinfo['deleted'] != 'no':
            not_active_sfa_files.append(bfid)
            continue
        pack_id = bfidinfo['package_id']
        if not pack_id in packages:
            packinfo = infoc.bfid_info(pack_id)

            if packinfo['deleted'] != 'no':
                print "File %s is found in deleted package %s"%(bfid, pack_id)
            else:
                packages[pack_id] = {'pnfs_name0':packinfo['pnfs_name0'],
                'deleted': packinfo['deleted'],
                'files':[]
                }

        packages[bfidinfo['package_id']]['files'].append(bfid)

    return packages, not_active_sfa_files
        
    
import string
code = None
with open('CDMS149555121000001.bfids', "r") as f:
    code = "bfids=" + string.join(f.readlines(), "")
if code:
    exec(code)
else:
    sys.exit(1)


if len(sys.argv) != 2:
    print 'usage: %s output_file'%(sys.argv[0])
    sys.exit(-1)
out_file=open(sys.argv[1], 'w')
csc = configuration_client.ConfigurationClient(('stkensrv2n', 7500))
ic_conf = csc.get("info_server")
infoc = info_client.infoClient(csc,
                       server_address=(ic_conf['host'],
                                       ic_conf['port']))
pack_files = open(PACK_BFIDS, 'r')
cnt=0
rc = identify_SFA_bfids_in_package(infoc, 'CDMS149555121000001', bfids)
out_file.write("Checked package %s\n"%('CDMS149555121000001',)) 
if rc:
    s = pprint.pformat(rc[0])
    out_file.write('%s\n'%(s,))
    if len(rc[1]) != 0:
        out_file.write("Not active files %s\n"%(rc[1],))


