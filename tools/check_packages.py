#!/usr/bin/env python
"""
Instructions
for stkensrv3n
1. Put empty packages bfids into files_to_check
2.  scp corresponding log files to /srv3/moibenko
3.  cd moibenko/empty_packages
4. run check_packages.py out_file
5. Check out_file.
If not all files were identified do more actios, such as read the 'empty'
package and check what SAF bfids it has and then identify these bfids in not empty packages.
After al checks are done delete 'empty' package.
"""

import os
import sys
import pprint

import configuration_client
import info_client
import  enstore_functions2
import en_eval
PACK_BFIDS='files_to_check'
LOG_FILES='/srv3/moibenko'


def identify_SFA_bfids_in_package(infoc, package_bfid, log_files):
    # find files belonging to a package in the log file
    pack_bfid_info = infoc.bfid_info(package_bfid)
    if not pack_bfid_info:
        print 'No info for %s'%(package_bfid,)
        return
    pack_fn = os.path.basename(pack_bfid_info['pnfs_name0'])
    #print "PACK_FN", pack_fn
    lfn = pack_fn.split('T')[0][pack_fn.find('-2')+1:]
    #print "LFN", lfn
    #return
    log_file_name = os.path.join(log_files,'LOG-%s'%(lfn,))
    grep_cmd = 'fgrep %s %s | fgrep packed_file'%(pack_fn, log_file_name)
    print "PACK_BF", package_bfid
    print "GREP_CMD", grep_cmd
    #return
    res = enstore_functions2.shell_command(grep_cmd)
    print "RES", res
    if res:
        sf_bfids = en_eval.en_eval(res[0][res[0].find('['):res[0].find(']')+1])
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
while True:
    l = pack_files.readline()
    if not l:
        break
    rc = identify_SFA_bfids_in_package(infoc, l[:-1], LOG_FILES)
    out_file.write("Checked package %s\n"%(l[:-1],)) 
    if rc:
        s = pprint.pformat(rc[0])
        out_file.write('%s\n'%(s,))
        if len(rc[1]) != 0:
            out_file.write("Not active files %s\n"%(rc[1],))
    cnt += 1


