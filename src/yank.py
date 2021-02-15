#!/usr/bin/env python
#
# yank -- yank volumes from the database and store the information in
#         a safe persistent store.
#
# usage:
#	yank vol ...
#

from __future__ import print_function
import option
import volume_clerk_client
import file_clerk_client
import shelve
import sys
import e_errors

# check the arguments
if len(sys.argv) < 2:
    print('usage: %s vol [vol [vol [..]]]' % (sys.argv[0]))
    sys.exit(0)

# get csc

intf = option.Interface()
csc = (intf.config_host, intf.config_port)

# get vcc and fcc
vcc = volume_clerk_client.VolumeClerkClient(csc)
fcc = file_clerk_client.FileClient(vcc.csc)

# get yank backup
yanked = shelve.open('YANKED')

for i in range(1, len(sys.argv)):
    v = sys.argv[i]
    print('yanking', v, '...', end=' ')
    # get volume infomation
    vol = vcc.inquire_vol(v)
    if vol['status'][0] == e_errors.OK:
        del vol['status']
        # get file information
        files = {}
        error = 0
        ff = fcc.get_bfids(v)
        if ff['status'][0] == e_errors.OK:
            bfids = ff['bfids']
            for j in bfids:
                f = fcc.bfid_info(j)
                if f['status'][0] == e_errors.OK:
                    del f['status']
                    if 'fc' in f:
                        del f['fc']
                    if 'vc' in f:
                        del f['vc']
                    files[j] = f
                else:
                    print('\n\t\t' + repr(f['status']))
                    error = error + 1
            yanked[v] = {'vol': vol, 'files': files}
        else:
            print('\n\t' + repr(ff['status']))
            error = error + 1
        if not error:
            # really erase the records
            for j in bfids:
                res = fcc.del_bfid(j)
                if res['status'][0] != e_errors.OK:
                    print('\n\t' + repr(res['status']))
                    error = error + 1
                    break
            # erase volume record
            res = vcc.rmvolent(v)
            if res['status'][0] != e_errors.OK:
                print('\n\t' + repr(res['status']))
                error = error + 1
        if not error:
            print('done')
        else:
            print('\n\t... failed')
    else:
        print(repr(vol['status']))
yanked.close()
