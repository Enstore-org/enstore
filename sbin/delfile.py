#!/usr/bin/env python
'''
This is a replacement for $ENSTORE_DIR/sbin/delfile
'''

import file_clerk_client
import volume_clerk_client
import option
import os
import string
import generic_client
import e_errors

vols = []

def get_trash():
    if os.environ.has_key('TRASH_CAN'):
        return os.environ['TRASH_CAN']
    else:
        if os.uname()[1][:3] == 'rip':
            return '/rip6a/pnfs/trash/4'
        else:
            return '/diska/pnfs/trash/4'

def get_bfid(mf):
    try:
        f = open(mf)
        r = f.readlines()
        f.close()
    except:
        return None, None

    if len(r) > 8:
        return string.strip(r[0]), string.strip(r[8])
    else:
        return None, None

if __name__ == '__main__':
    intf = option.Interface()
    fcc = file_clerk_client.FileClient((intf.config_host, intf.config_port))
    generic_client.init_done = 0
    vcc = volume_clerk_client.VolumeClerkClient((intf.config_host, intf.config_port))
    trash = get_trash()
    # print trash
    files = os.listdir(trash)
    for i in files:
        fp = os.path.join(trash, i)
        vol, bfid = get_bfid(fp)
        if bfid:
            if not vol in vols:
                vols.append(vol)
            print 'deleting', bfid, '...',
            # delete
            fcc.bfid = bfid
            result = fcc.set_deleted('yes')
            if result['status'][0] != e_errors.OK:
                print bfid, result['status'][1]
            else:
                print 'done'
            try:
                os.unlink(fp)
            except:
                print 'can not delete', fp

    for i in vols:
        print 'touching', i, '...',
        result = vcc.touch(i)
        if result['status'][0] == e_errors.OK:
            print 'done'
        else:
            print 'failed'
