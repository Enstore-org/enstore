#!/usr/bin/env python

import os
import file_clerk_client
import volume_clerk_client
import option
import pnfs
import sys
import stat
import generic_client
import volume_family
import e_errors
import string

def usage():
    print "usage: %s path [path2 [path3 [ ... ]]]"%(sys.argv[0])

if len(sys.argv) == 1 or sys.argv[1] == '--help':
    usage()
    sys.exit(0)

intf = option.Interface()
fcc = file_clerk_client.FileClient((intf.config_host, intf.config_port))
generic_client.init_done = 0
vcc = volume_clerk_client.VolumeClerkClient((intf.config_host, intf.config_port))

ff = {}

def error(s):
    print 'Error:  ', s

def layer_file(f, n):
    p, ff = os.path.split(f)
    return os.path.join(p, '.(use)(%d)(%s)'%(n, ff))

def check(f):
    msg = []
    if not os.access(f, os.R_OK):
        return ['no read permission']
    try:
        pf = pnfs.File(f)
    except:
        return ['corrupted meta-data']
    # get bfid
    f1 = open(layer_file(f, 1))
    bfid = f1.readline()
    f1.close()
    if bfid != pf.bfid:
        msg.append('bfid(%s, %s)'%(bfid, pf.bfid))
    fr = fcc.bfid_info(bfid)
    if fr['status'][0] != e_errors.OK:
        msg.append('not in db')
	return msg
    # volume label
    if pf.volume != fr['external_label']:
        msg.append('label(%s, %s)'%(pf.volume, fr['external_label']))
    # location cookie
    if pf.location_cookie != fr['location_cookie']:
        msg.append('location_cookie(%s, %s)'%(pf.location_cookie, fr['location_cookie']))
    # size
    real_size = os.stat(f)[stat.ST_SIZE]
    if real_size != eval(pf.size) or fr['size'] != real_size:
        msg.append('size(%d, %d, %d)'%(eval(pf.size), real_size, fr['size']))
    # file_family
    if ff.has_key(fr['external_label']):
        file_family = ff[fr['external_label']]
    else:
        vol = vcc.inquire_vol(fr['external_label'])
        if vol['status'][0] != e_errors.OK:
            msg.append('missing vol '+fr['external_label'])
            return msg
        file_family = volume_family.extract_file_family(vol['volume_family'])
        ff[fr['external_label']] = file_family
    if pf.file_family != file_family:
        msg.append('file_family(%s, %s)'%(pf.file_family, file_family))
    # pnfsid
    id = pf.get_pnfs_id()
    if id != pf.pnfs_id or id != fr['pnfsid']:
        msg.append('pnfsid(%s, %s, %s)'%(pf.pnfs_id, id, fr['pnfsid']))
    # drive
    if fr.has_key('drive'):	# some do not have this field
        if pf.drive != fr['drive']:
            msg.append('drive(%s, %s)'%(pf.drive, fr['drive']))
    # path
    if pf.path != fr['pnfs_name0']:
        p1 = string.split(pf.path, '/')
        p2 = string.split(fr['pnfs_name0'], '/')
        if p1[-1] != p2[-1] or p1[1:4] != p2[1:4]:
            msg.append('pnfs_path(%s, %s)'%(pf.path, fr['pnfs_name0']))
    # path2
    if pf.path != pf.p_path:
        msg.append('path(%s, %s)'%(pf.path, pf.p_path))
    # deleted
    if fr['deleted'] != 'no':
        msg.append('deleted(%s)'%(fr['deleted']))

    return msg

def check_file(f):
    # if f is a directory, recursively check its files
    if os.path.isdir(f):
        if os.access(f, os.R_OK) and os.access(f, os.X_OK):
            for i in os.listdir(f):
                check_file(os.path.join(f,i))
        else:
            print 'can not access directory', f
    elif os.path.isfile(f):
        print f+' ...',
        res = check(f)
        if res:
            for i in res:
                print i+' ...',
            print 'ERROR'
        else:
            print 'OK'
    elif os.path.islink(f):
        error('missing the original of link '+f)
    else:
        error('unrecognized type of '+f)

if __name__ == '__main__':

    for i in sys.argv[1:]:
        check_file(i)
