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

fcc = None
vcc = None
ff = {}

def usage():
    print "usage: %s path [path2 [path3 [ ... ]]]"%(sys.argv[0])
    print "usage: %s --help"%(sys.argv[0])
    print "usage: %s --infile file"%(sys.argv[0])

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
    try:
        if bfid != pf.bfid:
            msg.append('bfid(%s, %s)'%(bfid, pf.bfid))
    except:
        if len(bfid) < 8:
            msg.append('missing layer 1')
            return msg
        else:
            msg.append('missing layer 4')
            fr = fcc.bfid_info(bfid)
            if fr['status'][0] != e_errors.OK:
                msg.append('not in db')
                return msg
            if fr.has_key('pnfs_name0'):
                if pf.path != fr['pnfs_name0']:
                    msg.append('pnfs_path(%s, %s)'%(pf.path, fr['pnfs_name0']))
            else:
                msg.append('unknown file')
            id = pf.get_pnfs_id()
            if fr.has_key('pnfsid'):
                if id != fr['pnfsid']:
                    msg.append('pnfsid(%s, %s)'%(id, fr['pnfsid']))
            else:
                msg.append('no pnfs id in db')
            return msg

    fr = fcc.bfid_info(bfid)
    if fr['status'][0] != e_errors.OK:
        msg.append('not in db')
	return msg
    # volume label
    try:
        if pf.volume != fr['external_label']:
            msg.append('label(%s, %s)'%(pf.volume, fr['external_label']))
    except:
        msg.append('no or corrupted external_label')
    # location cookie
    try:
        if pf.location_cookie != fr['location_cookie']:
            msg.append('location_cookie(%s, %s)'%(pf.location_cookie, fr['location_cookie']))
    except:
        msg.append('no or corrupted location_cookie')
    # size
    try:
        real_size = os.stat(f)[stat.ST_SIZE]
        if long(pf.size) != long(fr['size']):
            msg.append('size(%d, %d, %d)'%(long(pf.size), long(real_size), long(fr['size'])))
        elif real_size != 1 and long(real_size) != long(pf.size):
            msg.append('size(%d, %d, %d)'%(long(pf.size), long(real_size), long(fr['size'])))
    except:
        msg.append('no or corrupted size')
    # file_family
    try:
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
    except:
        msg.append('no or corrupted file_family')
    # pnfsid
    try:
        id = pf.get_pnfs_id()
        if id != pf.pnfs_id or id != fr['pnfsid']:
            msg.append('pnfsid(%s, %s, %s)'%(pf.pnfs_id, id, fr['pnfsid']))
    except:
        msg.append('no or corrupted pnfsid')
    # drive
    try:
        if fr.has_key('drive'):	# some do not have this field
            if pf.drive != fr['drive']:
                if pf.drive != 'imported' or fr['drive'] != 'unknown:unknown':
                    msg.append('drive(%s, %s)'%(pf.drive, fr['drive']))
    except:
        msg.append('no or corrupted drive')
    # path
    try:
        if pf.path != fr['pnfs_name0']:
            p1 = string.split(pf.path, '/')
            p2 = string.split(fr['pnfs_name0'], '/')
            if p1[-1] != p2[-1] or p1[1:4] != p2[1:4]:
                msg.append('pnfs_path(%s, %s)'%(pf.path, fr['pnfs_name0']))
    except:
        msg.append('no or corrupted pnfs_path')

    # path2
    try:
        if pf.path != pf.p_path:
            msg.append('path(%s, %s)'%(pf.path, pf.p_path))
    except:
        msg.append('no or corrupted l4_pnfs_path')

    # deleted
    try:
        if fr['deleted'] != 'no':
            msg.append('deleted(%s)'%(fr['deleted']))
    except:
        msg.append('no deleted field')

    return msg

def check_file(f):
    # if f is a directory, recursively check its files
    if os.path.isdir(f):
        # skip symbolic link to a directory
        if not os.path.islink(f):
            # skip volmap
            if os.path.split(f)[1] != 'volmap':
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

    if len(sys.argv) == 1 or sys.argv[1] == '--help':
        usage()
        sys.exit(0)

    if len(sys.argv) == 3 and sys.argv[1] == '--infile':
        f = open(sys.argv[2])
        list = map(string.strip, f.readlines())
        f.close()
    else:
        list = sys.argv[1:]

    intf = option.Interface()
    fcc = file_clerk_client.FileClient((intf.config_host, intf.config_port))
    generic_client.init_done = 0
    vcc = volume_clerk_client.VolumeClerkClient((intf.config_host, intf.config_port))

    ff = {}

    for i in list:
        if i[:2] != '--':
            check_file(i)
