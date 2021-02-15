#!/usr/bin/env python

from __future__ import print_function
import db
import edb
import sys
import string
import time

pf = None
pv = None
bf = None
bv = None

verbose = 0


def ddiff(o1, o2):
    '''
ddiff(o1, o2) -- comparing two objects
            Complex objects, like lists and dictionaries, are
            compared recurrsively.

            Simple objects are compared by their text representation
            Truncating error may happen.
            This is on purpose so that internal time stamp, which is
            a float, will not be considered different from the same
            in journal file, which is a text representation and
            probably with truncated precision
    '''

    # different if of different types
    if not isinstance(o1, type(o2)):
        return 1

    # relax on floating point number
    if isinstance(o1, type(1.0)):
        if o1 == o2 or time.ctime(o1) == time.ctime(o2):
            return 0
        else:
            return 1

        return long(o1) != long(o2)

    # list?
    if isinstance(o1, type([])):
        if len(o1) != len(o2):
            return 1
        for i in range(0, len(o1)):
            if ddiff(o1[i], o2[i]):
                return 1
        return 0

    # dictionary?
    if isinstance(o1, type({})):
        if len(o1) != len(o2):
            return 1
        for i in o1.keys():
            if ddiff(o1[i], o2[i]):
                return 1
        return 0

    # for everything else
    return repr(o1) != repr(o2)


def check_volume(vol):
    result = []
    # checking volume record
    pvol = pv[vol]
    if not pvol:
        result.append("not exist in pdb")
    try:
        bvol = bv[vol]
    except BaseException:
        result.append("not exist in bdb")

    if result:
        return result

    # take care of missing fields
    if 'si_time' not in bvol:
        bvol['si_time'] = [bvol['declared'], bvol['declared']]
    else:
        if bvol['si_time'][0] == 0:
            bvol['si_time'][0] = 0.0
        if bvol['si_time'][1] == 0:
            bvol['si_time'][1] = 0.0
    if 'sum_mounts' not in bvol:
        bvol['sum_mounts'] = 0
    if 'comment' not in bvol:
        bvol['comment'] = ''
    parts = string.split(bvol['volume_family'], '.')
    if len(parts) >= 3:
        bvol['wrapper'] = parts[2]
    else:
        bvol['wrapper'] = 'none'
    if len(parts) < 3:
        bvol['volume_family'] = bvol['volume_family'] + '.none'
    if len(parts) > 3:
        bvol['volume_family'] = string.join(parts[:3], '.')
    if 'at_mover' in bvol:
        del bvol['at_mover']
    if 'status' in bvol:
        del bvol['status']
    if 'file_family' in bvol:
        del bvol['file_family']
    if 'storage_group' in bvol:
        del bvol['storage_group']
    if 'mounts' in bvol:
        del bvol['mounts']
    bvol['remaining_bytes'] = long(bvol['remaining_bytes'])

    bvol['eod_cookie'] = str(bvol['eod_cookie'])
    if bvol['eod_cookie'] == '\x00':
        bvol['eod_cookie'] = ''

    if ddiff(pvol, bvol):
        if verbose:
            print('pdb:', repr(pvol))
            print('bdb:', repr(bvol))
        result.append("different volume record")
        return result

    # check files
    q = "select bfid from file, volume\
	     where volume.label = '%s' and \
	     file.volume = volume.id;" % (vol)
    res = pf.db.query(q).getresult()
    pbfids = []
    for i in res:
        pbfids.append(i[0])

    bbfids = []
    c = bf.inx['external_label'].cursor()
    key, pkey = c.set(vol)
    while key:
        bbfids.append(pkey)
        key, pkey = c.nextDup()
    c.close()

    pl = len(pbfids)
    bl = len(bbfids)

    if pl != bl:
        result.append('pl(%d) != bl(%d)' % (pl, bl))
        return result

    for i in pbfids:
        if not i in bbfids:
            result.append('%s not in bdb' % (i))
            continue
        pfile = pf[i]
        bfile = bf[i]
        if 'pnfsvid' in bfile:
            del bfile['pnfsvid']
        if 'pnfs_mapname' in bfile:
            del bfile['pnfs_mapname']
        if 'drive' not in bfile:
            bfile['drive'] = ''
        if 'pnfs_name0' not in bfile:
            bfile['pnfs_name0'] = ''
        if 'pnfsid' not in bfile:
            bfile['pnfsid'] = ''
        if 'deleted' not in bfile:
            bfile['deleted'] = 'unknown'
        if bfile['sanity_cookie'] != (None, None):
            bfile['sanity_cookie'] = (
                long(
                    bfile['sanity_cookie'][0]), long(
                    bfile['sanity_cookie'][1]))
        if isinstance(bfile['size'], type(1)):
            bfile['size'] = long(bfile['size'])
        if isinstance(bfile['complete_crc'], type(1)):
            bfile['complete_crc'] = long(bfile['complete_crc'])
        if ddiff(pfile, bfile):
            if verbose:
                print('pdb:', repr(pfile))
                print('bdb:', repr(bfile))
            result.append('different file on %s' % (i))

    return result


if __name__ == '__main__':
    print("start", time.ctime(time.time()))
    pf = edb.FileDB()
    pv = edb.VolumeDB()
    bf = db.DbTable('file', '.', '/tmp', ['external_label'], 0)
    bv = db.DbTable('volume', '.', '/tmp', ['library', 'volume_family'], 0)

    if len(sys.argv) > 1:
        check_list = sys.argv[1:]
    else:
        check_list = pv.keys()

    for i in check_list:
        print('checking', i, '...', end=' ')
        result = check_volume(i)
        if not result:
            print('OK')
        else:
            for j in result:
                print(j, '...', end=' ')
            print('ERROR')

    pf.close()
    pv.close()
    bf.close()
    bv.close()
    print("finish", time.ctime(time.time()))
