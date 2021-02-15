#!/usr/bin/env python

from __future__ import print_function
import db
import sys
import time
import pprint


def formatedf(file):
    bfid = file.get('bfid', '')
    complete_crc = file.get('complete_crc', -1)
    if complete_crc is None or complete_crc == 'none':
        complete_crc = -1
    deleted = 'u'
    if 'deleted' in file:
        if file['deleted'] == 'no':
            deleted = 'n'
        elif file['deleted'] == 'yes':
            deleted = 'y'
    drive = file.get('drive', '')
    external_label = file.get('external_label', '')
    location_cookie = file.get('location_cookie', '')
    # if len(location_cookie) > 24:
    #	return None
    pnfs_name0 = file.get('pnfs_name0', '')
    pnfsid = file.get('pnfsid', '')
    sanity_cookie_0 = file.get('sanity_cookie', (None, None))[0]
    sanity_cookie_1 = file.get('sanity_cookie', (None, None))[1]
    if sanity_cookie_0 is None:
        sanity_cookie_0 = -1
    if sanity_cookie_1 is None:
        sanity_cookie_1 = -1
    size = file.get('size', 0)
    try:
        res = '%s\t%d\t%c\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d' % (
            bfid, complete_crc, deleted, drive, external_label,
            location_cookie, pnfs_name0, pnfsid,
            sanity_cookie_0, sanity_cookie_1, size)
    except BaseException:
        pprint.pprint(file)
        res = None

    return res


if __name__ == '__main__':
    f = db.DbTable('file', '.', '/tmp', [], 0)
    c = f.newCursor()
    k, v = c.first()
    count = 0
    if len(sys.argv) > 1:
        outf = open(sys.argv[1], 'w')
    else:
        outf = open('db.dmp', 'w')

    last_time = time.time()
    while k:
        l = formatedf(v)
        if l:
            outf.write(l + '\n')
        else:
            print(count)
        k, v = next(c)
        count = count + 1
        if count % 1000 == 0:
            time_now = time.time()
            print("%12d %14.2f records/sec" % (count,
                                               1000.0 / (time_now - last_time)))
            last_time = time_now
        # if count > 10:
        #	break
    outf.close()
    c.close()
    f.close()
