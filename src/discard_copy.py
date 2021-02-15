#!/usr/bin/env python
"""
discard_copy.py
"""
from __future__ import print_function

import duplication_util
import sys
import e_errors

dm = duplication_util.DuplicationManager()


def usage():
    print("Usage:")
    print("%s --bfid bfid ..." % (sys.argv[0]))
    print("%s --vol vol ..." % (sys.argv[0]))


def discard_vol(vol):
    # check if all files are copy
    q = "select bfid from file, volume where file.volume = volume.id and label = '%s' and deleted = 'n';" % (
        vol)
    res = db.db.query(q).getresult()
    for i in res:
        bfid = res[i][0]
        if not dm.is_copy(bfid):
            return "%s is not a primary file"
    # now, maak them deleted
    for i in res:
        bfid = res[i][0]
        r = dm.fcc.set_deleted('yes', 'no', bfid)
        if r['status'][0] != e_errors.OK:
            return "failed to mark %s deleted" % (bfid)
    return


if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
        sys.exit()

    if sys.argv[1] == '--vol':
        for i in sys.arg[2:]:
            print("discarding %s ..." % (i), end=' ')
            res = swap_vol(i)
            if res:
                print(res, "... ERROR")
            else:
                print("OK")
    elif sys.argv[1] == '--bfid':
        for i in sys.arg[2:]:
            print("discarding %s ..." % (i))
            if dm.is_copy(i):
                r = dm.fcc.set_deleted('yes', 'no', i)
                if r['status'][0] != e_errors.OK:
                    print("failed to mark %s deleted ... ERROR" % (i))
                else:
                    print("OK")
