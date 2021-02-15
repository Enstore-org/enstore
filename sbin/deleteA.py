#!/usr/bin/env python

from __future__ import print_function
import pnfs
import string
import sys
import os
import option
import file_clerk_client
import volume_clerk_client
import e_errors
import pprint

f_prefix = '/pnfs/cdfen/filesets'
f_p = string.split(f_prefix, '/')
f_n = len(f_p)

fcc = None
vcc = None
e_count = 0
te_count = 0

_mtab = None


def get_mtab():
    global _mtab
    if _mtab is None:
        _mtab = {}
        try:
            f = open('/etc/mtab')
            l = f.readline()
            while l:
                lc = string.split(l)
                if lc[1][:5] == '/pnfs':
                    c1 = string.split(lc[0], ':')
                    if len(c1) > 1:
                        _mtab[lc[1]] = (c1[1], c1[0])
                    else:
                        _mtab[lc[1]] = (c1[0], None)
                l = f.readline()
            f.close()
        except BaseException:
            _mtab = {}
            f.close()
    return _mtab

# get_local_pnfs_path(p) -- find local pnfs path


def get_local_pnfs_path(p):
    mtab = get_mtab()
    for i in mtab.keys():
        if string.find(p, i) == 0 and \
           string.split(os.uname()[1], '.')[0] == mtab[i][1]:
            p1 = os.path.join(
                '/pnfs/fs/usr', string.replace(p, i, mtab[i][0][1:]))
            return p1
    return p


class File(pnfs.File):
    def __init__(self, file):
        pnfs.File.__init__(self, file)

    # overwrite update
    # update() -- write out to pnfs files
    def update(self):
        if not self.bfid:
            return
        if not self.consistent():
            if self.path != self.p_path:
                if self.path != get_local_pnfs_path(self.p_path):
                    d1, f1 = os.path.split(self.path)
                    d2, f2 = os.path.split(self.p_path)
                    if f1 != f2:
                        raise 'DIFFERENT_PATH'
            else:
                raise 'INCONSISTENT'
        if self.exists():
            # writing layer 1
            f = open(self.layer_file(1), 'w')
            f.write(self.bfid)
            f.close()
            # writing layer 4
            f = open(self.layer_file(4), 'w')
            f.write(self.volume + '\n')
            f.write(self.location_cookie + '\n')
            f.write(str(self.size) + '\n')
            f.write(self.file_family + '\n')
            f.write(self.p_path + '\n')
            f.write(self.volmap + '\n')
            f.write(self.pnfs_id + '\n')
            f.write(self.pnfs_vid + '\n')
            f.write(self.bfid + '\n')
            f.write(self.drive + '\n')
            if self.complete_crc:
                f.write(self.complete_crc + '\n')
            f.close()
            # set file size
            self.set_size()
        return

    # over write create
    # create() -- create the file
    def create(self):
        # do not create if there is no BFID
        if not self.bfid:
            return
        if not self.exists():
            dir, file = os.path.split(self.path)
            if not os.path.exists(dir):
                os.makedirs(dir)
            f = open(self.path, 'w')
            f.close()
            self.update()


def usage():
    print("usage:")
    print('\t' + sys.argv[0], "file [file [...]]    check individual files")
    print(
        '\t' + sys.argv[0],
        "-f file [file [...]] check files listed in the files")
    print(
        '\t' + sys.argv[0],
        "-v vol [vol [...]]   check files on the volumes")

# to deal with /pnfs/fs/usr/...


def n_path(p):
    pl = string.split(p, '/')
    if pl[2] == 'fs' and pl[3] == 'usr':
        del pl[2]
        del pl[2]
        return string.join(pl, '/')
    else:
        return p

# a_path(p):
#    p = f_prefix/X/... --> f_prefix/_A_X/...
#


def a_path(p1):
    p = n_path(p1)
    pl = string.split(p, '/')
    # check prefix
    if pl[:f_n] != f_p:
        return None
    pl[f_n] = '.A_' + pl[f_n]
    return string.join(pl, '/')


def b_path(p1):
    p = n_path(p1)
    pl = string.split(p, '/')
    # check prefix
    if pl[:f_n] != f_p:
        return None
    pl[f_n] = '.B_' + pl[f_n]
    return string.join(pl, '/')


def error(msg):
    global e_count
    print(msg, '...', end=' ')
    e_count = e_count + 1


def check(bfid, vol):
    global e_count
    e_count = 0

    file = fcc.bfid_info(bfid)

    f = file.get('pnfs_name0', 'unknown')

    print('checking', bfid, f, '...', end=' ')

    if f == 'unknown' or file['deleted'] == 'yes':
        print('OK')
        return

    # does original file exist?
    if not os.access(f, os.F_OK):
        error('not exist')
        print('ERROR')
        return

    if not os.access(f, os.R_OK):
        error('not readable')
        print('ERROR')
        return

    # does A file exist? readable?
    fa = a_path(f)
    if not os.access(fa, os.F_OK):
        error(fa + ' not exist')
        print('ERROR')
        return

    if not os.access(fa, os.R_OK):
        error(fa + ' not readable')
        print('ERROR')
        return

    fb = b_path(f)
    # does B file exist? readable?
    if not os.access(fb, os.F_OK):
        error(fb + ' not exist')
        print('ERROR')
        return

    if not os.access(fb, os.R_OK):
        error(fb + ' not readable')
        print('ERROR')
        return

    f_o = pnfs.File(f)
    if len(f_o.__dict__) < 11:
        error('missing layer 4')
        print('ERROR')
        return

    if f_o.volume == vol:
        error('not migrated')
        print('ERROR')
        return

    f_a = pnfs.File(fa)
    if len(f_o.__dict__) < 11:
        error(fa + ' missing layer 4')
        print('ERROR')
        return

    if f_a.volume != vol or f_a.bfid != bfid:
        error('bad .A_* file')
        print('ERROR')
        return

    if e_count:
        print('ERROR')
    else:
        print('OK')
    return


def delete(bfid):
    ff = fcc.bfid_info(bfid)

    f = ff.get('pnfs_name0', 'unknown')
    if not f:
        f = 'unknown'

    if f == 'unknown':
        af = 'unknown'
    else:
        af = a_path(f)
    print('deleting', bfid, af, '...', end=' ')

    if ff["deleted"] == "yes" or ff["deleted"] == "unknown":
        print("already deleted ... OK")
        return

    ticket = {'bfid': bfid, 'pnfs_name0': af, 'deleted': 'yes'}
    res = fcc.modify(ticket)
    if res['status'][0] == e_errors.OK:
        print('OK')
    else:
        print('ERROR')


class Interface(option.Interface):
    def __init__(self, args=sys.argv, user_mode=0):
        option.Interface.__init__(self, args, user_mode)

    def print_help(self):
        usage()

    def print_usage(self, message=None):
        if message:
            print(message)
            print()
        usage()


if __name__ == '__main__':

    # initialize file clerk client
    intf = Interface()
    fcc = file_clerk_client.FileClient((intf.config_host, intf.config_port))

    vcc = volume_clerk_client.VolumeClerkClient(fcc.csc)

    for v2 in sys.argv[1:]:
        te_count = 0
        print('checking', v2, '...', end=' ')
        vol = vcc.inquire_vol(v2)
        # check volume
        if vol['status'][0] != e_errors.OK:
            # having trouble getting volume info
            print(vol['status'][1], '... ERROR')
            continue

        if vol['system_inhibit'][0] != 'NOTALLOWED' or \
           vol['system_inhibit'][1] != 'migrated':
            # not set to NOTALLOW
            print('not a migrated volume ... ERROR')
            continue

        # though it is not necessary, check it anyway
        if vol['media_type'] != '9940':
            print('not a 9940 media ...', end=' ')

        bfids = fcc.get_bfids(v2).get('bfids')
        if bfids is None:  # impossible
            print('no such volume ... ERROR')
            continue

        # done checking the volume information
        print()

        for i in bfids:
            check(i, v2)
            te_count = te_count + e_count

        if te_count == 0:
            print('deleting', v2, '...')
            for i in bfids:
                delete(i)

        # check if vol is empty

        print('final check', v2, '...', end=' ')
        af = fcc.list_active(v2).get('active_list')
        if af is None:
            print('can not get active file list ... ERROR')
        elif len(af):
            print('still has %d active files ... ERROR' % (len(af)))
        else:
            print('no active file left ... OK')

    sys.exit(te_count)
