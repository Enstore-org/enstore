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
    for i in mtab:
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


def check(f):
    global e_count
    e_count = 0
    print(f, '...', end=' ')

    # does file exist?
    if not os.access(f, os.F_OK):
        error('not exist')
        print('ERROR')
        return

    if not os.access(f, os.R_OK):
        error('not readable')
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

    # check A file. It should have no A-file
    fa = a_path(f)
    if os.access(fa, os.F_OK):
        error(fa + ' already exist')
        print('ERROR')
        return

    f_o = pnfs.File(f)
    if len(f_o.__dict__) < 11:
        error(' missing layer 4')
        print('ERROR')
        return

    f_b = pnfs.File(fb)

    if len(f_b.__dict__) < 11:
        error(fb + ' missing layer 4')
        print('ERROR')
        return

    # check size
    if long(f_o.size) != long(f_b.size):
        error("p_size(%d, %d)" % (long(f_o.size), long(f_b.size)))

    # check file family
    if f_o.file_family != f_b.file_family:
        error("p_file_family(%s, %s)" % (f_o.file_family, f_b.file_family))

    # get bfids
    file_o = fcc.bfid_info(f_o.bfid)
    if file_o['status'][0] != e_errors.OK:
        error(file_o['status'][1])
        print('ERROR')
        return
    file_b = fcc.bfid_info(f_b.bfid)
    if file_b['status'][0] != e_errors.OK:
        error(file_b['status'][1])
        print('ERROR')
        return

    # check size
    if long(file_o['size']) != long(file_b['size']):
        error("f_size(%d, %d)" % (long(file_o['size']), long(file_b['size'])))

    # check crc
    if file_o['complete_crc'] != file_b['complete_crc']:
        error("crc(%d, %d)" % (file_o['complete_crc'], file_b['complete_crc']))

    # check sanity
    if file_o['sanity_cookie'] != file_b['sanity_cookie']:
        error(
            "sanity_cookie(%s, %s)" %
            (repr(
                file_o['sanity_cookie']), repr(file_b['sanity_cookie'])))

    # check deleted
    if file_o['deleted'] == 'yes':
        error("is deleted")

    if file_b['deleted'] == 'yes':
        error("B file is deleted")

    if e_count:
        print('ERROR')
    else:
        print('OK')
    return


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

    te_count = 0
    if sys.argv[1] == '-f':
        for f2 in sys.argv[2:]:
            print('checking', f2, '...')
            if os.access(f2, os.R_OK):
                ff = open(f2)
                l = string.strip(ff.readline())
                while l:
                    if l[:5] == '/pnfs':
                        check(l)
                        te_count = te_count + e_count
                    l = string.strip(ff.readline())
                ff.close()
            else:
                print(f2, 'does not exist')
    elif sys.argv[1] == '-v':
        for v2 in sys.argv[2:]:
            print('checking', v2, '...', end=' ')
            vol = vcc.inquire_vol(v2)
            if vol['status'][0] != e_errors.OK:
                print('does not exist ... ERROR')
                continue
            if vol['system_inhibit'][1] == 'migrated':
                print('already migrated ... WARNING')
                continue
            print()
            result = fcc.list_active(v2)
            if result['status'][0] == e_errors.OK:
                for i in result['active_list']:
                    check(i)
                    te_count = te_count + e_count
            else:
                print(result['status'][1])
    else:
        for l in sys.argv:
            if l[:5] == '/pnfs':
                check(l)
                te_count = te_count + e_count

    sys.exit(te_count)
