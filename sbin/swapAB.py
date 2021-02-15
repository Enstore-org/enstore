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

library_prefixes = ['CDF', 'STK', 'D0']
library_migration = 'Migration'
library_9940b = '9940B'

f_prefix = '/pnfs/cdfen/filesets'
f_p = string.split(f_prefix, '/')
f_n = len(f_p)

fcc = None
e_count = 0
debug = 0
doit = 0
vcc = None
cbvolume = []

# restore the correct library


def fix_library(v2):
    # handling library
    vv = vcc.inquire_vol(v2)
    if vv['status'][0] == e_errors.OK:
        ll = string.split(vv['library'], '-')
        if len(ll) == 2 and ll[0] in library_prefixes and \
                ll[1] == library_migration:
            library = ll[0] + '-' + library_9940b
            print('===> %s : changing library from %s to %s ...' % (v2, vv['library'], library), end=' ')
            if doit:
                ticket = {'external_label': v2,
                          'library': library}
                result = vcc.modify(ticket)
                if result['status'][0] == e_errors.OK:
                    print('DONE', end=' ')
                else:
                    print('FAILED', end=' ')
            else:
                print('SIMULATED', end=' ')
    else:
        print('ERROR: volume %s does not exist' % (v2))


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

# to deal with /pnfs/fs/usr/...


def n_path(p):
    pl = string.split(p, '/')
    if pl[2] == 'fs' and pl[3] == 'usr':
        del pl[2]
        del pl[2]
        return string.join(pl, '/')
    else:
        return p


def usage():
    print("usage:")
    print('\t' + sys.argv[0], "file [file [...]]    swap individual files")
    print(
        '\t' + sys.argv[0],
        "-f file [file [...]] swap files listed in the files")
    print('\t' + sys.argv[0], "-v vol [vol [...]]   swap files on the volumes")

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
    print(msg, '...', 'ERROR')
    e_count = e_count + 1


def swap(f):
    global e_count
    e_count = 0
    print(f, '...', end=' ')

    # find the master file
    lf = get_local_pnfs_path(f)

    # is this the pnfs server?
    if lf[:8] != '/pnfs/fs':
        error('not pnfs server')
        return

    # does file exist? writable?
    if not os.access(lf, os.W_OK):
        error(lf + ' not writable')
        return

    fb = b_path(f)
    # does B file exist? readable?
    if not os.access(fb, os.F_OK):
        error(fb + ' not exist')
        return

    if not os.access(fb, os.R_OK):
        error(fb + ' not readable')
        return

    # check A file. It should have no A-file
    fa = a_path(f)
    if os.access(fa, os.F_OK):
        error(fa + ' already exist')
        return

    f_o = pnfs.File(lf)
    if len(f_o.__dict__) < 11:
        error(' missing layer 4')
        return

    f_b = pnfs.File(fb)
    if len(f_b.__dict__) < 11:
        error(fb + ' missing layer 4')
        return

    if debug:
        print()
        print("original pnfs file:")
        f_o.show()
        print()
        print("b pnfs file:")
        f_b.show()
        print()

    # handle library
    if not f_b.volume in cbvolume:
        fix_library(f_b.volume)
        if doit:
            cbvolume.append(f_b.volume)

    # check size
    if long(f_o.size) != long(f_b.size):
        error("p_size(%d, %d)" % (long(f_o.size), long(f_b.size)))
        return

    # check file family
    if f_o.file_family != f_b.file_family:
        error("p_file_family(%s, %s)" % (f_o.file_family, f_b.file_family))
        return

    # get bfids
    file_o = fcc.bfid_info(f_o.bfid)
    if file_o['status'][0] != e_errors.OK:
        error(file_o['status'][1])
        return
    file_b = fcc.bfid_info(f_b.bfid)
    if file_b['status'][0] != e_errors.OK:
        error(file_b['status'][1])
        return

    if debug:
        print()
        print("original file:")
        pprint.pprint(file_o)
        print()
        print("b file:")
        pprint.pprint(file_b)
        print()

    # check size
    if long(file_o['size']) != long(file_b['size']):
        error("f_size(%d, %d)" % (long(file_o['size']), long(file_b['size'])))
        return

    # check crc
    if file_o['complete_crc'] != file_b['complete_crc']:
        error("crc(%d, %d)" % (file_o['complete_crc'], file_b['complete_crc']))
        return

    # check sanity
    if file_o['sanity_cookie'] != file_b['sanity_cookie']:
        error(
            "sanity_cookie(%s, %s)" %
            (repr(
                file_o['sanity_cookie']), repr(
                file_b['sanity_cookie'])))
        return

    # check deleted
    if file_o['deleted'] == 'yes':
        error("is deleted")
        return

    if file_b['deleted'] == 'yes':
        error("B file is deleted")
        return

    # backup original
    fa = File(f)
    fa.path = get_local_pnfs_path(a_path(fa.path))
    if doit:
        fa.create()
    if debug:
        print()
        print("a_file:")
        fa.show()
        print()

    # update file record ...

    ticket = {
        'bfid': f_b.bfid,
        'pnfs_name0': file_o['pnfs_name0'],
        'pnfsid': file_o['pnfsid']}
    if debug:
        print()
        print("modify command")
        pprint.pprint(ticket)
        print()

    if doit:
        result = fcc.modify(ticket)
    else:
        result = {'status': (e_errors.OK, None)}

    if result['status'][0] != e_errors.OK:
        error('failed to update ' + repr(f_b.bfid))
        return

    # update pnfs info

    f_o.volume = f_b.volume
    f_o.location_cookie = f_b.location_cookie
    f_o.size = f_b.size
    f_o.file_family = f_b.file_family
    f_o.bfid = f_b.bfid
    f_o.drive = f_b.drive

    if doit:
        f_o.update()

    if debug:
        print()
        print('update original:')
        f_o.show()
        print()

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
            print('swapping', f2, '...')
            if os.access(f2, os.R_OK):
                ff = open(sys.argv[2])
                l = string.strip(ff.readline())
                while l:
                    if l[:5] == '/pnfs':
                        swap(l)
                        te_count = te_count + e_count
                    l = string.strip(ff.readline())
                ff.close()
            else:
                print(f2, 'does not exist')
    elif sys.argv[1] == '-v':
        for v2 in sys.argv[2:]:
            result = fcc.list_active(v2)
            if result['status'][0] == e_errors.OK:
                sdoit = doit  # save doit flag
                doit = 0  # force checking first
                te_count = 0
                print("checking", v2, "...", end=' ')
                vol = vcc.inquire_vol(v2)
                if vol['status'][0] != e_errors.OK:
                    print('does not exist ... ERROR')
                    continue
                if vol['system_inhibit'][1] == 'migrated':
                    print('already migrated ... WARNING')
                    continue
                print()
                for i in result['active_list']:
                    swap(i)
                    te_count = te_count + e_count
                doit = sdoit  # restore doit
                if te_count == 0 and doit:
                    # doit = 1
                    print("swapping", v2, "...")
                    for i in result['active_list']:
                        swap(i)
                        te_count = te_count + e_count
                    # set system inhibit to migrated
                    print('set', v2, 'to migrated ...', end=' ')
                    res = vcc.set_system_migrated(v2)
                    if res['status'][0] == e_errors.OK:
                        print('OK')
                    else:
                        print('FAILED')

                    # set system inhibit to notallowed
                    print('set', v2, 'to notallowed ...', end=' ')
                    res = vcc.set_system_notallowed(v2)
                    if res['status'][0] == e_errors.OK:
                        print('OK')
                    else:
                        print('FAILED')

            else:
                print(result['status'][1])
    else:
        for l in sys.argv:
            if l[:5] == '/pnfs':
                swap(l)
                te_count = te_count + e_count

    sys.exit(te_count)
