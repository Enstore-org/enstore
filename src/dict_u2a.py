#!/usr/bin/env python

###############################################################################
# $Id$
# Convert any dictionary key | value from unicode to ascii if its type is unicode.
# All strings in objects delivered by amqp are automatically converted to UTF.
# This creates a problem in processing this objects by existing enstore code,
# because it does not send strings in UTF.
# To resolve this problem in this module there are methods converting
# dictionary items to ascii if their format is UTF
###############################################################################
# system imports
from __future__ import print_function
import types


def convert_tuple(tpl):
    # do not check type of the argument here
    ntpl = []
    for i in tpl:
        if isinstance(i, str):
            ntpl.append(i.encode("utf-8"))
        elif isinstance(i, tuple) or isinstance(i, list):
            nt = convert_tuple(i)
            ntpl.append(nt)
        else:
            ntpl.append(i)
    if isinstance(tpl, tuple):
        rc = tuple(ntpl)
    else:
        # assume the argument was a list
        rc = ntpl
    return rc


def convert_kv(key, value):
    if isinstance(key, str):
        key = key.encode("utf-8")
    if isinstance(value, str):
        new_value = value.encode("utf-8")
    elif isinstance(value, tuple) or isinstance(value, list):
        new_value = convert_tuple(value)
    else:
        new_value = value
    return key, new_value


def convert_dict_u2a(d):
    nd = {}
    for k, v in d.items():
        nk, nv = convert_kv(k, v)
        if isinstance(nv, dict):
            nv = convert_dict_u2a(nv)  # call convert_dict_u2a recursively
        nd[nk] = nv
    return nd


if __name__ == "__main__":
    # dictionary in UTF format
    content = {
        u'cache': {
            u'ns': {
                u'type': u'pnfs',
                u'mnt': u'/pnfs/fnal.gov',
                u'id': None},
            u'arch': {
                u'type': u'enstore',
                u'id': None},
            u'en': {
                u'node': u'common',
                u'fsfn': u'common:/data/cache/1592/3/000100000000000000003638',
                u'mount': u'/data/cache',
                u'path': u'/data/cache/1592/3',
                u'id': u'000100000000000000003638',
                u'name': u'000100000000000000003638'}},
        u'file': {
            u'id': u'000100000000000000003638',
            u'name': u'/pnfs/fs/usr/data/moibenko/d2/LTO3/LTO3GS/d31.py',
            u'complete_crc': 2825635045,
            u'size': 6507},
        u'enstore': {
            u'deleted': u'no',
            u'vc': {
                u'file_family_width': 20,
                u'storage_group': u'ANM',
                u'library': u'LTO3GS',
                u'file_family': u'FF1',
                                u'wrapper': u'cpio_odc'},
            u'location_cookie': u'/data/cache/1592/3/000100000000000000003638',
            u'bfid': u'GCMS132070650300000'}}
    print("Before conversion", content)
    content = convert_dict_u2a(content)
    print("After conversion", content)
    import socket
    host = socket.gethostname()
    ip = socket.gethostbyname(host)

    # dictionary in ascii format
    ticket = {'lm': {'address': (ip, 7520)}, 'unique_id': '%s-1005321365-0-28872' % (host,), 'infile': '/pnfs/rip6/happy/mam/aci.py',
              'bfid': 'HAMS100471636100000', 'mover': 'MAM01.mover', 'at_the_top': 3, 'client_crc': 1, 'encp_daq': None,
              'encp': {'delayed_dismount': None, 'basepri': 1, 'adminpri': -1, 'curpri': 1, 'agetime': 0, 'delpri': 0},
              'fc': {'size': 1434, 'sanity_cookie': (1434, 657638438), 'bfid': 'HAMS100471636100000', 'location_cookie':
                     '0000_000000000_0000001', 'address': ('131.225.84.122', 7501), 'pnfsid': '00040000000000000040F2F8',
                     'pnfs_mapname': '/pnfs/rip6/volmap/alex/MM0001/0000_000000000_0000001', 'drive':
                     'happy:/dev/rmt/tps0d4n:0060112307', 'external_label': 'MM0001', 'deleted': 'no', 'pnfs_name0':
                     '/pnfs/rip6/happy/mam/aci.py', 'pnfsvid': '00040000000000000040F360', 'complete_crc': 657638438,
                     'status': ('ok', None)},
              'file_size': 1434, 'outfile': '/dev/null', 'volume': 'MM0001',
              'times': {'t0': 1005321364.951048, 'in_queue': 14.586493015289307, 'job_queued': 1005321365.7764519, 'lm_dequeued': 1005321380.363162},
              'version': 'v2_14  CVS $Revision$ ', 'retry': 0, 'work': 'read_from_hsm', 'callback_addr': ('131.225.13.132', 1463),
              'wrapper': {'minor': 5, 'inode': 0, 'fullname': '/dev/null', 'size_bytes': 1434, 'rmajor': 0, 'mode': 33268,
                          'pstat': (33204, 71365368, 5, 1, 6849, 5440, 1434, 1004716362, 1004716362, 1004716329), 'gname': 'hppc',
                          'sanity_size': 65536, 'machine': ('Linux', 'gccensrv2.fnal.gov', '2.2.17-14', '#1 Mon Feb 5 18:48:50 EST 2001', 'i686'),
                          'uname': 'moibenko', 'pnfsFilename': '/pnfs/rip6/happy/mam/aci.py', 'uid': 6849, 'gid': 5440, 'rminor': 0, 'major': 0},
              'vc': {'first_access': 1004716170.54972, 'sum_rd_err': 0, 'last_access': 1004741744.274856, 'media_type': '8MM',
                     'capacity_bytes': 5368709120, 'declared': 1004474612.7774431, 'remaining_bytes': 20105625600,
                     'wrapper': 'cpio_odc', 'external_label': 'MM0001', 'system_inhibit': ['none', 'none'],
                     'user_inhibit': ['none', 'none'], 'current_location': '0000_000000000_0000001', 'sum_rd_access': 7,
                     'volume_family': 'D0.alex.cpio_odc', 'address': ('131.225.84.122', 7502), 'file_family': 'alex',
                     'sum_wr_access': 2, 'library': 'mam', 'sum_wr_err': 1, 'non_del_files': 1, 'blocksize': 131072,
                     'eod_cookie': '0000_000000000_0000002', 'storage_group': 'D0', 'status': ('ok', None)},
              'status': (u'ok', None)
              }
    print("Before conversion", ticket)

    ticket = convert_dict_u2a(ticket)
    print("After conversion", ticket)
