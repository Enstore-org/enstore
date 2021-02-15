#!/usr/bin/env python
'''
fileinfo.py -- get enstore file info of a file specified in /pnfs path
as well as the volume info associated with it.
'''
from __future__ import print_function

import option
import file_clerk_client
import volume_clerk_client
import sys
import os
import string
import pprint
import e_errors

fcc = None
vcc = None

# layer_file(p, n): get layer n file path from the path p


def layer_file(p, n):
    d, f = os.path.split(p)
    return os.path.join(d, '.(use)(%d)(%s)' % (n, f))


def layer2crc(s):
    crc = None
    level = 0

    token = string.split(s, ';')
    for i in token:
        if i[:3] == ':c=':
            cc = string.split(i, ':')
            if len(cc) < 3:
                return None
            l1 = int(string.split(cc[1], '=')[1])
            c1 = long(cc[2], 16)
            if level == 0 or l1 < level:
                crc = c1
                level = l1
    return crc


def file_info(path):
    if not os.access(path, os.R_OK):
        return None
    # try to get bfid
    try:
        bfid = string.strip(open(layer_file(path, 1)).readline())
    except BaseException:
        return None

    if len(bfid) < 10:
        # poke layer 2
        l2 = open(layer_file(path, 2)).readlines()
        if len(l2) < 2:
            return None
        crc = layer2crc(string.strip(l2[1]))
        if crc:
            return {'file_info': {}, 'volume_info': {}, 'l2crc': crc}
        return None

    fi = fcc.bfid_info(bfid)
    if fi['status'][0] != e_errors.OK:
        return None
    vi = vcc.inquire_vol(fi['external_label'])
    if vi['status'][0] != e_errors.OK:
        return None

    return {'file_info': fi, 'volume_info': vi}


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('None')
        sys.exit(0)
    intf = option.Interface()
    fcc = file_clerk_client.FileClient((intf.config_host, intf.config_port))
    vcc = volume_clerk_client.VolumeClerkClient(fcc.csc)

    result = {}
    for i in sys.argv[1:]:
        out = file_info(i)
        result[i] = out
    pprint.pprint(result)
