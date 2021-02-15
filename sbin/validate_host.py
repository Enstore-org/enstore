#!/usr/bin/env python

###############################################################################
# $Id
#
# validate host including its alaises
#
###############################################################################
#
import socket
import os
import Interfaces


def this_alias(host):
    ip = socket.gethostbyname(host)
    interfaces_list = Interfaces.interfacesGet()
    #print interfaces_list
    for interface in interfaces_list.keys():
        if interfaces_list[interface]['ip'] == ip:
            rc = [host] + [] + [ip]
            return rc
    else:
        return None


def this_host(use_alias=0):
    rtn = socket.gethostbyname_ex(socket.getfqdn())

    if use_alias and (not os.environ['ENSTORE_CONFIG_HOST'] in rtn):
        # try alias
        rc = this_alias(os.environ['ENSTORE_CONFIG_HOST'])
        if rc:
            return rc
        else:
            return [rtn[0]] + rtn[1] + rtn[2]

    else:
        return [rtn[0]] + rtn[1] + rtn[2]


def is_on_host(host, use_alias=0):
    if host in this_host(use_alias):
        return 1

    return 0


if __name__ == '__main__':
    import sys
    if is_on_host(sys.argv[1], use_alias=1):
        sys.exit(1)
    else:
        sys.exit(0)
