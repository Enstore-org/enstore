#!/usr/bin/env python

from __future__ import print_function
import re
import pprint
import socket
import struct

"""
NetDirectory -- an object that helps network address look-up

Object of NetDirectory is initialized by a list of network addresses.
    A network address can be a name or an ip of the following format:

    "stkensrv0.fnal.gov" # exact name
    "stk*.fnal.gov"      # name with wildcard
    "*ensrv*.fnal.gov"   # name with multiple wildcards
                         # wildcard(s) can be any where in the name

    "131.225.84.10"      # exact ip
    "131.225.84.*"       # ip with wildcard
    "131.225.*"          # ip with wildcard
                         # wildcard in ip has to be the last component
                         # it does not allow "131.*.84.10"

Public methods:

add(node)
  -- add node(s) to the directory, where node can be a single address
     or a list of addresses

remove(node)
  -- remove node(s) from the directory, where node can be a single
     address (str) or a list of addresses

accept(node)
  -- check if node is accepted by the directory

show()
  -- dump internal


Example:

nd = NetDirectory(['stken*.fnal.gov', 'cdfen*.fnal.gov', '131.225.84.*'])

nd.accept('131.225.84.10')        # True
nd.accept('stkensrv0.fnal.gov')   # True
nd.accept('fsf-keyhole.fnal.gov') # False

"""


class NetDirectory:
    # To be initialized by a list of net addresses
    def __init__(self, list=[]):
        self._dir = {'host': [], 'ip': []}
        self.add(list)

    # _add_one(node): add node to the internal directory
    # This is for internal use only
    def _add_one(self, node):
        # tell if it is ip or node name
        if node[0].isdigit():
            k = 'ip'
        else:
            k = 'host'
        # append to the appopriate list
        if not node in self._dir[k]:
            self._dir[k].append(node)

    # _del_one(node): delete node from the internal directory
    # This is for internal use only
    def _del_one(self, node):
        # tell if it is ip or node name
        if node[0].isdigit():
            k = 'ip'
        else:
            k = 'host'
        # delete from the appropriate list
        for i in range(len(self._dir[k])):
            if node == self._dir[k][i]:
                del self._dir[k][i]
                return

    # add(node): public method to add node(s) to the directory
    # node could be a single node or a list of nodes
    def add(self, node):
        if isinstance(node, type([])):
            for i in node:
                self._add_one(i)
        else:
            self._add_one(node)
        # rebuild internal map and re.match object
        self._build_map()

    # remove(node): public method to delete node(s) from the directory
    # node could be a single node or a list of nodes
    def remove(self, node):
        if isinstance(node, type([])):
            for i in node:
                self._del_one(i)
        else:
            self._del_one(node)
        # rebuild internal map and re.match object
        self._build_map()

    # build_map(): build internal map and re.match object
    def _build_map(self):
        self.host_re_str, self.host_map = self._build_host_map()
        self.ip_map = self._build_ip_map()

    # _build_host_map(): build re.match object
    # internal use only
    def _build_host_map(self):
        # remember to take care of special characters
        s = ""
        for i in self._dir['host']:
            if s:
                s = s + '|' + i.replace('.', r'\.').replace('*', '.*')
            else:
                s = i.replace('.', r'\.').replace('*', '.*')
        return s, re.compile(s)

    #  _build_ip_map(): build a dictionary for ip look up
    def _build_ip_map(self):
        imap = {}
        for i in self._dir['ip']:
            s = i.split('.')
            p = []
            for j in s:
                if j.isdigit():
                    p.append(int(j))
                else:
                    p.append(j)
            n = len(p)
            if n > 0:
                if '*' not in imap:
                    if p[0] != '*':
                        if p[0] not in imap:
                            imap[p[0]] = {}
                    else:
                        imap = {'*': {}}
                        continue
            if n > 1:
                if '*' not in imap[p[0]]:
                    if p[1] != '*':
                        if p[1] not in imap[p[0]]:
                            imap[p[0]][p[1]] = {}
                    else:
                        imap[p[0]] = {'*': {}}
                        continue
            if n > 2:
                if '*' not in imap[p[0]][p[1]]:
                    if p[2] != '*':
                        if p[2] not in imap[p[0]][p[1]]:
                            imap[p[0]][p[1]][p[2]] = {}
                    else:
                        imap[p[0]][p[1]] = {'*': {}}
                        continue
            if n > 3:
                if '*' not in imap[p[0]][p[1]][p[2]]:
                    if p[3] != '*':
                        if p[3] not in imap[p[0]][p[1]][p[2]]:
                            imap[p[0]][p[1]][p[2]][p[3]] = {}
                    else:
                        imap[p[0]][p[1]][p[2]] = {'*': {}}
        return imap

    # show(): dump internal structures for debugging
    def show(self):
        print("_dir =")
        pprint.pprint(self._dir)
        print("host_re_str =", self.host_re_str)
        print("ip_map =")
        pprint.pprint(self.ip_map)
        print()

    # _check_ip(node): check if node is accepted by the ip_map
    def _check_ip(self, node):
        ip = struct.unpack("BBBB", socket.inet_aton(node))
        if len(ip) != 4:
            return False
        if '*' in self.ip_map:
            return True
        if ip[0] not in self.ip_map:
            return False
        if '*' in self.ip_map[ip[0]]:
            return True
        if ip[1] not in self.ip_map[ip[0]]:
            return False
        if '*' in self.ip_map[ip[0]][ip[1]]:
            return True
        if ip[2] not in self.ip_map[ip[0]][ip[1]]:
            return False
        if '*' in self.ip_map[ip[0]][ip[1]][ip[2]]:
            return True
        if ip[3] not in self.ip_map[ip[0]][ip[1]][ip[2]]:
            return False
        return True

    # _check_host(node): check if node is accepted by the match object
    def _check_host(self, node):
        return self.host_map.match(node) >= 0

    def accept(self, node):
        if node[0].isdigit():
            return self._check_ip(node)
        else:
            return self._check_host(node)


if __name__ == '__main__':
    # Test and examples
    allowed = ['cms*.fnal.gov', 'stk*.fnal.gov', 'cdf*.fnal.gov',
               'd0*.fnal.gov', '131.225.84.10', '131.225.13.*',
               '131.225.215.*', '131.225.164.*', '131.225.84.*']

    denied = ['fsf*.fnal.gov', '131.225.12.*']

    ad = NetDirectory(allowed)
    print("ad.show()")
    ad.show()

    dd = NetDirectory(denied)
    print("dd.show()")
    dd.show()

    print("ad.accept('131.225.84.10') =", ad.accept('131.225.84.10'))
    print("ad.accept('stkensrv0.fnal.gov') =", ad.accept('stkensrv0.fnal.gov'))

    print()
    print("ad.add(['sdss*.fnal.gov', '131.225.100.*'])")
    ad.add(['sdss*.fnal.gov', '131.225.100.*'])
    ad.show()

    print("ad.accept('131.225.100.10') =", ad.accept('131.225.100.10'))
    print("ad.accept('sdsssrv1.fnal.gov') =", ad.accept('sdsssrv1.fnal.gov'))
    print()
    print("ad.remove(['sdss*.fnal.gov', '131.225.100.*'])")
    ad.remove(['sdss*.fnal.gov', '131.225.100.*'])
    ad.show()
    print("ad.accept('131.225.100.10') =", ad.accept('131.225.100.10'))
    print("ad.accept('sdsssrv1.fnal.gov') =", ad.accept('sdsssrv1.fnal.gov'))
