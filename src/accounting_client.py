#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system import
from future.utils import raise_
import os
import sys
import pprint
import pwd
import socket
import time

# enstore import
import generic_client
import option
import enstore_constants

MY_NAME = enstore_constants.ACCOUNTING_CLIENT  # "accounting_client"
MY_SERVER = enstore_constants.ACCOUNTING_SERVER  # "accounting_server"
RCV_TIMEOUT = 10
RCV_TRIES = 1

# get a unique tag: <host_ip>+<pid>+<timestamp>


def unique_tag():
    return "%s+%d+%f" % (socket.gethostbyname(socket.gethostname()),
                         os.getpid(), time.time())


class accClient(generic_client.GenericClient):
    def __init__(self, csc, logname='UNKNOWN',
                 flags=0, logc=None, alarmc=None,
                 rcv_timeout=RCV_TIMEOUT, rcv_tries=RCV_TRIES,
                 server_address=None):
        self.logname = logname
        self.node = os.uname()[1]
        self.pid = os.getpid()
        generic_client.GenericClient.__init__(
            self, csc, MY_NAME, server_address=server_address,
            flags=flags, logc=logc, alarmc=alarmc,
            rcv_timeout=rcv_timeout, rcv_tries=rcv_tries,
            server_name=MY_SERVER)
        try:
            self.uid = pwd.getpwuid(os.getuid())[0]
        except BaseException:
            self.uid = "unknown"
        self.rcv_timeout = rcv_timeout
        self.rcv_tries = rcv_tries
        #self.server_address = self.get_server_address(MY_SERVER, self.rcv_timeout, self.rcv_tries)

    # send_no_wait
    def send2(self, ticket):
        if not self.server_address:
            return
        self.u.send_no_wait(ticket, self.server_address)

    # generic test
    def hello(self):
        if not self.server_address:
            return
        ticket = {'work': 'hello'}
        return self.send(ticket, 30, 1)

    # generic test for send_no_wait
    def hello2(self):
        if not self.server_address:
            return
        ticket = {'work': 'hello'}
        return self.send2(ticket)

    def debug(self, level=0):
        ticket = {
            'work': 'debugging',
            'level': level}
        self.send2(ticket)

    def debug_on(self):
        self.debug(1)

    def debug_off(self):
        self.debug(0)

    def log_start_mount(self, volume, type, sg=None, start=None):
        if not self.server_address:
            return
        if not start:
            start = time.time()
        ticket = {
            'work': 'log_start_mount',
            'node': self.node,
            'volume': volume,
            'storage_group': sg,
            'type': type,
            'logname': self.logname,
            'start': start}
        self.send2(ticket)

    def log_finish_mount(self, volume, finish=None, state='M'):
        if not self.server_address:
            return
        if not finish:
            finish = time.time()
        ticket = {
            'work': 'log_finish_mount',
            'node': self.node,
            'volume': volume,
            'finish': finish,
            'state': state}
        self.send2(ticket)

    def log_finish_mount_err(self, volume, finish=None, state='E'):
        if not self.server_address:
            return
        self.log_finish_mount(volume, finish, state)

    def log_start_dismount(self, volume, type, sg=None,
                           reads=0, writes=0, start=None):
        if not self.server_address:
            return
        if not start:
            start = time.time()
        ticket = {
            'work': 'log_start_dismount',
            'node': self.node,
            'volume': volume,
            'storage_group': sg,
            'reads': reads,
            'writes': writes,
            'type': type,
            'logname': self.logname,
            'start': start}
        self.send2(ticket)

    def log_finish_dismount(self, volume, finish=None, state='D'):
        if not self.server_address:
            return
        if not finish:
            finish = time.time()
        ticket = {
            'work': 'log_finish_dismount',
            'node': self.node,
            'volume': volume,
            'finish': finish,
            'state': state}
        self.send2(ticket)

    def log_finish_dismount_err(self, volume, finish=None, state='F'):
        if not self.server_address:
            return
        self.log_finish_dismount(volume, finish, state)

    def log_encp_xfer(self, date, src, dst, size, volume,
                      network_rate, drive_rate, disk_rate, overall_rate,
                      transfer_rate, mover, drive_id, drive_sn,
                      elapsed, media_changer, mover_interface, driver,
                      storage_group, encp_ip, encp_id, rw, encp_version='unknown',
                      file_family=None, wrapper=None, library=None):

        if not self.server_address:
            return

        if not date:
            date = time.time()

        ticket = {
            'work': 'log_encp_xfer',
            'date': date,
            'node': self.node,
            'pid': self.pid,
            'username': self.uid,
            'src': src,
            'dst': dst,
            'size': size,
            'volume': volume,
            'network_rate': network_rate,
            'drive_rate': drive_rate,
            'disk_rate': disk_rate,
            'overall_rate': overall_rate,
            'transfer_rate': transfer_rate,
            'mover': mover,
            'drive_id': drive_id,
            'drive_sn': drive_sn,
            'elapsed': elapsed,
            'media_changer': media_changer,
            'mover_interface': mover_interface,
            'driver': driver,
            'storage_group': storage_group,
            'encp_ip': encp_ip,
            'encp_id': encp_id,
            'rw': rw,
            'encp_version': encp_version,
            'file_family': file_family,
            'wrapper': wrapper,
            'library': library,
        }

        self.send2(ticket)

    def log_encp_error(self, src, dst, size, storage_group, encp_id, version,
                       type, error, node=None, date=None, file_family=None,
                       wrapper=None, mover=None, drive_id=None, drive_sn=None,
                       rw=None, volume=None, library=None):

        if not self.server_address:
            return

        if not date:
            date = time.time()

        if not node:
            node = self.node

        ticket = {
            'work': 'log_encp_error',
            'date': date,
            'node': node,
            'pid': self.pid,
            'username': self.uid,
            'src': src,
            'dst': dst,
            'size': size,
            'storage_group': storage_group,
            'encp_id': encp_id,
            'version': version,
            'type': type,
            'error': error,
            'file_family': file_family,
            'wrapper': wrapper,
            'mover': mover,
            'drive_id': drive_id,
            'drive_sn': drive_sn,
            'rw': rw,
            'volume': volume,
            'library': library,
        }
        self.send2(ticket)

    def log_start_event(self, name):
        tag = unique_tag()
        ticket = {
            'work': 'log_start_event',
            'tag': tag,
            'name': name,
            'node': self.node,
            'username': self.uid,
            'start': time.time()}
        self.send2(ticket)
        return tag

    def log_finish_event(self, tag, status=0, comment=None):
        ticket = {
            'work': 'log_finish_event',
            'tag': tag,
            'finish': time.time(),
            'status': status}

        if comment:
            ticket['comment'] = comment

        self.send2(ticket)

    def log_drive_info(self, ticket):
        ticket['work'] = 'log_drive_info'
        if 'date' not in ticket:
            ticket['date'] = time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.localtime(
                    time.time()))
        for key in 'date', 'drive_id', 'drive_sn', 'volume', \
                'drive_rate', 'rw', 'write_error_count', 'read_error_count', \
                'bfid':
            if key not in ticket:
                raise_(KeyError, "Required key \"" + key +
                       "\" is missing in input dictionary")
        self.send2(ticket)


if __name__ == '__main__':
    intf = option.Interface()
    ac = accClient((intf.config_host, intf.config_port))
    if sys.argv[1] == 'hello':
        pprint.pprint(ac.hello())
    elif sys.argv[1] == 'quit':
        pprint.pprint(ac.quit())
    elif sys.argv[1] == 'hello2':
        ac.hello2()
    elif sys.argv[1] == 'drive_info':
        ac.log_drive_info({'drive_id': 'TEST',
                           'drive_sn': 'TEST',
                           'volume': 'XXXXXX',
                           'drive_rate': 0.0,
                           'rw': 'w',
                           'write_error_count': 0,
                           'read_error_count': 0})
