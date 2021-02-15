#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################


import pg
import os
import sys
import pprint
import time

import Trace
import e_errors

# time2timestamp(t) -- convert time to "YYYY-MM-DD HH:MM:SS"


def time2timestamp(t):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))

# timestamp2time(ts) -- convert "YYYY-MM-DD HH:MM:SS" to time


def timestamp2time(s):
    return time.mktime(time.strptime(s, "%Y-%m-%d %H:%M:%S"))

#
# need this function to handle 8.2 and higher postgresql
#


def massage_tape_mounts_result(res):
    if 'oid_tape_mounts' not in res:
        res['oid_tape_mounts'] = res.get('oid(public.tape_mounts)')


class accDB:
    def __init__(self, host, dbname, port=None, user=None, logname='UNKNOWN'):
        self.logname = logname
        if port:
            self.db = pg.DB(host=host, dbname=dbname, port=port, user=user)
        else:
            self.db = pg.DB(host=host, dbname=dbname, user=user)
        self.pid = os.getpid()

    def close(self):
        self.db.close()

    def insert(self, table, dict):
        return self.db.insert(table, dict)

    def log_start_mount(self, node, volume, sg, mtype, logname, start):
        # This is the main table
        if not isinstance(start, type("")):
            start = time2timestamp(start)
        res = self.db.insert('tape_mounts', {
            'node': node,
            'volume': volume,
            'storage_group': sg,
            'reads': 0,
            'writes': 0,
            'type': mtype,
            'logname': logname,
            'start': start,
            'state': 'm'})
        #
        # handle 8.2 vs 8.{3,4} difference (the latter does not have oid_tape_mounts key)
        #
        massage_tape_mounts_result(res)
        # remember the start part
        try:
            res2 = self.db.insert('tape_mounts_tmp', {
                'volume': volume,
                'state': 'm',
                'id': res['oid_tape_mounts']})
        except BaseException:
            #
            # no need ot report error here. Failure to insert is part of the workflow
            #
            try:
                q = "select oid as oid_tape_mounts_tmp, "\
                    "volume, state from tape_mounts_tmp "\
                    "where volume = '%s' and state = 'm'" % (volume,)
                res2 = self.db.query(q).dictresult()[0]

                res2 = self.db.query("update tape_mounts_tmp set id=%d where oid=%d" % (res['oid_tape_mounts'],
                                                                                        res2['oid_tape_mounts_tmp'],))
            except BaseException:
                Trace.log(e_errors.ERROR, "%s: %s" %
                          (str(sys.exc_info()[0]),
                           str(sys.exc_info()[1])))
                return

    def log_finish_mount(self, node, volume, finish, state='M'):
        if not isinstance(finish, type("")):
            finish = time2timestamp(finish)
        try:
            q = "select oid as oid_tape_mounts_tmp, "\
                "volume, state, id from tape_mounts_tmp "\
                "where volume = '%s' and state = 'm';" % (volume)
            res = self.db.query(q).dictresult()[0]
            try:
                #
                # this works in postgresql 8.2
                #
                res2 = self.db.update('tape_mounts', {
                    'oid_tape_mounts': res['id'],
                    'finish': finish,
                    'state': state})

                res2 = self.db.delete('tape_mounts_tmp', {
                    'oid_tape_mounts_tmp': res['oid_tape_mounts_tmp']})
            except BaseException:
                #
                # this works in  postgresql 8.{3,4}
                #
                q = "update tape_mounts set finish='%s', state='%s' where tape_mounts.oid=%d" % (
                    finish, state, res['id'],)
                res2 = self.db.query(q)
                res2 = self.db.query(
                    "delete from tape_mounts_tmp where tape_mounts_tmp.oid=%d" %
                    (res['oid_tape_mounts_tmp'],))

        except BaseException:
            Trace.log(e_errors.ERROR, "%s: %s" %
                      (str(sys.exc_info()[0]),
                       str(sys.exc_info()[1])))
            return

    def log_start_dismount(self, node, volume, sg, reads,
                           writes, mtype, logname, start):
        if not isinstance(start, type("")):
            start = time2timestamp(start)

        # This is the main table
        res = self.db.insert('tape_mounts', {
            'node': node,
            'volume': volume,
            'storage_group': sg,
            'reads': reads,
            'writes': writes,
            'type': mtype,
            'logname': logname,
            'start': start,
            'state': 'd'})
        #
        # handle 8.2 vs 8.{3,4} difference (the latter does not have oid_tape_mounts key)
        #
        massage_tape_mounts_result(res)
        # remember the start part
        try:
            res2 = self.db.insert('tape_mounts_tmp', {
                'volume': volume,
                'state': 'd',
                'id': res['oid_tape_mounts']})
        except BaseException:
            #
            # no need ot report error here. Failure to insert is part of the workflow
            #
            try:
                q = "select oid as oid_tape_mounts_tmp, "\
                    "volume, state from tape_mounts_tmp "\
                    "where volume = '%s' and state = 'd';" % (volume)
                res2 = self.db.query(q).dictresult()[0]
                res2 = self.db.query("update tape_mounts_tmp set id=%d where oid=%d" % (res['oid_tape_mounts'],
                                                                                        res2['oid_tape_mounts_tmp'],))
            except BaseException:
                Trace.log(e_errors.ERROR, "%s: %s" %
                          (str(sys.exc_info()[0]),
                           str(sys.exc_info()[1])))
                return

    def log_finish_dismount(self, node, volume, finish, state='D'):
        if not isinstance(finish, type("")):
            finish = time2timestamp(finish)
        try:
            q = "select oid as oid_tape_mounts_tmp, "\
                "volume, state, id from tape_mounts_tmp "\
                "where volume = '%s' and state = 'd';" % (volume)
            res = self.db.query(q).dictresult()[0]
            try:
                #
                #  this works in postgresql 8.2
                #
                res2 = self.db.update('tape_mounts', {
                    'oid_tape_mounts': res['id'],
                    'finish': finish,
                    'state': state})
                res2 = self.db.delete('tape_mounts_tmp', {
                    'oid_tape_mounts_tmp': res['oid_tape_mounts_tmp']})
            except BaseException:
                #
                # this works in  postgresql 8.{3,4}
                #
                q = "update tape_mounts set finish='%s', state='%s' where tape_mounts.oid=%d" % (
                    finish, state, res['id'],)
                res2 = self.db.query(q)
                res2 = self.db.query(
                    "delete from tape_mounts_tmp where tape_mounts_tmp.oid=%d" %
                    (res['oid_tape_mounts_tmp'],))
        except BaseException:
            Trace.log(e_errors.ERROR, "%s: %s" %
                      (str(sys.exc_info()[0]),
                       str(sys.exc_info()[1])))
            return

    def log_encp_xfer2(self, xfer):
        if not isinstance(xfer['date'], type("")):
            xfer['date'] = time2timestamp(xfer['date'])
        self.insert('encp_xfer', xfer)

    def log_encp_xfer(self, date, node, pid, username, src, dst,
                      size, volume, network_rate, drive_rate, disk_rate,
                      overall_rate, transfer_rate, mover,
                      drive_id, drive_sn, elapsed, media_changer,
                      mover_interface, driver, storage_group, encp_ip,
                      encp_id, rw, encp_version=None, file_family=None,
                      wrapper=None, library=None):

        if not isinstance(date, type("")):
            date = time2timestamp(date)

        xfer = {'date'		: date,
                'node'		: node,
                'pid'		: pid,
                'username'	: username,
                'src'		: src,
                'dst'		: dst,
                'size'		: size,
                'volume'	: volume,
                'overall_rate'	: overall_rate,
                'network_rate'	: network_rate,
                'drive_rate'	: drive_rate,
                'disk_rate'	: disk_rate,
                'transfer_rate'	: transfer_rate,
                'mover'		: mover,
                'drive_id'	: drive_id,
                'drive_sn'	: drive_sn,
                'elapsed'	: elapsed,
                'media_changer'	: media_changer,
                'mover_interface': mover_interface,
                'driver'	: driver,
                'storage_group'	: storage_group,
                'encp_ip'	: encp_ip,
                'encp_id'	: encp_id,
                'rw'		: rw}

        if encp_version:
            xfer['encp_version'] = encp_version
        if file_family:
            xfer['file_family'] = file_family
        if wrapper:
            xfer['wrapper'] = wrapper
        if library:
            xfer['library'] = library

        self.insert('encp_xfer', xfer)

    def log_encp_error(self, date, node, pid, username, src, dst,
                       size, storage_group, encp_id, version, e_type, error,
                       file_family=None, wrapper=None, mover=None,
                       drive_id=None, drive_sn=None, rw=None,
                       volume=None, library=None):
        if not isinstance(date, type("")):
            date = time2timestamp(date)

        # take care of error being None
        if error is None:
            error = ''
        en_error = {
            'date'		: date,
            'node'		: node,
            'pid'		: pid,
            'username'	: username,
            'version'	: version,
            'type'		: e_type,
            'error'		: error}

        if src:
            en_error['src'] = src
        if dst:
            en_error['dst'] = dst
        if size:
            en_error['size'] = size
        else:
            en_error['size'] = -1
        if encp_id:
            en_error['encp_id'] = encp_id
        if storage_group:
            en_error['storage_group'] = storage_group
        if file_family:
            en_error['file_family'] = file_family
        if wrapper:
            en_error['wrapper'] = wrapper
        if mover:
            en_error['mover'] = mover
        if drive_id:
            en_error['drive_id'] = drive_id
        if drive_sn:
            en_error['drive_sn'] = drive_sn
        if rw:
            en_error['rw'] = rw
        if volume:
            en_error['volume'] = volume
        if library:
            en_error['library'] = library

        self.insert('encp_error', en_error)

    # This pair of function need a unique tag to work
    # The calling function should provide such a tag
    # A simple key is host_ip.pid.time

    def log_start_event(self, tag, name, node, username, start):
        if not isinstance(start, type("")):
            start = time2timestamp(start)

        res = self.db.insert('event', {
            'tag': tag,
            'name': name,
            'node': node,
            'username': username,
            'start': start})

    def log_finish_event(self, tag, finish, status=0, comment=None):
        if not isinstance(finish, type("")):
            finish = time2timestamp(finish)

        if comment:
            commentstr = ", comment = '%s'" % (comment)
        else:
            commentstr = ""

        qs = "update event set finish = '%s', "\
             "status = %d%s where tag = '%s' and "\
             "finish is null;" % (finish, status, commentstr, tag)
        res = self.db.query(qs)
