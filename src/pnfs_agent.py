#!/usr/bin/env python

###############################################################################
#
# $Id$
# $Author$
#
# pnfs agent is server sitting on a host that has pnfs mounted
# encp talks to this agent via pnfs_agent_client
#
###############################################################################

# system imports
from __future__ import print_function
import sys
import os
import time
import errno
import string
import socket
import stat
import types
import traceback
import threading

# enstore imports
import hostaddr
import dispatching_worker
import generic_server
import Trace
import e_errors
import enstore_constants
import monitored_server
import event_relay_messages
import pnfs
#import atomic
import enstore_functions2
import file_utils
import find_pnfs_file
import udp_server

MY_NAME = enstore_constants.PNFS_AGENT  # "pnfs_agent"
MAX_THREADS = 50

default_pinfo = {"inode": 0,
                 # "gid" : None,
                 # "gname" : None,
                 # "uid" : None,
                 # "uname" : None,
                 "major": None,
                 "minor": None,
                 "rmajor": None,
                 "rminor": None,
                 "mode": None,
                 "pnfsFilename": None,
                 }

fullpath = enstore_functions2.fullpath


class PnfsAgent(dispatching_worker.DispatchingWorker,
                generic_server.GenericServer):

    def __init__(self, csc, name=MY_NAME):
        generic_server.GenericServer.__init__(self, csc, name,
                                              function=self.handle_er_msg)
        self.name = name
        self.shortname = name
        self.keys = self.csc.get(self.name)
        dispatching_worker.DispatchingWorker.__init__(self, (self.keys['hostip'], self.keys['port']),
                                                      use_raw=1)
        self.conf = self.csc.get(name)
        self.max_threads = self.conf.get('max_threads', MAX_THREADS)

        # self.set_error_handler(self.handle_error)
        self.alive_interval = monitored_server.get_alive_interval(self.csc,
                                                                  MY_NAME,
                                                                  self.keys)

        # If the pnfs_agent is started without /pnfs/fs mounted, give
        # error and exit.
        admin_mp_list = pnfs.get_enstore_admin_mount_point()
        if len(admin_mp_list) == 0:
            message = "No PNFS admin mount point found.  Exiting."
            Trace.log(e_errors.ERROR, message)
            sys.stderr.write("%s\n" % (message,))
            sys.exit(1)

    """
    def handle_error(self, exc, msg, tb):
        x = tb
        Trace.log(e_errors.ERROR, "handle pnfs agent error %s %s"%(exc, msg))
        Trace.trace(10, "%s " %(self.current_work_ticket, ))
        if self.current_work_ticket:
            try:
                Trace.trace(10, "handle error: calling transfer failed, str(msg)=%s"%(str(msg),))
                self.transfer_failed(exc, msg)
            except:
                pass
    """

    # show_state -- show internal configuration values
    def show_state(self, ticket):
        ticket['state'] = {}
        for i in self.__dict__.keys():
            ticket['state'][i] = repr(self.__dict__[i])
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    def get_pnfsstat(self, ticket):
        filename = pnfs.get_enstore_fs_path(ticket['filename'])
        p = pnfs.Pnfs(filename)
        try:
            ticket['statinfo'] = p.get_stat()
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['statinfo'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except OSError as msg:
            ticket['statinfo'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    def get_stat(self, ticket):
        filename = pnfs.get_enstore_fs_path(ticket['filename'])
        try:
            ticket['statinfo'] = tuple(os.stat(filename))
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['statinfo'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except OSError as msg:
            ticket['statinfo'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    def touch(self, ticket):
        filename = pnfs.get_enstore_fs_path(ticket['filename'])
        mode = ticket.get('mode', None)
        p = pnfs.Pnfs()

        # Address the issue of ownership.
        # file_utils.euid_lock.acquire() should work without locking
        if ticket['gid'] and ticket['gid'] >= 0 and os.getgid() == 0:
            os.setegid(ticket['gid'])
        if ticket['uid'] and ticket['uid'] >= 0 and os.getuid() == 0:
            os.seteuid(ticket['uid'])

        try:
            if mode:
                p.touch(filename)  # , mode)
            else:
                p.touch(filename)
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
            Trace.trace(5, "OSError: %s" % (ticket,))
        except IOError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))

        # Address the issue of ownership.
        os.seteuid(0)
        os.setegid(0)
        # file_utils.euid_lock.release() should work without locking

        self.reply_to_caller(ticket)
        return

    def creat(self, ticket):
        filename = pnfs.get_enstore_fs_path(ticket['filename'])
        mode = ticket.get('mode', None)
        Trace.trace(5, "creat 0_1")
        p = pnfs.Pnfs()
        Trace.trace(5, "creat 0_2")

        # Address the issue of ownership.
        # file_utils.euid_lock.acquire() should work without locking
        try:
            if mode:
                Trace.trace(5, "creat 1")
                p.creat(filename, mode)
            else:
                Trace.trace(5, "creat 2")
                p.creat(filename)
            if ticket['gid'] and ticket['gid'] >= 0 and ticket['uid'] and ticket['uid'] >= 0:
                os.chown(filename, ticket['gid'], ticket['uid'])
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            Trace.trace(5, "creat 3")
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
            Trace.trace(5, "OSError: %s" % (ticket,))
        except IOError as msg:
            Trace.trace(5, "creat 4")
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
            # file_utils.euid_lock.release() should work without locking
        Trace.trace(5, "creat 5")

        self.reply_to_caller(ticket)
        return

    def get_file_stat(self, ticket):
        filename = pnfs.get_enstore_fs_path(ticket['filename'])
        if (os.path.exists(filename)):
            if (pnfs.is_pnfs_path(filename)):
                tmp = os.stat(filename)
                filesize = tmp[stat.ST_SIZE]
                pin = pnfs.Pnfs(filename)
                bfid = pin.get_bit_file_id()
                ticket['bfid'] = bfid
                pinfo = {}
                for k in ['pnfsFilename', 'gid', 'gname', 'uid', 'uname',
                          'major', 'minor', 'rmajor', 'rminor',
                          'mode', 'pstat', 'inode']:
                    try:
                        pinfo[k] = getattr(pin, k)
                    except AttributeError:
                        if k in default_pinfo:
                            pinfo[k] = default_pinfo[k]
                ticket['pinfo'] = pinfo
                if (filesize == 1):
                    pin.pstatinfo()
                    pin.get_file_size()
                    filesize = pin.file_size
                for i in range(0, len(tmp)):
                    ticket['statinfo'].append(tmp[i])
                ticket['statinfo'][stat.ST_SIZE] = filesize
                ticket['status'] = (e_errors.OK, None)
            else:
                msg = "file %s exists but not PNFS file" % filename
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)
        else:
            msg = "file %s does not exist on PNFS" % filename
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)
        self.reply_to_caller(ticket)
        return

    def get_library(self, ticket):
        dirname = pnfs.get_enstore_fs_path(ticket['dirname'])
        if (os.path.exists(dirname)):
            if (os.path.isdir(dirname)):
                t = pnfs.Tag(dirname)
                ticket['library'] = t.get_library().split(",")[0]
                ticket['status'] = (e_errors.OK, None)
            else:
                msg = "%s not a directory" % dirname
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)

        else:
            msg = "directory %s does not exist" % dirname
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)

        self.reply_to_caller(ticket)
        return

    def set_library(self, ticket):
        dirname = ticket['dirname']
        if (os.path.exists(dirname)):
            if (os.path.isdir(dirname)):
                t = pnfs.Tag(dirname)
                t.set_library(ticket['library'], dirname)
                ticket['status'] = (e_errors.OK, None)
            else:
                msg = "%s not a directory" % dirname
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)

        else:
            msg = "directory %s does not exist" % dirname
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)

        self.reply_to_caller(ticket)
        return

    def get_file_family(self, ticket):
        dirname = pnfs.get_enstore_fs_path(ticket['dirname'])
        if (os.path.exists(dirname)):
            if (os.path.isdir(dirname)):
                t = pnfs.Tag(dirname)
                ticket['file_family'] = t.get_file_family()
                ticket['status'] = (e_errors.OK, None)
            else:
                msg = "%s not a directory" % dirname
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)

        else:
            msg = "directory %s does not exist" % dirname
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)
        self.reply_to_caller(ticket)
        return

    def set_file_family(self, ticket):
        dirname = ticket['dirname']
        if (os.path.exists(dirname)):
            if (os.path.isdir(dirname)):
                t = pnfs.Tag(dirname)
                t.set_file_family(ticket['file_family'], dirname)
                ticket['status'] = (e_errors.OK, None)
            else:
                msg = "%s not a directory" % dirname
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)

        else:
            msg = "directory %s does not exist" % dirname
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)
        self.reply_to_caller(ticket)
        return

    def get_file_family_width(self, ticket):
        dirname = pnfs.get_enstore_fs_path(ticket['dirname'])
        if (os.path.exists(dirname)):
            if (os.path.isdir(dirname)):
                t = pnfs.Tag(dirname)
                ticket['file_family_width'] = t.get_file_family_width()
                ticket['status'] = (e_errors.OK, None)
            else:
                msg = "%s not a directory" % dirname
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)

        else:
            msg = "directory %s does not exist" % dirname
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)
        self.reply_to_caller(ticket)
        return

    def set_file_family_width(self, ticket):
        dirname = ticket['dirname']
        if (os.path.exists(dirname)):
            if (os.path.isdir(dirname)):
                t = pnfs.Tag(dirname)
                t.set_file_family_width(ticket['file_family_width'], dirname)
                ticket['status'] = (e_errors.OK, None)
            else:
                msg = "%s not a directory" % dirname
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)

        else:
            msg = "directory %s does not exist" % dirname
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)
        self.reply_to_caller(ticket)
        return

    def get_file_family_wrapper(self, ticket):
        dirname = pnfs.get_enstore_fs_path(ticket['dirname'])
        if (os.path.exists(dirname)):
            if (os.path.isdir(dirname)):
                t = pnfs.Tag(dirname)
                ticket['file_family_wrapper'] = t.get_file_family_wrapper()
                ticket['status'] = (e_errors.OK, None)
            else:
                msg = "%s not a directory" % dirname
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)

        else:
            msg = "directory %s does not exist" % dirname
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)
        self.reply_to_caller(ticket)
        return

    def set_file_family_wrapper(self, ticket):
        dirname = ticket['dirname']
        if (os.path.exists(dirname)):
            if (os.path.isdir(dirname)):
                t = pnfs.Tag(dirname)
                t.set_file_family_wrapper(
                    ticket['file_family_wrapper'], dirname)
                ticket['status'] = (e_errors.OK, None)
            else:
                msg = "%s not a directory" % dirname
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)

        else:
            msg = "directory %s does not exist" % dirname
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)
        self.reply_to_caller(ticket)
        return

    def get_storage_group(self, ticket):
        dirname = pnfs.get_enstore_fs_path(ticket['dirname'])
        if (os.path.exists(dirname)):
            if (os.path.isdir(dirname)):
                t = pnfs.Tag(dirname)
                ticket['storage_group'] = t.get_storage_group()
                ticket['status'] = (e_errors.OK, None)
            else:
                msg = "%s not a directory" % dirname
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)

        else:
            msg = "directory %s does not exist" % dirname
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)
        self.reply_to_caller(ticket)
        return

    def set_storage_group(self, ticket):
        dirname = ticket['dirname']
        if (os.path.exists(dirname)):
            if (os.path.isdir(dirname)):
                t = pnfs.Tag(dirname)
                t.set_storage_group(ticket['storage_group'], dirname)
                ticket['status'] = (e_errors.OK, None)
            else:
                msg = "%s not a directory" % dirname
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.DOESNOTEXIST, None)

        else:
            msg = "directory %s does not exist" % dirname
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.DOESNOTEXIST, None)
        self.reply_to_caller(ticket)
        return

    def get_path(self, ticket):
        pnfs_id = ticket['pnfs_id']
        shortcut = ticket['shortcut']
        dirname = pnfs.get_enstore_fs_path(ticket['dirname'])
        p = pnfs.Pnfs()
        try:
            ticket['path'] = p.get_path(pnfs_id, dirname, shortcut)
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    def e_access(self, ticket):
        try:
            path = pnfs.get_enstore_fs_path(ticket['path'])
        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
            self.reply_to_caller(ticket)
            return
        mode = ticket['mode']
        rc = file_utils.e_access(path, mode)
        Trace.trace(
            10, 'e_access for file %s mode %s rc=%s' %
            (path, mode, rc,))
        ticket['rc'] = rc
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    def set_bit_file_id(self, ticket):
        bfid = ticket['bfid']
        fname = pnfs.get_enstore_fs_path(ticket['fname'])
        p = pnfs.Pnfs(fname)
        p.set_bit_file_id(bfid)
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    def get_bit_file_id(self, ticket):
        fname = pnfs.get_enstore_fs_path(ticket['fname'])
        p = pnfs.Pnfs(fname)
        ticket['bfid'] = p.get_bit_file_id()
        ticket['status'] = (e_errors.OK, None)
        Trace.trace(10, 'get_bit_file_id %s %s' % (fname, ticket['bfid'],))
        self.reply_to_caller(ticket)
        return

    def get_id(self, ticket):
        fname = pnfs.get_enstore_fs_path(ticket['fname'])
        try:
            p = pnfs.Pnfs()
            ticket['file_id'] = p.get_id(fname)
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['file_id'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['file_id'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        Trace.trace(10, 'get_id %s %s' % (fname, ticket['file_id'],))
        return

    def get_nameof(self, ticket):
        pnfsid = ticket['pnfsid']
        try:
            p = pnfs.Pnfs()
            ticket['nameof'] = p.get_nameof(pnfsid)
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['nameof'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['nameof'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        Trace.trace(10, 'get_nameof %s %s' % (pnfsid, ticket['pnfsid'],))
        return

    def get_parent_id(self, ticket):
        pnfsid = ticket['pnfsid']
        try:
            p = pnfs.Pnfs()
            admin_mp_list = pnfs.get_enstore_admin_mount_point()
            parent_list = []
            for mp in admin_mp_list:
                try:
                    parent_id = p.get_parent(pnfsid, mp)
                    parent_list.append(parent_id)
                except BaseException:
                    pass
            if len(parent_list) == 0:
                raise OSError(errno.ENOENT,
                              "%s: %s" % (errno.errorcode[errno.ENOENT],
                                          pnfs.parent_file("/pnfs/fs/usr/",
                                                           ".(parent)(%s)" % (pnfsid,))))
            elif len(parent_list) > 1:
                raise OSError(errno.EEXIST, "To many matches: %s" % (pnfsid,))

            else:  # len(parent_list) == 1
                ticket['parent_id'] = parent_list[0]
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['parent_id'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['parent_id'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        Trace.trace(10, 'get_parent_id pnfs %s' % (ticket['parent_id'],))
        return

    def readlayer(self, ticket):
        #print "ticket['fname']:", ticket['fname']
        try:
            fname = pnfs.get_enstore_fs_path(ticket['fname'])
        except OSError as msg:
            ticket['status'] = (e_errors.OSERROR, str(msg))
            self.reply_to_caller(ticket)
            print("No FN", ticket)
            return
        except KeyError as msg:
            ticket['status'] = (e_errors.KEYERROR, str(msg))
            self.reply_to_caller(ticket)
            print("No FN", ticket)
            return

        layer = ticket['layer']
        try:
            p = pnfs.Pnfs(fname)
            ticket['layer_info'] = p.readlayer(layer, fname)
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['layer_info'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['layer_info'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        Trace.trace(10, 'get_layer pnfs %s' % (ticket['layer_info'],))
        return

    def writelayer(self, ticket):
        try:
            fname = pnfs.get_enstore_fs_path(ticket['fname'])
        except OSError as msg:
            ticket['status'] = (e_errors.OSERROR, str(msg))
            self.reply_to_caller(ticket)
            return

        layer = ticket['layer']
        value = ticket['value']
        try:
            p = pnfs.Pnfs(fname)
            p.writelayer(layer, value, fname)
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    def get_xreference(self, ticket):
        fname = pnfs.get_enstore_fs_path(ticket['fname'])
        try:
            p = pnfs.Pnfs(fname)
            ticket['xref'] = p.get_xreference()
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['xref'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['xref'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        # Trace.log(e_errors.INFO,
        #          'get_xreference pnfs %s'%(ticket['xref'],))
        Trace.trace(10, 'get_xreference pnfs %s' % (ticket['xref'],))
        return

    def set_xreference(self, ticket):
        fname = pnfs.get_enstore_fs_path(ticket['filepath'])
        try:
            p = pnfs.Pnfs(fname)
            p.set_xreference(ticket['volume'],
                             ticket['location_cookie'],
                             ticket['size'],
                             ticket['file_family'],
                             ticket['pnfsFilename'],
                             ticket['volume_filepath'],
                             ticket['id'],
                             ticket['volume_fileP'],
                             ticket['bit_file_id'],
                             ticket['drive'],
                             ticket['crc'],
                             fname)
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['xref'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['xref'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    def get_file_size(self, ticket):
        fname = pnfs.get_enstore_fs_path(ticket['fname'])
        try:
            ticket['size'] = tuple(os.stat(fname))[stat.ST_SIZE]
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['size'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except OSError as msg:
            ticket['size'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    def set_file_size(self, ticket):
        fname = pnfs.get_enstore_fs_path(ticket['fname'])
        p = pnfs.Pnfs(fname)
        try:
            p.set_file_size(ticket['size'])
            ticket['status'] = (e_errors.OK, None)
            Trace.log(e_errors.INFO,
                      'set_file_size %s %s' % (fname, ticket['size'],))
        except OSError as msg:
            ticket['size'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except OSError as msg:
            ticket['size'] = None
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    """
    def set_outfile_permissions(self,ticket) :
        work_ticket = ticket['ticket']
        if not ticket.get('copy', None):  #Don't set permissions if copy.
            #Attempt to get the input files permissions and set the output
            # file to match them.
            if work_ticket['outfile'] != "/dev/null":
                try:
                    perms = None
                    if ( os.path.exists(work_ticket['infile']) ):
                        perms = os.stat(work_ticket['infile'])[stat.ST_MODE]
                    else:
                        perms = work_ticket['wrapper']['pstat'][stat.ST_MODE]
                    os.chmod(work_ticket['outfile'], perms)
                    uid=work_ticket['wrapper'].get('uid',None)
                    gid=work_ticket['wrapper'].get('gid',None)
                    os.chown(work_ticket['outfile'],uid,gid)
                    work_ticket['status'] = (e_errors.OK, None)
                except OSError, msg:
                    Trace.log(e_errors.ERROR, "chmod %s failed %s:" % \
                              (work_ticket['outfile'], msg))
                    work_ticket['status'] = (e_errors.USERERROR,
                                        "Unable to set permissions.")
#        file_utils.set_outfile_permissions(work_ticket)
        ticket['status']   = (e_errors.OK, None)
        ticket['ticket']   = work_ticket
        self.reply_to_caller(ticket)
        return
    """

    def chmod(self, ticket):
        fname = pnfs.get_enstore_fs_path(ticket['fname'])
        mode = ticket['mode']
        p = pnfs.Pnfs(fname)
        try:
            p.chmod(mode)
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
            Trace.trace(5, "OSError: %s" % (ticket,))
        except IOError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    # change the ownership of the existing file
    def chown(self, ticket):
        fname = ticket['fname']
        uid = ticket['uid']
        gid = ticket['gid']
        p = pnfs.Pnfs(fname)
        try:
            p.chown(uid, gid, fname)
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
            Trace.trace(5, "OSError: %s" % (ticket,))
        except IOError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    # Delete the file while clobbering layers.
    def utime(self, ticket):
        fname = ticket['fname']
        p = pnfs.Pnfs(fname)
        try:
            p.utime()
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    # Delete the file while clobbering layers.
    def rm(self, ticket):
        fname = pnfs.get_enstore_fs_path(ticket['fname'])
        p = pnfs.Pnfs(fname)
        try:
            p.rm()
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    # Delete the file.
    def remove(self, ticket):
        fname = pnfs.get_enstore_fs_path(ticket['fname'])
        try:
            os.remove(fname)
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    def is_pnfs_path(self, ticket):
        try:
            fname = pnfs.get_enstore_fs_path(ticket['fname'])
        except KeyError:
            print("is_pnfs_path", ticket)
        except ValueError:
            # This will happen for non-pnfs files where "/pnfs/" is not
            # found.
            ticket['rc'] = 0
            fname = None
        except OSError as msg:
            fname = None
            if msg.args[0] in [errno.ENOENT]:
                # The path was a local path that contained a directory
                # named "pnfs".  This pnfs is not the mount point for
                # PNFS, just a regular directory with the name "pnfs".
                ticket['rc'] = 0
            else:
                ticket['rc'] = None  # Error.

        if fname is not None:
            check_name_only = ticket['check_name_only']
            ticket['rc'] = pnfs.is_pnfs_path(fname,
                                             check_name_only=check_name_only)

        # Send the info.
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    # Make a directory.
    def mkdir(self, ticket):
        err = False
        try:
            fname = pnfs.get_enstore_fs_path(ticket['path'])
        except KeyError as msg:
            ticket['status'] = (e_errors.KEYERROR, str(msg))
            self.reply_to_caller(ticket)
            return
        try:
            os.mkdir(fname)
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
            err = True
        except IOError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
            err = True
        if err == False:
            uid = ticket.get('uid', None)
            gid = ticket.get('gid', None)
            if uid and gid:
                os.chown(fname, uid, gid)
        self.reply_to_caller(ticket)
        return

    # Make a directory.  (Make any missing directories in the path.)
    def mkdirs(self, ticket):
        err = False
        try:
            dirname = pnfs.get_enstore_fs_path(ticket['dirname'])
        except KeyError as msg:
            ticket['status'] = (e_errors.KEYERROR, str(msg))
            self.reply_to_caller(ticket)
            return
        try:
            os.makedirs(dirname)
            ticket['status'] = (e_errors.OK, None)

        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
            err = True
        except IOError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
            err = True
        if err == False:
            uid = ticket.get('uid', None)
            gid = ticket.get('gid', None)
            if uid and gid:
                try:
                    os.chown(dirname, uid, gid)
                except OSError as msg:
                    ticket['errno'] = msg.args[0]
                    ticket['status'] = (e_errors.OSERROR, str(msg))

                except IOError as msg:
                    ticket['errno'] = msg.args[0]
                    ticket['status'] = (e_errors.IOERROR, str(msg))

        self.reply_to_caller(ticket)
        return

    # Remove a directory.
    def rmdir(self, ticket):
        dirname = pnfs.get_enstore_fs_path(ticket['dirname'])
        try:
            os.rmdir(dirname)
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    # List directory contents.
    def list_dir(self, ticket):
        dirname = pnfs.get_enstore_fs_path(ticket['dirname'])
        try:
            dir_list = os.listdir(dirname)

            # Stuff the filenames to be sent back.
            ticket['dir_list'] = []
            for fname in dir_list:
                fname = os.path.join(dirname, fname)
                # If the user only wants regular files, weed out the others.
                if ticket.get('just_files', None):
                    try:
                        fstat = file_utils.get_stat(fname)
                        if not stat.S_ISREG(fstat[stat.ST_MODE]):
                            continue
                    except OSError as msg:
                        ticket['status'] = (e_errors.OSERROR, str(msg))
                        break

                ticket['dir_list'].append({'name': fname})
            else:
                ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

    # Find file knowing bfid and pnfsid.
    def find_pnfsid_path(self, ticket):
        try:
            paths = find_pnfs_file.find_pnfsid_path(
                ticket['pnfsid'], ticket['bfid'],
                ticket.get('file_record', None),
                ticket.get('likely_path', None),
                ticket.get('path_type', enstore_constants.BOTH))

            # Stuff the filenames to be sent back.
            ticket['paths'] = paths
            ticket['status'] = (e_errors.OK, None)
        except OSError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.OSERROR, str(msg))
        except IOError as msg:
            ticket['errno'] = msg.args[0]
            ticket['status'] = (e_errors.IOERROR, str(msg))
        self.reply_to_caller(ticket)
        return

###############################################################################

    # Process the  request that was (generally) sent from UDPClient.send
    # overrides dispatching worker method
    # the difference: creates a thread
    # if number of threads is maximal waits until a running thread exits
    def process_request(self, request, client_address):

        # Since get_request() gets requests from UDPServer.get_message()
        # and self.read_fd(fd).  Thusly, some care needs to be taken
        # from within UDPServer.process_request() to be tolerent of
        # requests not originally read with UDPServer.get_message().
        ticket = udp_server.UDPServer.process_request(self, request,
                                                      client_address)

        Trace.trace(
            6, "pnfs_agent:process_request %s; %s" %
            (request, ticket,))
        # This checks help process cases where the message was repeated
        # by the client.
        if not ticket:
            Trace.trace(6, "pnfs_agent: no ticket!!!")
            return

        # look in the ticket and figure out what work user wants
        try:
            function_name = ticket["work"]
        except (KeyError, AttributeError, TypeError) as detail:
            ticket = {'status': (e_errors.KEYERROR,
                                 "cannot find any named function")}
            msg = "%s process_request %s from %s" % \
                (detail, ticket, client_address)
            Trace.trace(6, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        try:
            Trace.trace(5, "process_request: function %s" % (function_name,))
            function = getattr(self, function_name)
        except (KeyError, AttributeError, TypeError) as detail:
            ticket = {'status': (e_errors.KEYERROR,
                                 "cannot find requested function `%s'"
                                 % (function_name,))}
            msg = "%s process_request %s %s from %s" % \
                (detail, ticket, function_name, client_address)
            Trace.trace(6, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # call the user function
        t = time.time()
        '''
        while 1:
            c = threading.activeCount()
            if c < self.max_threads:
                Trace.trace(5, "threads %s"%(c,))
                dispatching_worker.run_in_thread(None, function, (ticket,), after_function=self._done_cleanup)
                #self.run_in_thread(function, (ticket,))
                #self.run_in_thread(function, (ticket,self._done_cleanup))
                break
	'''
        c = threading.activeCount()
        Trace.trace(5, "threads %s" % (c,))
        if c < self.max_threads:
            dispatching_worker.run_in_thread(
                None, function, (ticket,), after_function=self._done_cleanup)
            #self.run_in_thread(function, (ticket,))
            #self.run_in_thread(function, (ticket,self._done_cleanup))
        else:
            function(ticket)

        #apply(function, (ticket,))
        Trace.trace(
            5, "process_request: function %s time %s" %
            (function_name, time.time() - t))


class PnfsAgentInterface(generic_server.GenericServerInterface):
    pass


if __name__ == "__main__":

    # get the interface
    intf = PnfsAgentInterface()
    vc = PnfsAgent((intf.config_host, intf.config_port))
    vc.handle_generic_commands(intf)
    Trace.init(vc.log_name, 'yes')
    # Trace.init(vc.log_name)

    Trace.log(e_errors.INFO, '%s' % (sys.argv,))
    # vc._do_print({'levels':[5,6,]})

    while True:
        try:
            vc.serve_forever()
        except SystemExit:
            Trace.log(e_errors.INFO, "%s exiting." % (vc.name,))
            os._exit(0)
            break
        except BaseException:
            try:
                exc, msg, tb = sys.exc_info()
                full_tb = traceback.format_exception(exc, msg, tb)
                for l in full_tb:
                    Trace.log(e_errors.ERROR, l[:-1], {}, "TRACEBACK")
                Trace.log(e_errors.INFO, "restarting after exception")
                vc.restart()
            except BaseException:
                pass

    Trace.log(e_errors.INFO, 'ERROR returned from serve_forever')
