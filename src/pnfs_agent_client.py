#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# PNFS agend client
# Author: Dmitry Litvintsev (litvinse@fnal.gov) 08/05
#
###############################################################################

# system imports
from __future__ import print_function
from future.utils import raise_
import sys
import pprint
import stat
import os
import socket
import string
import errno
# import cPickle

# enstore imports
import option
import generic_client
import backup_client
#import udp_client
import Trace
import e_errors
import enstore_constants
import enstore_functions2

# For layer_file() and is_pnfs_path
from pnfs import is_access_name

MY_NAME = enstore_constants.PNFS_AGENT_CLIENT  # "PNFS_A_CLIENT"
MY_SERVER = enstore_constants.PNFS_AGENT  # "pnfs_agent"

RCV_TIMEOUT = 10
RCV_TRIES = 5


class PnfsAgentClient(generic_client.GenericClient,
                      backup_client.BackupClient):

    def __init__(self, csc, server_address=None, flags=0, logc=None,
                 alarmc=None, rcv_timeout=RCV_TIMEOUT,
                 rcv_tries=RCV_TRIES):

        # self.print_id is unique in each of pnfs.Pnfs, chimera.ChimeraFS,
        # and pnfs_agent_client.PnfsAgentClient.  It is to be used for
        # the printing of messages to name the specific interface
        # being used by namespace.StorageFS.
        self.print_id = "pnfs_agent"

        generic_client.GenericClient.__init__(self, csc, MY_NAME, server_address,
                                              flags=flags, logc=logc,
                                              alarmc=alarmc,
                                              rcv_timeout=rcv_timeout,
                                              rcv_tries=rcv_tries,
                                              server_name=MY_SERVER)

        self.r_ticket = {'work': 'get_file_stat',
                         'filename': "",
                         'statinfo': [],
                         'pinfo': {},
                         'bfid': None
                         }

    def status(self, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        return self.send({"work": "show_state"}, rcv_timeout, tries)

    def show_state(self, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        return self.send({'work': 'show_state'},
                         rcv_timeout=rcv_timeout, tries=tries)

###############################################################################

    # FIXME (Could replace with pnfs.Pnfs().layer_file()?)
    def layer_file(self, filename, layer_number):
        pn, fn = os.path.split(filename)
        if is_access_name(fn):
            return os.path.join(pn, "%s(%d)" % (fn, layer_number))
        else:
            return os.path.join(pn, ".(use)(%d)(%s)" % (layer_number, fn))

    # FIXME (Could replace with pnfs.Pnfs().access_file()?)
    def access_file(self, pn, pnfsid):
        return os.path.join(pn, ".(access)(%s)" % pnfsid)

    # FIXME (Could replace with pnfs.Pnfs().use_file()?)
    def use_file(self, f, layer):
        pn, fn = os.path.split(f)
        if is_access_name(fn):
            # Use the .(access)() extension path for layers.
            return "%s(%s)" % (f, layer)
        else:
            return os.path.join(pn, '.(use)(%d)(%s)' % (layer, fn))

    # FIXME (Could replace with pnfs.Pnfs().fset_file()?)
    def fset_file(self, f, size):
        pn, fn = os.path.split(f)
        if is_access_name(fn):
            pnfsid = fn[10:-1]  # len(".(access)(") == 10 and len ")" == 1
            parent_id = self.get_parent(pnfsid, pn)

            directory = os.path.join(pn, ".(access)(%s)" % parent_id)
            name = self.get_nameof(pnfsid, pn)
        else:
            directory = pn
            name = fn

        return os.path.join(directory, ".(fset)(%s)(size)(%s)" % (name, size))

    # FIXME (Could replace with pnfs.Pnfs().nameof_file()?)
    def nameof_file(self, pn, pnfsid):
        return os.path.join(pn, ".(nameof)(%s)" % (pnfsid,))

    # FIXME (Could replace with pnfs.Pnfs().const_file()?)
    def const_file(self, f):
        pn, fn = os.path.split(f)
        if is_access_name(fn):
            pnfsid = fn[10:-1]  # len(".(access)(") == 10 and len ")" == 1
            parent_id = self.get_parent(pnfsid, pn)

            directory = os.path.join(pn, ".(access)(%s)" % parent_id)
            name = self.get_nameof(pnfsid, pn)
        else:
            directory = pn
            name = fn

        return os.path.join(directory, ".(const)(%s)" % (name,))

###############################################################################

    def is_pnfs_path(self, pathname, check_name_only=None,
                     rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ####################
        # Do the first part of check locally as done in
        # pnfs.py
        # This allows to send requests to pnfs agent only when needed,
        # thus reducing the traffic between pnfs agent client and pnfs agent

        if not pathname:  # Handle None and empty string.
            return False

        full_pathname = enstore_functions2.fullpath(pathname)[1]

        # Determine if the target file or directory is in the pnfs namespace.
        if string.find(full_pathname, "/pnfs/") < 0:
            return False  # If we get here it is not a pnfs directory.
        ####################

        ticket = {'work': 'is_pnfs_path',
                  'fname': pathname,
                  'check_name_only': check_name_only
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        if not e_errors.is_ok(ticket):
            return None  # Should this raise an exception instead?
        return ticket['rc']

    def isdir(self, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'get_stat',
                  'filename': filename,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        if not e_errors.is_ok(ticket):
            return None
        else:
            return stat.S_ISDIR(ticket['statinfo'][stat.ST_MODE])

    def isfile(self, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'get_stat',
                  'filename': filename,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        if not e_errors.is_ok(ticket):
            return None
        else:
            return stat.S_ISREG(ticket['statinfo'][stat.ST_MODE])

    def islink(self, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'get_stat',
                  'filename': filename,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        if not e_errors.is_ok(ticket):
            return None
        else:
            return stat.S_ISLNK(ticket['statinfo'][stat.ST_MODE])

    def e_access(self, path, mode, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'e_access',
                  'path': path,
                  'mode': mode,
                  'rc': 1
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        if not e_errors.is_ok(ticket):
            return None
        return ticket['rc']

###############################################################################

    """
    def get_directory_name(self, filename):
        ticket = { 'work' : 'is_pnfs_path',
                   'fname' : filename,
                   }
        ticket = self.send(ticket)
        if not e_errors.is_ok(ticket):
            return None   #Should this raise an exception instead?
        return ticket['rc']
    """

###############################################################################

    def get_file_stat(self, filename, rcv_timeout=RCV_TIMEOUT,
                      tries=RCV_TRIES):
        if (self.r_ticket['filename'] != filename):
            self.r_ticket['filename'] = filename
            self.r_ticket = self.send(self.r_ticket, rcv_timeout=rcv_timeout,
                                      tries=tries)
            if self.r_ticket['status'][0] == e_errors.OK:
                return self.r_ticket['statinfo'], self.r_ticket['bfid'], self.r_ticket['pinfo']
            else:
                return None, None, None
        else:
            if (self.r_ticket['status'][0] == e_errors.OK):
                return self.r_ticket['statinfo'], self.r_ticket['bfid'], self.r_ticket['pinfo']
            else:
                return None, None, None

    def get_pnfsstat(self, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'get_pnfsstat',
                  'filename': filename,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        if e_errors.is_ok(ticket):
            if not ticket['statinfo']:
                message = "Received non-error stat() reply with missing " \
                          "stat info: %s" % (ticket,)
                Trace.log(e_errors.ERROR, message)
            return ticket['statinfo']
        elif ticket['status'][0] == e_errors.IOERROR:
            raise IOError(ticket['errno'], ticket['status'][1])
        elif ticket['status'][0] == e_errors.OSERROR:
            raise OSError(ticket.get('errno', e_errors.UNKNOWN),
                          ticket['status'][1])
        elif e_errors.is_timedout(ticket):
            raise OSError(errno.ETIMEDOUT, "pnfs_agent")
        else:
            raise e_errors.EnstoreError(None, ticket['status'][0],
                                        ticket['status'][1])

    def get_stat(self, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'get_stat',
                  'filename': filename,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        if e_errors.is_ok(ticket):
            if not ticket['statinfo']:
                message = "Received non-error stat() reply with missing " \
                          "stat info: %s" % (ticket,)
                Trace.log(e_errors.ERROR, message)
            return ticket['statinfo']
        elif ticket['status'][0] == e_errors.IOERROR:
            raise IOError(ticket['errno'], ticket['status'][1])
        elif ticket['status'][0] == e_errors.OSERROR:
            raise OSError(ticket.get('errno', e_errors.UNKNOWN),
                          ticket['status'][1])
        elif e_errors.is_timedout(ticket):
            raise OSError(errno.ETIMEDOUT, "pnfs_agent")
        else:
            raise e_errors.EnstoreError(None, ticket['status'][0],
                                        ticket['status'][1])

###############################################################################

    def p_get_library(self, dirname,
                      rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'get_library',
                  'dirname': dirname,
                  'library': None
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_set_library(self, library, dirname,
                      rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'set_library',
                  'dirname': dirname,
                  'library': library
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_get_file_family(self, dirname,
                          rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'get_file_family',
                  'dirname': dirname,
                  'file_family': None
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_set_file_family(self, file_family, dirname,
                          rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'set_file_family',
                  'dirname': dirname,
                  'file_family': file_family
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_get_file_family_width(self, dirname, rcv_timeout=RCV_TIMEOUT,
                                tries=RCV_TRIES):
        ticket = {'work': 'get_file_family_width',
                  'dirname': dirname,
                  'file_family_width': None
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_set_file_family_width(self, file_family_width, dirname,
                                rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'set_file_family_width',
                  'dirname': dirname,
                  'file_family_width': file_family_width
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_get_file_family_wrapper(self, dirname,
                                  rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'get_file_family_wrapper',
                  'dirname': dirname,
                  'file_family_wrapper': None
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_set_file_family_wrapper(self, file_family_wrapper, dirname,
                                  rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'set_file_family_wrapper',
                  'dirname': dirname,
                  'file_family_wrapper': file_family_wrapper
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_get_storage_group(self, dirname,
                            rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'get_storage_group',
                  'dirname': dirname,
                  'storage_group': None
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_set_storage_group(self, storage_group, dirname,
                            rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'set_storage_group',
                  'dirname': dirname,
                  'storage_group': storage_group
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

###############################################################################

    def p_get_path(self, pnfs_id, mount_point, shortcut=None,
                   rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'get_path',
                  'pnfs_id': pnfs_id,
                  'dirname': mount_point,
                  'shortcut': shortcut,
                  'path': None
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_set_bit_file_id(self, bfid, fname, rcv_timeout=RCV_TIMEOUT,
                          tries=RCV_TRIES):
        ticket = {'work': 'set_bit_file_id',
                  'fname': fname,
                  'bfid': bfid
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_get_bit_file_id(self, fname, rcv_timeout=RCV_TIMEOUT,
                          tries=RCV_TRIES):
        ticket = {'work': 'get_bit_file_id',
                  'fname': fname,
                  'bfid': None
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_get_id(self, fname, rcv_timeout=RCV_TIMEOUT,
                 tries=RCV_TRIES):
        ticket = {'work': 'get_id',
                  'fname': fname,
                  'file_id': None
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_get_nameof(self, pnfsid, rcv_timeout=RCV_TIMEOUT,
                     tries=RCV_TRIES):
        ticket = {'work': 'get_nameof',
                  'pnfsid': pnfsid,
                  'nameof': None
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_get_parent_id(self, pnfsid, rcv_timeout=RCV_TIMEOUT,
                        tries=RCV_TRIES):
        ticket = {'work': 'get_parent_id',
                  'pnfsid': pnfsid,
                  'parent_id': None
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    p_get_parent = p_get_parent_id  # backward compatibility

    def p_get_file_size(self, fname, rcv_timeout=RCV_TIMEOUT,
                        tries=RCV_TRIES):
        ticket = {'work': 'get_file_size',
                  'fname': fname,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_set_file_size(self, size, fname, rcv_timeout=RCV_TIMEOUT,
                        tries=RCV_TRIES):
        ticket = {'work': 'set_file_size',
                  'fname': fname,
                  'size': size
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_set_xreference(self, volume, location_cookie, size, file_family,
                         pnfsFilename, volume_filepath, id, volume_fileP,
                         bit_file_id, drive, crc, filepath,
                         rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):

        ticket = {'work': 'set_xreference',
                  'volume': volume,
                  'location_cookie': location_cookie,
                  'size': size,
                  'file_family': file_family,
                  'pnfsFilename': pnfsFilename,
                  'volume_filepath': volume_filepath,
                  'id': id,
                  'volume_fileP': volume_fileP,
                  'bit_file_id': bit_file_id,
                  'drive': drive,
                  'crc': crc,
                  'filepath': filepath
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    def p_get_xreference(self, fname, rcv_timeout=RCV_TIMEOUT,
                         tries=RCV_TRIES):
        ticket = {'work': 'get_xreference',
                  'fname': fname,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

###############################################################################

    def readlayer(self, layer, fname, rcv_timeout=RCV_TIMEOUT,
                  tries=RCV_TRIES):
        ticket = {'work': 'readlayer',
                  'fname': fname,
                  'layer': layer
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        if e_errors.is_ok(ticket):
            return ticket['layer_info']
        return []  # What sould happen here?

    def writelayer(self, layer, value, fname, rcv_timeout=RCV_TIMEOUT,
                   tries=RCV_TRIES):
        ticket = {'work': 'writelayer',
                  'fname': fname,
                  'layer': layer,
                  'value': value
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        if not e_errors.is_ok(ticket):
            raise_(OSError, ticket['status'][1])

###############################################################################

    # modify the permissions of the target
    def p_chmod(self, mode, filename, rcv_timeout=RCV_TIMEOUT,
                tries=RCV_TRIES):
        ticket = {'work': 'chmod',
                  'fname': filename,
                  'mode': mode
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    # modify the ownership of the target
    def p_chown(self, uid, gid, filename, rcv_timeout=RCV_TIMEOUT,
                tries=RCV_TRIES):
        ticket = {'work': 'chown',
                  'fname': filename,
                  'uid': uid,
                  'gid': gid,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    # create a new file or update its times
    def p_touch(self, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'touch',
                  'filename': filename,
                  'uid': os.geteuid(),
                  'gid': os.getegid(),
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    # create a new file
    def p_creat(self, filename, mode=None, rcv_timeout=RCV_TIMEOUT,
                tries=RCV_TRIES):
        ticket = {'work': 'creat',
                  'filename': filename,
                  'uid': os.geteuid(),
                  'gid': os.getegid(),
                  }
        if mode:
            ticket['mode'] = mode
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    # update the access and mod time of a file
    def p_utime(self, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'utime',
                  'fname': filename,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    # delete a pnfs file including its metadata
    def p_rm(self, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'rm',
                  'fname': filename,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    # delete a pnfs file (leaving the metadata do be put in the trashcan)
    def p_remove(self, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'remove',
                  'fname': filename,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    # make a directory
    def p_mkdir(self, dirname, uid=None, gid=None, rcv_timeout=RCV_TIMEOUT,
                tries=RCV_TRIES):
        if uid is None:
            uid = os.getuid()
        if gid is None:
            gid = os.getgid()
        ticket = {'work': 'mkdir',
                  'path': dirname,
                  'uid': uid,
                  'gid': gid
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    # make a directory
    def p_mkdirs(self, dirname, uid=None, gid=None, rcv_timeout=RCV_TIMEOUT,
                 tries=RCV_TRIES):
        ticket = {'work': 'mkdirs',
                  'dirname': dirname,
                  'uid': uid,
                  'gid': gid
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    # remove a directory
    def p_rmdir(self, dirname, rcv_timeout=RCV_TIMEOUT,
                tries=RCV_TRIES):
        ticket = {'work': 'rmdir',
                  'dirname': dirname,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    # remove a directory
    def p_list_dir(self, dirname, just_files=0, rcv_timeout=RCV_TIMEOUT,
                   tries=RCV_TRIES):
        ticket = {'work': 'list_dir',
                  'dirname': dirname,
                  'just_files': just_files,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket

    # find a file knowning pnfsid and bfid
    def p_find_pnfsid_path(self, pnfsid, bfid, file_record=None,
                           likely_path=None,
                           path_type=enstore_constants.BOTH,
                           rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        ticket = {'work': 'find_pnfsid_path',
                  'pnfsid': pnfsid,
                  'bfid': bfid,
                  'file_record': file_record,
                  'likely_path': likely_path,
                  'path_type': path_type,
                  }
        ticket = self.send(ticket, rcv_timeout=rcv_timeout, tries=tries)
        return ticket


###############################################################################

    # Take a a ticket and convert it into a traceback.

    def raise_exception(self, ticket):
        if ticket['status'][0] == e_errors.OK:
            return
        elif ticket['status'][0] == e_errors.IOERROR:
            raise IOError(ticket['errno'], ticket['status'][1])
        elif ticket['status'][0] == e_errors.OSERROR:
            raise OSError(ticket.get('errno', 0), ticket['status'][1])
        elif ticket['status'][0] == e_errors.KEYERROR:
            raise KeyError(ticket.get('errno', 0), ticket['status'][1])
        elif ticket['status'][0] == e_errors.NET_ERROR:
            raise socket.error(ticket.get('errno', 0), ticket['status'][1])
        elif ticket['status'][0] == e_errors.PNFS_ERROR:
            raise OSError(ticket.get('errno', 0), ticket['status'][1])
        else:
            # Is there anything better?
            raise OSError(ticket.get('errno', 0), ticket['status'][1])

    def get_library(self, dirname, rcv_timeout=RCV_TIMEOUT,
                    tries=RCV_TRIES):
        reply_ticket = self.p_get_library(dirname, rcv_timeout=rcv_timeout,
                                          tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return reply_ticket['library']

    def set_library(self, library, dirname, rcv_timeout=RCV_TIMEOUT,
                    tries=RCV_TRIES):
        reply_ticket = self.p_set_library(library, dirname,
                                          rcv_timeout=rcv_timeout, tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return library  # legacy

    def get_file_family(self, dirname, rcv_timeout=RCV_TIMEOUT,
                        tries=RCV_TRIES):
        reply_ticket = self.p_get_file_family(dirname, rcv_timeout=rcv_timeout,
                                              tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return reply_ticket['file_family']

    def set_file_family(self,
                        file_family, dirname,
                        rcv_timeout=RCV_TIMEOUT,
                        tries=RCV_TRIES):
        reply_ticket = self.p_set_file_family(file_family,
                                              dirname,
                                              rcv_timeout=rcv_timeout,
                                              tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return file_family  # legacy

    def get_file_family_width(self, dirname, rcv_timeout=RCV_TIMEOUT,
                              tries=RCV_TRIES):
        reply_ticket = self.p_get_file_family_width(dirname,
                                                    rcv_timeout=rcv_timeout,
                                                    tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return reply_ticket['file_family_width']

    def set_file_family_width(self, file_family_width, dirname,
                              rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_set_file_family_width(file_family_width, dirname,
                                                    rcv_timeout=rcv_timeout,
                                                    tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return file_family_width  # legacy

    def get_file_family_wrapper(self, dirname, rcv_timeout=RCV_TIMEOUT,
                                tries=RCV_TRIES):
        reply_ticket = self.p_get_file_family_wrapper(dirname,
                                                      rcv_timeout=rcv_timeout,
                                                      tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return reply_ticket['file_family_wrapper']

    def set_file_family_wrapper(self, file_family_wrapper, dirname,
                                rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_set_file_family_wrapper(file_family_wrapper,
                                                      dirname,
                                                      rcv_timeout=rcv_timeout,
                                                      tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return file_family_wrapper  # legacy

    def get_storage_group(self, dirname, rcv_timeout=RCV_TIMEOUT,
                          tries=RCV_TRIES):
        reply_ticket = self.p_get_storage_group(dirname,
                                                rcv_timeout=rcv_timeout,
                                                tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return reply_ticket['storage_group']

    def set_storage_group(self, storage_group, dirname,
                          rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_set_storage_group(storage_group, dirname,
                                                rcv_timeout=rcv_timeout,
                                                tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return storage_group  # legacy

    def chmod(self, mode, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_chmod(mode, filename, rcv_timeout=rcv_timeout,
                                    tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)

    def chown(self, uid, gid, filename, rcv_timeout=RCV_TIMEOUT,
              tries=RCV_TRIES):
        reply_ticket = self.p_chown(uid, gid, filename,
                                    rcv_timeout=rcv_timeout, tries=tries)
        print("reply_ticket:", reply_ticket)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)

    def touch(self, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_touch(filename, rcv_timeout=rcv_timeout,
                                    tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)

    def creat(self, filename, mode=None, rcv_timeout=RCV_TIMEOUT,
              tries=RCV_TRIES):
        reply_ticket = self.p_creat(filename, mode=mode,
                                    rcv_timeout=rcv_timeout, tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)

    def utime(self, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_utime(filename, rcv_timeout=rcv_timeout,
                                    tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)

    def rm(self, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_rm(filename, rcv_timeout=rcv_timeout,
                                 tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)

    def remove(self, filename, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_remove(filename, rcv_timeout=rcv_timeout,
                                     tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)

    def mkdir(self, dirname, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_mkdir(dirname, rcv_timeout=rcv_timeout,
                                    tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)

    def mkdirs(self, dirname, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_mkdirs(dirname, rcv_timeout=rcv_timeout,
                                     tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)

    def rmdir(self, dirname, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_rmdir(dirname, rcv_timeout=rcv_timeout,
                                    tries=tries)
        self.raise_exception(reply_ticket)

    def list_dir(self, dirname, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_list_dir(dirname, rcv_timeout=rcv_timeout,
                                       tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)

    def set_file_size(self, size, fname, rcv_timeout=RCV_TIMEOUT,
                      tries=RCV_TRIES):
        reply_ticket = self.p_set_file_size(size, fname,
                                            rcv_timeout=rcv_timeout,
                                            tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)

    def set_xreference(self, volume, location_cookie, size, file_family,
                       pnfsFilename, volume_filepath, id, volume_fileP,
                       bit_file_id, drive, crc, filepath,
                       rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_set_xreference(
            volume, location_cookie, size, file_family,
            pnfsFilename, volume_filepath, id, volume_fileP,
            bit_file_id, drive, crc, filepath,
            rcv_timeout=rcv_timeout, tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)

    def get_xreference(self, fname, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_get_xreference(fname, rcv_timeout=rcv_timeout,
                                             tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return reply_ticket['xref']

    def get_file_size(self, fname, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_get_file_size(fname, rcv_timeout=rcv_timeout,
                                            tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return reply_ticket['size']

    def get_path(self, pnfs_id, mount_point, shortcut=None,
                 rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_get_path(pnfs_id, mount_point, shortcut,
                                       rcv_timeout=rcv_timeout, tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return reply_ticket['path']

    def set_bit_file_id(self, bfid, fname, rcv_timeout=RCV_TIMEOUT,
                        tries=RCV_TRIES):
        reply_ticket = self.p_set_bit_file_id(bfid, fname,
                                              rcv_timeout=rcv_timeout,
                                              tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)

    def get_bit_file_id(self, fname, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_get_bit_file_id(fname, rcv_timeout=rcv_timeout,
                                              tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return reply_ticket['bfid']

    def get_id(self, fname, rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_get_id(fname, rcv_timeout=rcv_timeout,
                                     tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return reply_ticket['file_id']

    def get_nameof(self, pnfsid, rcv_timeout=RCV_TIMEOUT,
                   tries=RCV_TRIES):
        reply_ticket = self.p_get_nameof(pnfsid, rcv_timeout=rcv_timeout,
                                         tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return reply_ticket['nameof']

    def get_parent_id(self, pnfsid, rcv_timeout=RCV_TIMEOUT,
                      tries=RCV_TRIES):
        reply_ticket = self.p_get_parent_id(pnfsid, rcv_timeout=rcv_timeout,
                                            tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return reply_ticket['parent_id']

    get_parent = get_parent_id

    def find_pnfsid_path(self, pnfsid, bfid, file_record=None,
                         likely_path=None,
                         path_type=enstore_constants.BOTH,
                         rcv_timeout=RCV_TIMEOUT, tries=RCV_TRIES):
        reply_ticket = self.p_find_pnfsid_path(
            pnfsid, bfid, file_record, likely_path, path_type,
            rcv_timeout=rcv_timeout, tries=tries)
        if not e_errors.is_ok(reply_ticket):
            self.raise_exception(reply_ticket)
        return reply_ticket['paths']

    find_id_path = find_pnfsid_path


class PnfsAgentClientInterface(generic_client.GenericClientInterface):
    def __init__(self, args=sys.argv, user_mode=1):
        self.alive_rcv_timeout = RCV_TIMEOUT
        self.alive_retries = RCV_TRIES
        self.enable = 0
        self.status = 0
        self.notify = []
        self.sendto = []
        self.dump = 0
        self.warm_restart = 0
        self.mkdir = 0
        self.mkdirs = 0
        self.rmdir = 0
        self.exists = 0
        self.list_dir = 0
        self.layer = None
        self.remove = 0
        self.touch = 0
        self.size = 0
        self.id = 0
        self.just_files = 0  # optionally used with --list-dir
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)
        return

    def valid_dictionaries(self):
        return (self.alive_options, self.help_options, self.trace_options,
                self.pnfs_agent_options)

    pnfs_agent_options = {
        option.EXISTS: {option.HELP_STRING: "return true if file exists, false"
                        " otherwise",
                        option.DEFAULT_VALUE: option.DEFAULT,
                        option.DEFAULT_TYPE: option.STRING,
                        option.VALUE_USAGE: option.REQUIRED,
                        option.USER_LEVEL: option.ADMIN},
        option.ID: {option.HELP_STRING: "prints the pnfs id",
                    option.DEFAULT_VALUE: option.DEFAULT,
                    option.DEFAULT_NAME: "id",
                    option.DEFAULT_TYPE: option.INTEGER,
                    option.VALUE_NAME: "filename",
                    option.VALUE_TYPE: option.STRING,
                    option.VALUE_USAGE: option.REQUIRED,
                    option.VALUE_LABEL: "filename",
                    option.FORCE_SET_DEFAULT: option.FORCE,
                    option.USER_LEVEL: option.ADMIN,
                    },
        option.LAYER: {option.HELP_STRING: "get layer information",
                       option.VALUE_TYPE: option.INTEGER,
                       option.VALUE_USAGE: option.REQUIRED,
                       # option.VALUE_LABEL:"layer",
                       option.USER_LEVEL: option.ADMIN,
                       option.EXTRA_VALUES: [{option.VALUE_NAME: "filename",
                                              option.VALUE_TYPE: option.STRING,
                                              option.VALUE_USAGE: option.REQUIRED, },
                                             ]},
        option.LIST_DIR: {option.HELP_STRING: "list directory contents",
                          option.DEFAULT_VALUE: option.DEFAULT,
                          option.DEFAULT_TYPE: option.STRING,
                          option.VALUE_USAGE: option.REQUIRED,
                          option.USER_LEVEL: option.ADMIN},
        option.JUST_FILES: {option.HELP_STRING: "used with --list-dir to only" \
                            " report regular files",
                            option.DEFAULT_VALUE: option.DEFAULT,
                            option.DEFAULT_TYPE: option.INTEGER,
                            option.VALUE_USAGE: option.IGNORED,
                            option.USER_LEVEL: option.ADMIN},
        option.MKDIR: {option.HELP_STRING: "make directory",
                       option.DEFAULT_VALUE: option.DEFAULT,
                       option.DEFAULT_TYPE: option.STRING,
                       option.VALUE_USAGE: option.REQUIRED,
                       option.USER_LEVEL: option.ADMIN},
        option.MKDIRS: {option.HELP_STRING: "make directory; including missing",
                        option.DEFAULT_VALUE: option.DEFAULT,
                        option.DEFAULT_TYPE: option.STRING,
                        option.VALUE_USAGE: option.REQUIRED,
                        option.USER_LEVEL: option.ADMIN},
        option.REMOVE: {option.HELP_STRING: "remove file",
                        option.DEFAULT_VALUE: option.DEFAULT,
                        option.DEFAULT_TYPE: option.STRING,
                        option.VALUE_USAGE: option.REQUIRED,
                        option.USER_LEVEL: option.ADMIN},
        option.RMDIR: {option.HELP_STRING: "remove directory",
                       option.DEFAULT_VALUE: option.DEFAULT,
                       option.DEFAULT_TYPE: option.STRING,
                       option.VALUE_USAGE: option.REQUIRED,
                       option.USER_LEVEL: option.ADMIN},
        option.SIZE: {option.HELP_STRING: "sets the size of the file",
                      option.DEFAULT_VALUE: option.DEFAULT,
                      option.DEFAULT_NAME: "size",
                      option.DEFAULT_TYPE: option.INTEGER,
                      option.VALUE_NAME: "filename",
                      option.VALUE_TYPE: option.STRING,
                      option.VALUE_USAGE: option.REQUIRED,
                      option.VALUE_LABEL: "filename",
                      option.FORCE_SET_DEFAULT: option.FORCE,
                      option.USER_LEVEL: option.USER2,
                      option.EXTRA_VALUES: [{option.VALUE_NAME: "filesize",
                                             option.VALUE_TYPE: option.LONG,
                                             option.VALUE_USAGE: option.REQUIRED,
                                             }, ]
                      },
        option.STATUS: {option.HELP_STRING: "print pnfs_agent status",
                        option.DEFAULT_VALUE: option.DEFAULT,
                        option.DEFAULT_TYPE: option.INTEGER,
                        option.VALUE_USAGE: option.IGNORED,
                        option.USER_LEVEL: option.ADMIN},
        option.TOUCH: {option.HELP_STRING: "make file",
                       option.DEFAULT_VALUE: option.DEFAULT,
                       option.DEFAULT_TYPE: option.STRING,
                       option.VALUE_USAGE: option.REQUIRED,
                       option.USER_LEVEL: option.ADMIN},
    }


def do_work(intf):
    pac = PnfsAgentClient((intf.config_host, intf.config_port),
                          rcv_timeout=intf.alive_rcv_timeout,
                          rcv_tries=intf.alive_retries)
    Trace.init(pac.get_name(MY_NAME))
    ticket = {}
    try:
        ticket = pac.handle_generic_commands(MY_SERVER, intf)
        if ticket:
            pass
        elif intf.status:
            ticket = pac.status(intf.alive_rcv_timeout, intf.alive_retries)
            pprint.pprint(ticket)
        elif intf.mkdir:
            if not pac.e_access(intf.mkdir, os.F_OK):
                ticket = pac.p_mkdir(intf.mkdir)
            else:
                ticket = {'status': (e_errors.OK, None)}
        elif intf.mkdirs:
            if not pac.e_access(intf.mkdirs, os.F_OK):
                ticket = pac.p_mkdirs(intf.mkdirs)
            else:
                ticket = {'status': (e_errors.OK, None)}
        elif intf.rmdir:
            if pac.e_access(intf.rmdir, os.F_OK):
                ticket = pac.p_rmdir(intf.rmdir)
            else:
                ticket = {'status': (e_errors.OK, None)}
        elif intf.exists:
            if pac.e_access(intf.exists, os.F_OK):
                sys.exit(0)
            else:
                sys.exit(1)
        elif intf.list_dir:
            ticket = pac.p_list_dir(intf.list_dir, intf.just_files)
            if e_errors.is_ok(ticket):
                for file_info in ticket.get('dir_list', {}):
                    print(file_info['name'])
        elif intf.layer:
            layer_info = pac.readlayer(intf.layer, intf.filename)
            if not layer_info:
                ticket = {'status': (e_errors.UNKNOWN, None)}
            else:
                for line in layer_info:
                    print(line, end=' ')
                ticket = {'status': (e_errors.OK, None)}
        elif intf.remove:
            if pac.e_access(intf.remove, os.F_OK):
                ticket = pac.p_remove(intf.remove)
            else:
                ticket = {'status': (e_errors.OK, None)}
        elif intf.touch:
            if not pac.e_access(intf.touch, os.F_OK):
                ticket = pac.p_touch(intf.touch)
            else:
                ticket = {'status': (e_errors.OK, None)}
        elif intf.size:
            if pac.e_access(intf.filename, os.F_OK):
                ticket = pac.p_set_file_size(intf.filesize, intf.filename)
            else:
                ticket = {'status': (e_errors.DOESNOTEXIST, intf.filename)}
        elif intf.id:
            if pac.e_access(intf.filename, os.F_OK):
                ticket = pac.p_get_id(intf.filename)
                if e_errors.is_ok(ticket):
                    print(ticket['file_id'])
            else:
                ticket = {'status': (e_errors.DOESNOTEXIST, intf.filename)}
        else:
            intf.print_help()
            sys.exit(0)

        pac.check_ticket(ticket)
    except (KeyboardInterrupt):
        sys.exit(1)


if __name__ == "__main__":
    Trace.init(MY_NAME)
    Trace.trace(6, 'pac called with args: %s' % (sys.argv,))

    # fill in the interface
    intf = PnfsAgentClientInterface(user_mode=0)

    do_work(intf)
