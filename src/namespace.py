#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
from __future__ import print_function
from future.utils import raise_
import os
import sys
import string
import types
import errno

# enstore modules
import enstore_functions2
import configuration_client
import chimera
import option
import enstore_constants
import file_utils
import Trace

UNKNOWN = "unknown"  # Same in pnfs and chimera.

__pychecker__ = "no-override"


class StorageFS(chimera.ChimeraFS):

    def __init__(self, pnfsFilename="", mount_point="", shortcut=None):
        try:
            self.__class__ = chimera.ChimeraFS
            chimera.ChimeraFS.__init__(self, pnfsFilename,
                                       mount_point, shortcut)
        except BaseException:
            if Trace.log_func != Trace.default_log_func:
                # Send the traceback to the log file.
                Trace.handle_error(severity=99)
            raise_(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])


class Tag(chimera.Tag):

    def __init__(self, directory=None):
        try:
            self.__class__ = chimera.Tag
            chimera.Tag.__init__(self, directory)
        except BaseException:
            if Trace.log_func != Trace.default_log_func:
                # Send the traceback to the log file.
                Trace.handle_error(severity=99)
            raise_(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])

############################################################################


def is_storage_local_path(filename, check_name_only=None):
    return chimera.is_chimera_path(filename, check_name_only)


def is_storage_path(filename, check_name_only=None):
    pathname = os.path.abspath(filename)
    return is_storage_local_path(pathname, check_name_only)


def is_id(id):
    return chimera.is_chimeraid_or_pnfsid(id)

##############################################################################

# Return the directory name.  If a  Chimera path is given,
# the directory is split off and returned.  If the file is a special
# .(access)() file, then special handling is done to
# determine the .(accces)() name of the directory while trying to use
# as few resources as possible.


def get_directory_name(filepath):
    if not isinstance(filepath, bytes):
        return None

    # Determine if it is an ".(access)()" name.
    if chimera.is_access_name(filepath):
        # Since, we have the .(access)() name we need to split off the id.
        dirname, filename = os.path.split(filepath)
        pnfsid = filename[10:-1]  # len(".(access)(") == 10 and len ")" == 1

        # Create the filename to obtain the parent id.
        parent_id_name = os.path.join(dirname, ".(parent)(%s)" % pnfsid)

        # Read the parent id.  Try and avoid instantiating a StorageFS class
        # for performance.
        f = open(parent_id_name)
        parent_id = f.readlines()[0].strip()
        f.close()
        directory_name = os.path.join(dirname, ".(access)(%s)" % parent_id)
    else:
        directory_name = os.path.dirname(filepath)

    return directory_name


# Keys for global cache.
DB_NUMBER = "db_number"
DB_INFO = "db_info"
DB_MOUNT_POINTS = "db_mount_point"

EMPTY_MOUNT_POINT = {DB_INFO: "",
                     DB_NUMBER: -1,
                     DB_MOUNT_POINTS: ["", ],
                     }


def parse_mtab():
    # Different systems have different names for this file.
    # /etc/mtab: Linux, IRIX
    # /etc/mnttab: SunOS
    # MacOS doesn't have one.
    for mtab_file in ["/etc/mtab", "/etc/mnttab"]:
        try:
            fp = file_utils.open(mtab_file, "r")
            mtab_data = fp.readlines()
            fp.close()
            break
        except OSError as msg:
            if msg.args[0] in [errno.ENOENT]:
                continue
            else:
                raise_(sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2])
    else:
        # Should this raise an error?
        mtab_data = []

    index = len(chimera.mount_points_cache)  # Keep any Chimera values unique.
    for line in mtab_data:
        # The 2nd and 3rd items in the list are important to us here.
        line_of_mtab_file = line[:-1].split()
        mp = line_of_mtab_file[1]
        fs_type = line_of_mtab_file[2]

        # If the filesystem is not an NFS filesystem, skip it.
        if fs_type.startswith("nfs"):
            # To figure out if the NFS mount is really Chimera/PNFS
            # we run a tags command. If exception is raised, then it is not
            # PNFS or Chimera mount
            try:
                dataname = os.path.join(mp, ".(tags)()")
                db_fp = file_utils.open(dataname, "r")
                db_fp.close()
            except IOError:
                # This is a normal NFS file system, or the PNFS/Chimera
                # file system is not available at the moment.
                continue

            # We have found a Chimera filesystem.

            # Make up values for Chimera to return that look like PNFS
            # .(get)(database) values.
            mount_name = os.path.basename(mp)
            if mount_name == "fs" or mount_name == "fnal.gov":
                mount_name = "admin"
            db_id = 0  # For Chimera this is always zero.  If this value is
            # ever allowed to change in the future, then
            # Chimera needs to support .(get)(database) files.
            accessible = "enabled"  # enabled or disabled

            # Put the made up values together.
            new_db_data = "%s:%s:r:%s:/%s" % (mount_name, db_id,
                                              accessible,
                                              str(index))

            # Add this to the cached Chimera mount points.
            chimera.add_mtab(new_db_data, None, mp)
        elif fs_type == "lustre":
            # Reserved.
            pass
        else:
            pass

    # Return combined information.  The return format is a dictionary keyed by
    # .(get)(database) information and the value is a two-tuple of PNFS
    # database number and mount point.  [For Chimera the .(get)(database)
    # values are faked to be unique and the database number is None.]
    temp_cache = chimera.mount_points_cache.copy()
    found_mountpoints = {}
    for db_key, db_value in temp_cache.items():
        found_mountpoints[db_key] = db_value
    return found_mountpoints


def process_mtab():
    if not chimera.mount_points_cache:
        # If we haven't read the mount points in yet, do so now.
        parse_mtab()
        # Some filesystems need some extra processing.
        chimera._process_mtab()

    # Get the lists, then put the first items at the beginning.
    chimera_list = chimera.sort_mtab()
    new_list = []
    if len(chimera_list) > 0 and chimera_list[0]:
        new_list.append(chimera_list[0])
    return new_list + chimera_list[1:]


def get_enstore_mount_point(sfs_id=None):

    if not chimera.mount_points_cache:
        process_mtab()

    return chimera.get_enstore_mount_point(sfs_id)


def get_enstore_admin_mount_point(sfs_id=None):

    if not chimera.mount_points_cache:
        process_mtab()
    return chimera.get_enstore_admin_mount_point(sfs_id)

##############################################################################


class NamespaceInterface(option.Interface):

    def __init__(self, args=sys.argv, user_mode=1):
        option.Interface.__init__(self, args=args, user_mode=user_mode)

    pnfs_user_options = {
        option.BFID: {option.HELP_STRING: "lists the bit file id for file",
                      option.DEFAULT_VALUE: option.DEFAULT,
                      option.DEFAULT_NAME: "bfid",
                      option.DEFAULT_TYPE: option.INTEGER,
                      option.VALUE_NAME: "file",
                      option.VALUE_TYPE: option.STRING,
                      option.VALUE_USAGE: option.REQUIRED,
                      option.VALUE_LABEL: "filename",
                      option.FORCE_SET_DEFAULT: option.FORCE,
                      option.USER_LEVEL: option.USER
                      },
        option.CAT: {option.HELP_STRING: "see --layer",
                     option.DEFAULT_VALUE: option.DEFAULT,
                     option.DEFAULT_NAME: "layer",
                     option.DEFAULT_TYPE: option.INTEGER,
                     option.VALUE_NAME: "file",
                     option.VALUE_TYPE: option.STRING,
                     option.VALUE_USAGE: option.REQUIRED,
                     option.VALUE_LABEL: "filename",
                     option.FORCE_SET_DEFAULT: option.FORCE,
                     option.USER_LEVEL: option.USER,
                     option.EXTRA_VALUES: [{option.DEFAULT_VALUE: option.DEFAULT,
                                            option.DEFAULT_NAME: "named_layer",
                                            option.DEFAULT_TYPE: option.INTEGER,
                                            option.VALUE_NAME: "named_layer",
                                            option.VALUE_TYPE: option.INTEGER,
                                            option.VALUE_USAGE: option.OPTIONAL,
                                            option.VALUE_LABEL: "layer",
                                            }]
                     },
        option.FILE_FAMILY: {option.HELP_STRING:
                             "gets file family tag, default; "
                             "sets file family tag, optional",
                             option.DEFAULT_VALUE: option.DEFAULT,
                             option.DEFAULT_NAME: "file_family",
                             option.DEFAULT_TYPE: option.INTEGER,
                             option.VALUE_TYPE: option.STRING,
                             option.USER_LEVEL: option.USER,
                             option.VALUE_USAGE: option.OPTIONAL,
                             },
        option.FILE_FAMILY_WIDTH: {option.HELP_STRING:
                                   "gets file family width tag, default; "
                                   "sets file family width tag, optional",
                                   option.DEFAULT_VALUE: None,
                                   option.DEFAULT_NAME: "file_family_width",
                                   option.DEFAULT_TYPE: option.INTEGER,
                                   option.VALUE_TYPE: option.INTEGER,
                                   option.USER_LEVEL: option.USER,
                                   option.VALUE_USAGE: option.OPTIONAL,
                                   },
        option.FILE_FAMILY_WRAPPER: {option.HELP_STRING:
                                     "gets file family wrapper tag, default; "
                                     "sets file family wrapper tag, optional",
                                     option.DEFAULT_VALUE: option.DEFAULT,
                                     option.DEFAULT_NAME: "file_family_wrapper",
                                     option.DEFAULT_TYPE: option.INTEGER,
                                     option.VALUE_TYPE: option.STRING,
                                     option.USER_LEVEL: option.USER,
                                     option.VALUE_USAGE: option.OPTIONAL,
                                     },
        option.FILESIZE: {option.HELP_STRING: "print out real filesize",
                          option.VALUE_NAME: "file",
                          option.VALUE_TYPE: option.STRING,
                          option.VALUE_LABEL: "file",
                          option.USER_LEVEL: option.USER,
                          option.VALUE_USAGE: option.REQUIRED,
                          },
        option.INFO: {option.HELP_STRING: "see --xref",
                      option.DEFAULT_VALUE: option.DEFAULT,
                      option.DEFAULT_NAME: "xref",
                      option.DEFAULT_TYPE: option.INTEGER,
                      option.VALUE_NAME: "file",
                      option.VALUE_TYPE: option.STRING,
                      option.VALUE_USAGE: option.REQUIRED,
                      option.VALUE_LABEL: "filename",
                      option.USER_LEVEL: option.USER,
                      option.FORCE_SET_DEFAULT: option.FORCE,
                      },
        option.LAYER: {option.HELP_STRING: "lists the layer of the file",
                       option.DEFAULT_VALUE: option.DEFAULT,
                       option.DEFAULT_NAME: "layer",
                       option.DEFAULT_TYPE: option.INTEGER,
                       option.VALUE_NAME: "file",
                       option.VALUE_TYPE: option.STRING,
                       option.VALUE_USAGE: option.REQUIRED,
                       option.VALUE_LABEL: "filename",
                       option.FORCE_SET_DEFAULT: option.FORCE,
                       option.USER_LEVEL: option.USER,
                       option.EXTRA_VALUES: [{option.DEFAULT_VALUE:
                                              option.DEFAULT,
                                              option.DEFAULT_NAME: "named_layer",
                                              option.DEFAULT_TYPE: option.INTEGER,
                                              option.VALUE_NAME: "named_layer",
                                              option.VALUE_TYPE: option.INTEGER,
                                              option.VALUE_USAGE: option.OPTIONAL,
                                              option.VALUE_LABEL: "layer",
                                              }]
                       },
        option.LIBRARY: {option.HELP_STRING: "gets library tag, default; "
                         "sets library tag, optional",
                         option.DEFAULT_VALUE: option.DEFAULT,
                         option.DEFAULT_NAME: "library",
                         option.DEFAULT_TYPE: option.INTEGER,
                         option.VALUE_TYPE: option.STRING,
                         option.USER_LEVEL: option.USER,
                         option.VALUE_USAGE: option.OPTIONAL,
                         },
        option.STORAGE_GROUP: {option.HELP_STRING: "gets storage group tag, "
                               "default; sets storage group tag, optional",
                               option.DEFAULT_VALUE: option.DEFAULT,
                               option.DEFAULT_NAME: "storage_group",
                               option.DEFAULT_TYPE: option.INTEGER,
                               option.VALUE_TYPE: option.STRING,
                               option.USER_LEVEL: option.ADMIN,
                               option.VALUE_USAGE: option.OPTIONAL,
                               },
        option.TAG: {option.HELP_STRING: "lists the tag of the directory",
                     option.DEFAULT_VALUE: option.DEFAULT,
                     option.DEFAULT_NAME: "tag",
                     option.DEFAULT_TYPE: option.INTEGER,
                     option.VALUE_NAME: "named_tag",
                     option.VALUE_TYPE: option.STRING,
                     option.VALUE_USAGE: option.REQUIRED,
                     option.VALUE_LABEL: "tag",
                     option.FORCE_SET_DEFAULT: 1,
                     option.USER_LEVEL: option.USER,
                     option.EXTRA_VALUES: [{option.DEFAULT_VALUE: "",
                                            option.DEFAULT_NAME: "directory",
                                            option.DEFAULT_TYPE: option.STRING,
                                            option.VALUE_NAME: "directory",
                                            option.VALUE_TYPE: option.STRING,
                                            option.VALUE_USAGE: option.OPTIONAL,
                                            option.FORCE_SET_DEFAULT: option.FORCE,
                                            }]
                     },
        option.TAGCHMOD: {option.HELP_STRING: "changes the permissions"
                          " for the tag; use UNIX chmod style permissions",
                          option.DEFAULT_VALUE: option.DEFAULT,
                          option.DEFAULT_NAME: "tagchmod",
                          option.DEFAULT_TYPE: option.INTEGER,
                          option.VALUE_NAME: "permissions",
                          option.VALUE_TYPE: option.STRING,
                          option.VALUE_USAGE: option.REQUIRED,
                          option.FORCE_SET_DEFAULT: option.FORCE,
                          option.USER_LEVEL: option.USER,
                          option.EXTRA_VALUES: [{option.VALUE_NAME: "named_tag",
                                                 option.VALUE_TYPE: option.STRING,
                                                 option.VALUE_USAGE: option.REQUIRED,
                                                 option.VALUE_LABEL: "tag",
                                                 }, ]
                          },
        option.TAGCHOWN: {option.HELP_STRING: "changes the ownership"
                          " for the tag; OWNER can be 'owner' or 'owner.group'",
                          option.DEFAULT_VALUE: option.DEFAULT,
                          option.DEFAULT_NAME: "tagchown",
                          option.DEFAULT_TYPE: option.INTEGER,
                          option.VALUE_NAME: "owner",
                          option.VALUE_TYPE: option.STRING,
                          option.VALUE_USAGE: option.REQUIRED,
                          option.FORCE_SET_DEFAULT: option.FORCE,
                          option.USER_LEVEL: option.USER,
                          option.EXTRA_VALUES: [{option.VALUE_NAME: "named_tag",
                                                 option.VALUE_TYPE: option.STRING,
                                                 option.VALUE_USAGE: option.REQUIRED,
                                                 option.VALUE_LABEL: "tag",
                                                 }, ]
                          },
        option.TAGS: {option.HELP_STRING: "lists tag values and permissions",
                      option.DEFAULT_VALUE: option.DEFAULT,
                      option.DEFAULT_NAME: "tags",
                      option.DEFAULT_TYPE: option.INTEGER,
                      option.VALUE_USAGE: option.IGNORED,
                      option.USER_LEVEL: option.USER,
                      option.EXTRA_VALUES: [{option.DEFAULT_VALUE: "",
                                             option.DEFAULT_NAME: "directory",
                                             option.DEFAULT_TYPE: option.STRING,
                                             option.VALUE_NAME: "directory",
                                             option.VALUE_TYPE: option.STRING,
                                             option.VALUE_USAGE: option.OPTIONAL,
                                             option.FORCE_SET_DEFAULT: option.FORCE,
                                             }]
                      },
        option.XREF: {option.HELP_STRING: "lists the cross reference "
                      "data for file",
                      option.DEFAULT_VALUE: option.DEFAULT,
                      option.DEFAULT_NAME: "xref",
                      option.DEFAULT_TYPE: option.INTEGER,
                      option.VALUE_NAME: "file",
                      option.VALUE_TYPE: option.STRING,
                      option.VALUE_USAGE: option.REQUIRED,
                      option.VALUE_LABEL: "filename",
                      option.USER_LEVEL: option.USER,
                      option.FORCE_SET_DEFAULT: option.FORCE,
                      },
    }

    pnfs_admin_options = {
        option.CP: {option.HELP_STRING: "echos text to named layer of the file",
                    option.DEFAULT_VALUE: option.DEFAULT,
                    option.DEFAULT_NAME: "cp",
                    option.DEFAULT_TYPE: option.INTEGER,
                    option.VALUE_NAME: "unixfile",
                    option.VALUE_TYPE: option.STRING,
                    option.VALUE_USAGE: option.REQUIRED,
                    option.FORCE_SET_DEFAULT: option.FORCE,
                    option.USER_LEVEL: option.ADMIN,
                    option.EXTRA_VALUES: [{option.VALUE_NAME: "file",
                                           option.VALUE_TYPE: option.STRING,
                                           option.VALUE_USAGE: option.REQUIRED,
                                           option.VALUE_LABEL: "filename",
                                           },
                                          {option.VALUE_NAME: "named_layer",
                                           option.VALUE_TYPE: option.INTEGER,
                                           option.VALUE_USAGE: option.REQUIRED,
                                           option.VALUE_LABEL: "layer",
                                           }, ]
                    },
        option.CONST: {option.HELP_STRING: "Return information about the"
                       " underlying database.  Only PNFS returns valid"
                       " information.",
                       option.DEFAULT_VALUE: option.DEFAULT,
                       option.DEFAULT_NAME: "const",
                       option.DEFAULT_TYPE: option.INTEGER,
                       option.VALUE_NAME: "file",
                       option.VALUE_TYPE: option.STRING,
                       option.VALUE_USAGE: option.REQUIRED,
                       option.VALUE_LABEL: "filename",
                       option.FORCE_SET_DEFAULT: option.FORCE,
                       option.USER_LEVEL: option.ADMIN,
                       },
        option.COUNTERS: {option.HELP_STRING: "Return information about the"
                          " underlying database.  Only PNFS returns valid"
                          " information.",
                          option.DEFAULT_VALUE: option.DEFAULT,
                          option.DEFAULT_NAME: "counters",
                          option.DEFAULT_TYPE: option.INTEGER,
                          option.VALUE_NAME: "file",
                          option.VALUE_TYPE: option.STRING,
                          option.VALUE_USAGE: option.REQUIRED,
                          option.VALUE_LABEL: "filename",
                          option.FORCE_SET_DEFAULT: option.FORCE,
                          option.USER_LEVEL: option.ADMIN,
                          },
        option.CURSOR: {option.HELP_STRING: "Return information about the"
                        " underlying database.  Only PNFS returns valid"
                        " information.",
                        option.DEFAULT_VALUE: option.DEFAULT,
                        option.DEFAULT_NAME: "cursor",
                        option.DEFAULT_TYPE: option.INTEGER,
                        option.VALUE_NAME: "file",
                        option.VALUE_TYPE: option.STRING,
                        option.VALUE_USAGE: option.REQUIRED,
                        option.VALUE_LABEL: "filename",
                        option.FORCE_SET_DEFAULT: option.FORCE,
                        option.USER_LEVEL: option.ADMIN,
                        },
        option.DUMP: {option.HELP_STRING: "dumps info",
                      option.DEFAULT_VALUE: option.DEFAULT,
                      option.DEFAULT_NAME: "dump",
                      option.DEFAULT_TYPE: option.INTEGER,
                      option.VALUE_USAGE: option.IGNORED,
                      option.USER_LEVEL: option.ADMIN,
                      },
        option.ECHO: {option.HELP_STRING: "sets text to named layer of the file",
                      option.DEFAULT_VALUE: option.DEFAULT,
                      option.DEFAULT_NAME: "echo",
                      option.DEFAULT_TYPE: option.INTEGER,
                      option.VALUE_NAME: "text",
                      option.VALUE_TYPE: option.STRING,
                      option.VALUE_USAGE: option.REQUIRED,
                      option.FORCE_SET_DEFAULT: option.FORCE,
                      option.USER_LEVEL: option.ADMIN,
                      option.EXTRA_VALUES: [{option.VALUE_NAME: "file",
                                             option.VALUE_TYPE: option.STRING,
                                             option.VALUE_USAGE: option.REQUIRED,
                                             option.VALUE_LABEL: "filename",
                                             },
                                            {option.VALUE_NAME: "named_layer",
                                             option.VALUE_TYPE: option.INTEGER,
                                             option.VALUE_USAGE: option.REQUIRED,
                                             option.VALUE_LABEL: "layer",
                                             }, ]
                      },
        option.ID: {option.HELP_STRING: "prints the pnfs id",
                    option.DEFAULT_VALUE: option.DEFAULT,
                    option.DEFAULT_NAME: "id",
                    option.DEFAULT_TYPE: option.INTEGER,
                    option.VALUE_NAME: "file",
                    option.VALUE_TYPE: option.STRING,
                    option.VALUE_USAGE: option.REQUIRED,
                    option.VALUE_LABEL: "filename",
                    option.FORCE_SET_DEFAULT: option.FORCE,
                    option.USER_LEVEL: option.USER2,
                    },
        option.IO: {option.HELP_STRING: "sets io mode (can't clear it easily)",
                    option.DEFAULT_VALUE: option.DEFAULT,
                    option.DEFAULT_NAME: "io",
                    option.DEFAULT_TYPE: option.INTEGER,
                    option.VALUE_NAME: "file",
                    option.VALUE_TYPE: option.STRING,
                    option.VALUE_USAGE: option.REQUIRED,
                    option.VALUE_LABEL: "filename",
                    option.FORCE_SET_DEFAULT: option.FORCE,
                    option.USER_LEVEL: option.ADMIN,
                    },
        option.LS: {option.HELP_STRING: "does an ls on the named layer "
                    "in the file",
                    option.DEFAULT_VALUE: option.DEFAULT,
                    option.DEFAULT_NAME: "ls",
                    option.DEFAULT_TYPE: option.INTEGER,
                    option.VALUE_NAME: "file",
                    option.VALUE_TYPE: option.STRING,
                    option.VALUE_USAGE: option.REQUIRED,
                    option.VALUE_LABEL: "filename",
                    option.FORCE_SET_DEFAULT: option.FORCE,
                    option.USER_LEVEL: option.ADMIN,
                    option.EXTRA_VALUES: [{option.DEFAULT_VALUE: option.DEFAULT,
                                           option.DEFAULT_NAME: "named_layer",
                                           option.DEFAULT_TYPE: option.INTEGER,
                                           option.VALUE_NAME: "named_layer",
                                           option.VALUE_TYPE: option.STRING,
                                           option.VALUE_USAGE: option.OPTIONAL,
                                           option.VALUE_LABEL: "layer",
                                           }]
                    },
        option.MOUNT_POINT: {option.HELP_STRING: "prints the mount point of "
                             "the pnfs file or directory",
                             option.DEFAULT_VALUE: option.DEFAULT,
                             option.DEFAULT_NAME: "mount_point",
                             option.DEFAULT_TYPE: option.INTEGER,
                             option.VALUE_NAME: "file",
                             option.VALUE_TYPE: option.STRING,
                             option.VALUE_USAGE: option.REQUIRED,
                             option.VALUE_LABEL: "filename",
                             option.FORCE_SET_DEFAULT: option.FORCE,
                             option.USER_LEVEL: option.USER2,
                             },
        option.NAMEOF: {option.HELP_STRING: "prints the filename of the PNFS ID"
                        " or Chimera ID.  (CWD must be under /pnfs)",
                        option.DEFAULT_VALUE: option.DEFAULT,
                        option.DEFAULT_NAME: "nameof",
                        option.DEFAULT_TYPE: option.INTEGER,
                        option.VALUE_NAME: "pnfs_id",
                        option.VALUE_TYPE: option.STRING,
                        option.VALUE_USAGE: option.REQUIRED,
                        option.FORCE_SET_DEFAULT: option.FORCE,
                        option.USER_LEVEL: option.ADMIN,
                        },
        option.PARENT: {option.HELP_STRING: "prints the PNFS ID or Chimera ID"
                        "of the parent directory (CWD must be under /pnfs)",
                        option.DEFAULT_VALUE: option.DEFAULT,
                        option.DEFAULT_NAME: "parent",
                        option.DEFAULT_TYPE: option.INTEGER,
                        option.VALUE_NAME: "pnfs_id",
                        option.VALUE_TYPE: option.STRING,
                        option.VALUE_USAGE: option.REQUIRED,
                        option.FORCE_SET_DEFAULT: option.FORCE,
                        option.USER_LEVEL: option.ADMIN,
                        },
        option.PATH: {option.HELP_STRING:
                      "prints the file path of the PNFS id or Chimera ID.  "
                      "(CWD must be under /pnfs)",
                      option.DEFAULT_VALUE: option.DEFAULT,
                      option.DEFAULT_NAME: "path",
                      option.DEFAULT_TYPE: option.INTEGER,
                      option.VALUE_NAME: "pnfs_id",
                      option.VALUE_TYPE: option.STRING,
                      option.VALUE_USAGE: option.REQUIRED,
                      option.FORCE_SET_DEFAULT: option.FORCE,
                      option.USER_LEVEL: option.ADMIN,
                      },
        option.POSITION: {option.HELP_STRING: "Return information about the"
                          " underlying database.  Only PNFS returns valid"
                          " information.",
                          option.DEFAULT_VALUE: option.DEFAULT,
                          option.DEFAULT_NAME: "position",
                          option.DEFAULT_TYPE: option.INTEGER,
                          option.VALUE_NAME: "file",
                          option.VALUE_TYPE: option.STRING,
                          option.VALUE_USAGE: option.REQUIRED,
                          option.VALUE_LABEL: "filename",
                          option.FORCE_SET_DEFAULT: option.FORCE,
                          option.USER_LEVEL: option.ADMIN,
                          },
        option.RM: {option.HELP_STRING: "deletes (clears) named layer of the file",
                    option.DEFAULT_VALUE: option.DEFAULT,
                    option.DEFAULT_NAME: "rm",
                    option.DEFAULT_TYPE: option.INTEGER,
                    option.VALUE_NAME: "file",
                    option.VALUE_TYPE: option.STRING,
                    option.VALUE_USAGE: option.REQUIRED,
                    option.VALUE_LABEL: "filename",
                    option.FORCE_SET_DEFAULT: option.FORCE,
                    option.USER_LEVEL: option.ADMIN,
                    option.EXTRA_VALUES: [{option.VALUE_NAME: "named_layer",
                                           option.VALUE_TYPE: option.INTEGER,
                                           option.VALUE_USAGE: option.REQUIRED,
                                           option.VALUE_LABEL: "layer",
                                           }, ]
                    },
        option.SHOWID: {option.HELP_STRING: "prints the PNFS ID information",
                        option.DEFAULT_VALUE: option.DEFAULT,
                        option.DEFAULT_NAME: "showid",
                        option.DEFAULT_TYPE: option.INTEGER,
                        option.VALUE_NAME: "pnfs_id",
                        option.VALUE_TYPE: option.STRING,
                        option.VALUE_USAGE: option.REQUIRED,
                        option.FORCE_SET_DEFAULT: option.FORCE,
                        option.USER_LEVEL: option.ADMIN,
                        },
        option.SIZE: {option.HELP_STRING: "sets the size of the file",
                      option.DEFAULT_VALUE: option.DEFAULT,
                      option.DEFAULT_NAME: "size",
                      option.DEFAULT_TYPE: option.INTEGER,
                      option.VALUE_NAME: "file",
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
        option.TAGECHO: {option.HELP_STRING: "echos text to named tag",
                         option.DEFAULT_VALUE: option.DEFAULT,
                         option.DEFAULT_NAME: "tagecho",
                         option.DEFAULT_TYPE: option.INTEGER,
                         option.VALUE_NAME: "text",
                         option.VALUE_TYPE: option.STRING,
                         option.VALUE_USAGE: option.REQUIRED,
                         option.FORCE_SET_DEFAULT: option.FORCE,
                         option.USER_LEVEL: option.ADMIN,
                         option.EXTRA_VALUES: [{option.VALUE_NAME: "named_tag",
                                                option.VALUE_TYPE: option.STRING,
                                                option.VALUE_USAGE: option.REQUIRED,
                                                option.VALUE_LABEL: "tag",
                                                }, ]
                         },

        option.TAGRM: {option.HELP_STRING: "removes the tag (tricky, see DESY "
                       "documentation)",
                       option.DEFAULT_VALUE: option.DEFAULT,
                       option.DEFAULT_NAME: "tagrm",
                       option.DEFAULT_TYPE: option.INTEGER,
                       option.VALUE_NAME: "named_tag",
                       option.VALUE_TYPE: option.STRING,
                       option.VALUE_USAGE: option.REQUIRED,
                       option.VALUE_LABEL: "tag",
                       option.FORCE_SET_DEFAULT: option.FORCE,
                       option.USER_LEVEL: option.ADMIN,
                       },
    }

    def valid_dictionaries(self):
        return (self.help_options, self.pnfs_user_options,
                self.pnfs_admin_options)

    # parse the options like normal but make sure we have other args
    def parse_options(self):
        self.pnfs_id = ""  # Assume the command is a dir and/or file.
        self.file = ""
        self.dir = ""
        option.Interface.parse_options(self)

        if not self.option_list:
            self.print_usage("No valid options were given.")

        # No pnfs options take extra arguments beyond those specifed in the
        # option dictionaries.  If there are print message and exit.
        self.check_correct_count()

        if getattr(self, "help", None):
            self.print_help()

        if getattr(self, "usage", None):
            self.print_usage()

##############################################################################


def do_work(intf):
    rtn = 0

    try:
        if intf.file:
            p = StorageFS(intf.file)
            t = None
            n = None
        elif intf.pnfs_id:
            p = StorageFS(intf.pnfs_id, shortcut=True)
            t = None
            n = None
        else:
            p = None
            if intf.dir:
                t = Tag(intf.dir)
            elif hasattr(intf, "directory") and intf.directory:
                t = Tag(intf.directory)
            else:
                t = Tag(os.getcwd())
            n = None
    except OSError as msg:
        print(str(msg))
        return 1

    for arg in intf.option_list:
        if string.replace(arg, "_", "-") in intf.options.keys():
            arg = string.replace(arg, "-", "_")
            for instance in [t, p, n]:
                if getattr(instance, "p" + arg, None):
                    try:
                        # Not all functions use/need intf passed in.
                        rtn = getattr(instance, "p" + arg)(*())
                    except TypeError:
                        rtn = getattr(instance, "p" + arg)(*(intf,))
                    break
            else:
                print("p%s not found" % arg)
                rtn = 1

    return rtn


##############################################################################
if __name__ == "__main__":

    intf = NamespaceInterface(user_mode=0)

    intf._mode = "admin"

    sys.exit(do_work(intf))
