#!/usr/bin/env python

###############################################################################
#
# $Id$
#
# this script generates list of recent files on tape
#
###############################################################################

# system imports
from edb import timestamp2time
import enstore_functions2

import time
import sys
import os
import pg
import errno
import shutil

# enstore modules
import option
import configuration_client
import file_clerk_client
import e_errors
import log_trans_fail  # for copy_it


DURATION = 72  # hours
PREFIX = 'RECENT_FILES_ON_TAPE_'


class RecentFilesOnTapeInterface(option.Interface):
    def __init__(self, args=sys.argv, user_mode=0):

        self.duration = DURATION  # hours
        self.output_dir = None
        option.Interface.__init__(self, args=args, user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.rfl_options)

    #  define our specific parameters
    parameters = [
        "[[storage_group1 [storage_group2] ...]]",
    ]

    rfl_options = {
        option.DURATION: {option.HELP_STRING:
                          "Duration in hours to report.  "
                          "(Default 12 hours)",
                          option.VALUE_USAGE: option.REQUIRED,
                          option.VALUE_TYPE: option.INTEGER,
                          option.USER_LEVEL: option.ADMIN, },
        option.OUTPUT_DIR: {option.HELP_STRING:
                            "Specify a directory to place the output.  "
                            "(Default is the tape_inventory dir.)",
                            option.VALUE_USAGE: option.REQUIRED,
                            option.VALUE_TYPE: option.STRING,
                            option.USER_LEVEL: option.ADMIN, },
    }


def make_recent_file(storage_group,
                     duration,
                     database,
                     out_dir,
                     temp_dir):

    out_file = os.path.join(out_dir, PREFIX + storage_group)
    temp_file = os.path.join(temp_dir, PREFIX + storage_group + ".temp")

    f = open(temp_file, 'w')
    head = "Recent (packed and package ) written to tape in the last %s hours for storage group %s" % (
        duration, storage_group)
    f.write("Date this listing was generated: %s\n" % time.ctime())
    f.write("Brought to You by: %s\n" % (os.path.basename(sys.argv[0]),))
    f.write("\n%s\n\n" % (head))
    f.close()
    #
    # Note, this query does not return "direct encped files" as
    # they have NULL for archive status.
    #
    query = "SELECT coalesce(to_char(f.archive_mod_time, 'YYYY-MM-DD HH24:MI:SS'), \
	to_char(f.update,'YYYY-MM-DD HH24:MI:SS')), v.storage_group, v.file_family, \
	CASE WHEN  f.package_id is not null and f.package_id <> f.bfid \
	THEN (select label from file, volume where volume.id=file.volume and file.bfid=f.package_id and file.archive_status = 'ARCHIVED') \
	ELSE v.label \
	END as volume, \
	CASE when f.package_id is not null and f.package_id <> f.bfid \
	THEN (select location_cookie from file where bfid=f.package_id and file.archive_status = 'ARCHIVED') \
	ELSE f.location_cookie \
	END as location_cookie, \
	f.bfid, f.size, f.crc, f.pnfs_id, f.pnfs_path, coalesce(f.archive_status,'ARCHIVED') \
        FROM file f, volume v \
	WHERE f.volume=v.id and v.system_inhibit_0 != 'DELETED' and f.deleted = 'n' \
	and v.storage_group='%s' \
	and f.update > CURRENT_TIMESTAMP - INTERVAL '%s hours'  \
	and f.bfid not in (select bfid from files_in_transition) \
	ORDER by v.file_family, volume, location_cookie, bfid desc" % (storage_group, duration,)

    cmd = "psql -A -F ' ' -p %d -h %s -U %s %s -c \"%s\" >> %s" % (
        database['db_port'], database['db_host'],
        database['dbuser'], database['dbname'], query, temp_file)
    os.system(cmd)

    if os.access(out_file, os.F_OK):
        f_stat = os.stat(out_file)
        time_tuple = time.localtime(f_stat.st_mtime)
        Y = str(time_tuple.tm_year)
        m = "{0:02d}".format(time_tuple.tm_mon)
        d = "{0:02d}".format(time_tuple.tm_mday)
        save_dir = os.path.join(out_dir, Y, m, d)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        shutil.move(out_file, save_dir)

    try:
        os.rename(temp_file, out_file)  # Do the temp file swap.
    except (OSError, IOError) as msg:
        if msg.errno == errno.EXDEV:
            log_trans_fail.copy_it(temp_file, out_file)
        else:
            raise


def main(intf):
    # Get some configuration information.
    csc = configuration_client.ConfigurationClient(
        (intf.config_host, intf.config_port))
    database = csc.get('database')
    if not e_errors.is_ok(database):
        sys.stdout.write("No database information.\n")
        sys.exit(1)
    crons_dict = csc.get('crons')
    if not e_errors.is_ok(crons_dict):
        sys.stdout.write("No crons information.\n")
        sys.exit(1)
    temp_dir = crons_dict.get("tmp_dir", "/tmp")
    sg_list = []
    for item in intf.args:
        sg_list.append(item)
    # If no storage groups on the command line, do all of them.
    if not sg_list:  # Get the connection to the database.
        edb = pg.DB(
            host=database.get('dbhost', "localhost"),
            port=database.get('dbport', 5432),
            dbname=database.get('dbname', "accounting"),
            user=database.get('dbuser', "enstore"),
        )
        q = "select distinct storage_group from volume;"
        res = edb.query(q).getresult()
        for row in res:
            # row[0] is the storage_group
            sg_list.append(row[0])

    # By default stuff things into the tape_inventory directory,
    # however if the user specifies a different directory, use that.
    if intf.output_dir:
        if not os.path.exists(intf.output_dir):
            sys.stdout.write("Output directory not found.\n")
            sys.exit(1)
        out_dir = intf.output_dir
    else:
        # Make the default path the tape inventory dir.  Put only
        # if the html_dir exists.
        if not crons_dict.get("html_dir", None):
            sys.stdout.write("No html_dir information.\n")
            sys.exit(1)
        if not os.path.exists(crons_dict["html_dir"]):
            sys.stdout.write(
                "No html_dir found.  Consider using "
                "--output-dir.\n")
            sys.exit(1)
        inventory_dir = os.path.join(crons_dict["html_dir"],
                                     "tape_inventory")
        if not os.path.exists(inventory_dir):
            os.mkdir(inventory_dir)

        out_dir = inventory_dir

    # Make the page for each storage group.
    for sg in sg_list:
        if sg == "cms":
            continue
        make_recent_file(sg, intf.duration, database,
                         out_dir, temp_dir)


if __name__ == '__main__':
    # Get inforation from the Enstore servers.
    rfl_intf = RecentFilesOnTapeInterface()

    main(rfl_intf)
