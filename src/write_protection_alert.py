#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# a replacement for write protect alarm part (wpa) in inventory
# Author: Dmitry Litvintsev (litvinse@fnal.gov) 10/09
#
###############################################################################

# system imports
import getopt
import sys
import pg
import configuration_client
import os
import time

# enstore imports

NON_EXEMPT = "select label, system_inhibit_1,date(si_time_1),library, media_type, \
CASE when write_protected='n' \
     then 'OFF' \
     when write_protected='y'\
     then 'ON' \
     else  \
	'--' \
END as wp from volume where system_inhibit_0!='DELETED' and \
	system_inhibit_1 in (%s)  and \
	media_type in (%s)  and \
        library not in (%s) and \
	write_protected='n' and \
	storage_group  not in (select storage_group from no_flipping_storage_group) and \
	(storage_group,file_family) not in (select storage_group,file_family from no_flipping_file_family)"

ORDER = "order by si_time_1,label,library,media_type"
NOT_SHELF = "library not like 'shelf%'"

EXEMPT = "select label, system_inhibit_1,date(si_time_1),library, media_type, \
CASE when write_protected='n' \
     then 'OFF' \
     when write_protected='y'\
     then 'ON' \
     else  \
	'--' \
END as wp from volume  \
where system_inhibit_0!='DELETED' and \
	system_inhibit_1 in (%s)  and \
	media_type in (%s)  and \
        library not in (%s) and \
	write_protected='n' and \
	(storage_group in (select storage_group from no_flipping_storage_group) or \
	(storage_group,file_family) in (select storage_group,file_family from no_flipping_file_family))"

ALL = "select count(*),library from volume where system_inhibit_0!='DELETED' and \
library not in (%s) "

SHOULD = "select count(*),library from volume where system_inhibit_0!='DELETED' and \
	system_inhibit_1 in (%s)  and \
	media_type in (%s)  and \
        library not in (%s) and \
	storage_group  not in (select storage_group from no_flipping_storage_group) and \
	(storage_group,file_family) not in (select storage_group,file_family from no_flipping_file_family)"

GROUP = "group by library"


def print_common_header(fp):
    command_name = os.path.basename(sys.argv[0])

    fp.write("Date this listing was generated: %s\n" %
             (time.ctime(time.time())))
    fp.write("Brought to You by: %s\n\n" % (command_name,))


def print_write_protect_alert_header(fp):
    print_common_header(fp)

    wpa_format = "%-16s %-12s %-18s %-16s %-16s %-16s %-3s\n\n"
    wpa_titles = (
        "volume",
        "state",
        "time",
        "library",
        "media type",
        "wp",
        "exemption")
    fp.write(wpa_format % wpa_titles)


WPA_FORMAT = "%-16s %-12s %-18s %-16s %-16s %-16s %-3s\n"


def prepare_query(query, wpa_states, wpa_media_types, wpa_excluded_libraries):
    states = ""
    for state in wpa_states:
        states = states + "'" + state + "',"
    types = ""
    for type in wpa_media_types:
        types = types + "'" + type + "',"
    excluded_libraries = ""
    for lib in wpa_excluded_libraries:
        excluded_libraries = excluded_libraries + "'" + lib + "',"
    return query % (states[:-1], types[:-1], excluded_libraries[:-1],)


def prepare_query1(query, wpa_excluded_libraries):
    excluded_libraries = ""
    for lib in wpa_excluded_libraries:
        excluded_libraries = excluded_libraries + "'" + lib + "',"
    return query % (excluded_libraries[:-1],)


FILE_NAME = "/tmp/WRITE_PROTECTION_ALERT_NEW"


def do_work(file_name):
    csc = configuration_client.ConfigurationClient()
    inventory = csc.get('inventory', timeout=15, retry=3)
    inventory_rcp_dir = inventory.get('inventory_rcp_dir', None)
    wpa_states = inventory.get('wpa_states', ["full", "readonly"])
    wpa_media_types = inventory.get(
        'wpa_media_types', [
            "9940", "9940B", "LTO3", "LTO4"])
    wpa_excluded_libraries = inventory.get(
        'wpa_excluded_libraries', [
            "null1", "disk", "test", "CD-LTO4G1T"])
    if inventory_rcp_dir is None:
        sys.stderr.write("Destination directory is not defined \n")
        sys.stderr.flush()
        sys.exit(1)
    enstoredb = csc.get("database", {})
    db = pg.DB(host=enstoredb.get('db_host', "localhost"),
               dbname=enstoredb.get('dbname', "enstoredb"),
               port=enstoredb.get('db_port', 5432),
               user=enstoredb.get('dbuser_reader', "enstore_reader"))
    fp = open(file_name, "w")

    # get should
    q = prepare_query(
        SHOULD,
        wpa_states,
        wpa_media_types,
        wpa_excluded_libraries)
    q = q + " and " + NOT_SHELF + " " + GROUP
    should_library_counts = {}
    for res in db.query(q).getresult():
        should_library_counts[res[1]] = res[0]
    to_do_library_counts = {}
    for key in should_library_counts.keys():
        to_do_library_counts[key] = 0
    # get non exempt
    print_write_protect_alert_header(fp)
    q = prepare_query(
        NON_EXEMPT,
        wpa_states,
        wpa_media_types,
        wpa_excluded_libraries)
    q = q + " and " + NOT_SHELF + " " + ORDER
    for res in db.query(q).getresult():
        if res[3] in to_do_library_counts:
            to_do_library_counts[res[3]] = to_do_library_counts[res[3]] + 1
        wpa_values = (res[0], res[1], res[2], res[3], res[4], res[5], "NO")
        fp.write(WPA_FORMAT % wpa_values)
    # get exempt
    q = prepare_query(
        EXEMPT,
        wpa_states,
        wpa_media_types,
        wpa_excluded_libraries)
    q = q + " and " + NOT_SHELF + " " + ORDER
    for res in db.query(q).getresult():
        wpa_values = (res[0], res[1], res[2], res[3], res[4], res[5], "YES")
        fp.write(WPA_FORMAT % wpa_values)
    # get all
    q = prepare_query1(ALL, wpa_excluded_libraries)
    q = q + " and " + NOT_SHELF + " " + GROUP
    all_library_counts = {}
    for res in db.query(q).getresult():
        all_library_counts[res[1]] = res[0]
    # get total number of volumes
    total = db.query(
        "select count(*) from volume where system_inhibit_0 != 'DELETED'").getresult()[0][0]
    fp.write("\n\n")
    fp.write("Total: %5d\n" % (total))
    for key in should_library_counts.keys():
        fp.write("\n%s:\n----------------\n" % (key))
        wpa_format = "  Total: %5d\n Should: %5d\n   Done: %5d\nNot yet: %5d\n  Ratio: %5.2f%%\n"
        wpa_values = (all_library_counts[key],
                      should_library_counts[key],
                      should_library_counts[key] - to_do_library_counts[key],
                      to_do_library_counts[key], float((should_library_counts[key] - to_do_library_counts[key]) * 100. / should_library_counts[key]))
        fp.write(wpa_format % wpa_values)
    fp.close()
    db.close()

    # if inventory_rcp_dir:
    #    os.system("enrcp %s %s" % (FILE_NAME, inventory_rcp_dir,))


if __name__ == "__main__":
    do_work(FILE_NAME)
