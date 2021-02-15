#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

from __future__ import print_function
import enstore_constants
import pg


def acc_encp_lines(csc, numEncps=100):
    acc = csc.get(enstore_constants.ACCOUNTING_SERVER)
    # connect to the db
    try:
        dbport = acc.get('dbport', None)
        if dbport:
            db = pg.DB(host=acc['dbhost'],
                       port=dbport,
                       dbname=acc['dbname'],
                       user=acc['dbuser'])
        else:
            db = pg.DB(host=acc['dbhost'],
                       dbname=acc['dbname'],
                       user=acc['dbuser'])
    except pg.Error as detail:
        # could not connect to the db
        print(" %s  %s" % (pg.Error, detail))
        return []
    res = db.query("select  date, node, pid, username, src, dst, size, rw, overall_rate, network_rate, drive_rate, volume, mover, drive_id, drive_sn, elapsed,  media_changer, mover_interface, driver, storage_group, encp_ip, encp_id, disk_rate, transfer_rate, encp_version, file_family, wrapper from encp_xfer order by date desc limit %s;" % (numEncps,))
    res_err = db.query(
        "select date, node, pid, username, encp_id, version, type, error, src, dst, size, storage_group, file_family, wrapper,  mover, drive_id,  drive_sn,  rw, volume from encp_error order by date desc limit %s;" %
        (numEncps,))
    # combine the 2 lists and take the top numEncps entries to return
    res_total = sorted(res.dictresult() + res_err.dictresult())
    # close connection to the db
    db.close()
    # return the results as a list, returning only the last numEncps elements.
    return res_total[-numEncps:]
