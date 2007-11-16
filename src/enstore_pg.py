#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import enstore_constants
import pg

def acc_encp_lines(csc, numEncps=100):
    acc = csc.get(enstore_constants.ACCOUNTING_SERVER)
    # connect to the db
    try:
        dbport = acc.get('dbport', None)
        if dbport:
            db = pg.DB(host=acc['dbhost'], port=dbport, dbname=acc['dbname'])
        else:
            db = pg.DB(host=acc['dbhost'], dbname=acc['dbname'])
    except pg.Error, detail:
        # could not connect to the db
        print " %s  %s"%(pg.Error, detail)
        return []
    res = db.query("select * from encp_xfer order by date desc limit %s;"%(numEncps,))
    res_err = db.query("select * from encp_error order by date desc limit %s;"%(numEncps,))
    # combine the 2 lists and take the top numEncps entries to return
    res_total = res.getresult() + res_err.getresult()
    res_total.sort()
    # close connection to the db
    db.close()
    # return the results as a list, returning only the last numEncps elements.
    return res_total[-numEncps:]
