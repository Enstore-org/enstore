#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import enstore_constants
#import pg
import accounting_client
import e_errors
import Trace

def acc_encp_lines(csc, numEncps=100):
    acc = accounting_client.accClient(csc)
    r = acc.last_xfers(numEncps)
    if r and r['status'][0] == e_errors.OK:
        res=r['result']
    else:
        Trace.log(e_errors.WARNING, "Can not get Last xfers: %s"%(r,))
        res = []
    r = acc.last_bad_xfers(numEncps)
    if r and r['status'][0] == e_errors.OK:
                res_err=r['result']
    else:
       Trace.log(e_errors.WARNING, "Can not get Last bad xfers: %s"%(r,))
       res_err = [] 
    # combine the 2 lists and take the top numEncps entries to return
    res_total = res + res_err
    res_total.sort()
    # return the results as a list, returning only the last numEncps elements.
    return res_total[-numEncps:]
