#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# generic framework class 
# Author: Dmitry Litvintsev (litvinse@fnal.gov) 08/05
#
###############################################################################
import enstore_plotter_module
import enstore_plotter_framework
import ratekeeper_plotter_module
import mounts_plot
import getopt
import sys
import socket
import pg
import traceback

FNAL_DOMAIN="131.225" 

def usage(cmd):
    print "Usage: %s -s [--sleep=] "%(cmd,)
    print "\t --sleep : sampling interval in seconds"

    
if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hs:ms:rs", ["help","mounts","rate"])
    except getopt.GetoptError:
        print "Failed to process arguments"
        usage(sys.argv[0])
        sys.exit(2)

    f = enstore_plotter_framework.EnstorePlotterFramework()

    for o, a in opts:
        if o in ("-h", "--help"):
            usage(sys.argv[0])
            sys.exit(1)
        if o in ("-m", "--mounts"):
            aModule   = mounts_plot.MountsPlot("mounts")
            f.add(aModule)
            acc = f.get_configuration_client().get('database', {})
            try:
                db = pg.DB(host  = acc.get('db_host', "localhost"),
                           dbname= acc.get('dbname', "enstoredb"),
                           port  = acc.get('db_port', 5432),
                           user  = acc.get('dbuser', "enstore"))
                for row in db.query("select distinct library from volume where media_type!='null'").getresult():
                    if not row:
                        continue
                    aModule = mounts_plot.MountsPlot("mounts")
                    aModule.add_parameter("library","row[0]");
                    f.add(aModule)
                db.close()
            except:
                exc,msg,tb=sys.exc_info()
                for l in traceback.format_exception( exc, msg, tb ):
                    print l
                
        if o in ("-r","--rate"):
            aModule   = ratekeeper_plotter_module.RateKeeperPlotterModule("ratekeeper")
            f.add(aModule)
            aModule.setActive(False)

    f.do_work()
