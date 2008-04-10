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

import getopt
import sys
import socket
import pg
import traceback

import enstore_plotter_module
import enstore_plotter_framework
import ratekeeper_plotter_module
import drive_utilization_plotter_module
import slots_usage_plotter_module
import mounts_plot
import pnfs_backup_plotter_module

FNAL_DOMAIN="131.225" 

def usage(cmd):
    print "Usage: %s -m [--mounts] -r [--rate] -u [--utilization] "%(cmd,)
    print "\t -r [--rate]        : plot ratekeeper plots"
    print "\t -m [--mounts]      : plot mount plots "
    print "\t -u [--utilization] : plot drive utilization"
    print "\t -s [--slots]       : plot slot utilization"
    print "\t -p [--pnfs-backup] : plot pnfs backup time"
    print "\t -h [--help]        : show this message"
    
if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "phs:ms:rs:s:ss", ["help","mounts","rate","utilization","slots", "pnfs-bakup"])
    except getopt.GetoptError:
        print "Failed to process arguments"
        usage(sys.argv[0])
        sys.exit(2)

    f = enstore_plotter_framework.EnstorePlotterFramework()

    for o, a in opts:
        if o in ("-h", "--help"):
            usage(sys.argv[0])
            sys.exit(1)
        # mounts plots
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
                    if not row:                        continue
                    aModule = mounts_plot.MountsPlot("mounts")
                    aModule.add_parameter("library",row[0]);
                    f.add(aModule)
                db.close()
            except:
                exc,msg,tb=sys.exc_info()
                for l in traceback.format_exception( exc, msg, tb ):
                    print l
        # ratekeeper plots
        if o in ("-r","--rate"):
            aModule   = ratekeeper_plotter_module.RateKeeperPlotterModule("ratekeeper")
            f.add(aModule)
        if o in ("-u","--utilization"):
            aModule   = drive_utilization_plotter_module.DriveUtilizationPlotterModule("utilization")
            f.add(aModule)
        # slot utilization
        if o in ("-s","--slots"):
            aModule   = slots_usage_plotter_module.SlotUsagePlotterModule("slots")
            f.add(aModule)
        # pnfs backup time
        if o in ("-p","--pnfs-backup"):
            aModule   = pnfs_backup_plotter_module.PnfsBackupPlotterModule("pnfs_backup")
            f.add(aModule)
        

    f.do_work()
