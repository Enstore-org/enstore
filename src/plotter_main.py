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

# system imports
import getopt
import sys

# enstore imports
import enstore_plotter_framework
import ratekeeper_plotter_module
import drive_utilization_plotter_module
import slots_usage_plotter_module
import mounts_plotter_module
import pnfs_backup_plotter_module
import file_family_analysis_plotter_module
import encp_rate_multi_plotter_module
import quotas_plotter_module
import tapes_burn_rate_plotter_module
import migration_summary_plotter_module


def usage(cmd):
    print "Usage: %s [options] "%(cmd,)
    print "\t -r [--rate]        : plot ratekeeper plots"
    print "\t -m [--mounts]      : plot mount plots "
    print "\t -u [--utilization] : plot drive utilization (old name)"
    print "\t -d [--drives]      : plot drive utilization"
    print "\t -s [--slots]       : plot slot utilization"
    print "\t -p [--pnfs-backup] : plot pnfs backup time"
    print "\t -p [--file-family-analysis] : plot file family analysis"
    print "\t -e [--encp-rate-multi] : plot multiple encp rates"
    print "\t -q [--quotas]      : plot quotas by storage group"
    print "\t -i [--migration-summary] : plot migration progress"
    print "\t -h [--help]        : show this message"
    
if __name__ == "__main__":
    try:
        short_args = "hmrudspfeqti"
        long_args = ["help", "mounts", "rate", "utilization", "drives",
                     "slots", "pnfs-bakup", "file-family-analysis",
                     "quotas", "tapes-burn-rate", "migration-summary"]
        opts, args = getopt.getopt(sys.argv[1:], short_args, long_args)
    except getopt.GetoptError, msg:
        print msg
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
            aModule = mounts_plotter_module.MountsPlotterModule("mounts")
            f.add(aModule)
        # ratekeeper plots
        if o in ("-r","--rate"):
            aModule = ratekeeper_plotter_module.RateKeeperPlotterModule("ratekeeper")
            f.add(aModule)
        # drive utilization
        if o in ("-u","--utilization", "-d", "--drives"):
            aModule = drive_utilization_plotter_module.DriveUtilizationPlotterModule("utilization")
            f.add(aModule)
        # slot utilization
        if o in ("-s","--slots"):
            aModule   = slots_usage_plotter_module.SlotUsagePlotterModule("slots")
            f.add(aModule)
        # pnfs backup time
        if o in ("-p","--pnfs-backup"):
            aModule = pnfs_backup_plotter_module.PnfsBackupPlotterModule("pnfs_backup")
            f.add(aModule)
        # file family analysis
        if o in ("-f","--file-family-analysis"):
            aModule = file_family_analysis_plotter_module.FileFamilyAnalysisPlotterModule("file_family_analisys")
            f.add(aModule)
        # encp rate multi
        if o in ("-e","--encp-rate-multi"):
            aModule = encp_rate_multi_plotter_module.EncpRateMultiPlotterModule("encp_rate_multi")
            f.add(aModule)
        # quotas
        if o in ("-q","--quotas"):
            aModule = quotas_plotter_module.QuotasPlotterModule("quotas")
            f.add(aModule)
        # tapes burn rate
        if o in ("-t","--tapes-burn-rate"):
            aModule = tapes_burn_rate_plotter_module.TapesBurnRatePlotterModule("tapes_burn_rate")
            f.add(aModule)
        # migration summary
        if o in ("-i","--migration-summary"):
            aModule = migration_summary_plotter_module.MigrationSummaryPlotterModule("migration_summary")
            f.add(aModule)

    f.do_work()
