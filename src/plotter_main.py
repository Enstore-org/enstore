#!/usr/bin/env python

###############################################################################
#
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
import files_rw_plotter_module
import files_rw_sep_plotter_module
import encp_rate_multi_plotter_module
import quotas_plotter_module
import tapes_burn_rate_plotter_module
import migration_summary_plotter_module
import bytes_per_day_plotter_module
import mover_summary_plotter_module
import mount_latency_plotter_module
import mounts_per_robot_plotter_module
import drive_hours_plotter_module
import drive_hours_sep_plotter_module

def usage(cmd):
    print "Usage: %s [options] "%(cmd,)
    print "\t -r [--rate]            : plot ratekeeper plots"
    print "\t -m [--mounts]          : plot mount plots "
    print "\t -u [--utilization]     : plot drive utilization (old name)"
    print "\t -d [--drives]          : plot drive utilization"
    print "\t -D [--drive-hours]     : plot drive hours per day, stacked by storage group"
    print "\t -H [--drive-hours-sep] : plot drive hours per day, separately for each storage group"
    print "\t -s [--slots]           : plot slot utilization"
    print "\t -p [--pnfs-backup]     : plot pnfs backup time"
    print "\t -f [--file-family-analysis] : plot file family analysis"
    print "\t -F [--files-rw]        : plot file reads and writes per mount, stacked by storage group"
    print "\t -W [--files-rw-sep]    : plot file reads and writes per mount, separately for each storage group"
    print "\t -e [--encp-rate-multi] : plot multiple encp rates"
    print "\t -q [--quotas]          : plot quotas by storage group"
    print "\t -t [--tapes-burn-rate] : plot tape usage by storage group"
    print "\t -i [--migration-summary] : plot migration progress"
    print "\t -b [--bytes-per-day]   : plot bytes transferred per day"
    print "\t -M [--mover-summary]   : plot mover summary"
    print "\t -L [--library-mounts]  : plot tape library mounts"
    print "\t -l [--latencies]       : plot latencies plot"
    print "\t -S [--sfa-stats]       : plot Small Files Aggregation Statistics"
    print "\t -h [--help]            : show this message"

if __name__ == "__main__":
    try:
        short_args = "hmrudDHspfFWeqtibMlLS"
        long_args = ["help", "mounts", "rate", "utilization", "drives",
                     "drive-hours", "drive-hours-sep", "slots", "pnfs-backup",
                     "file-family-analysis", "files-rw", "files-rw-sep",
                     "quotas", "tapes-burn-rate", "migration-summary",
                     "bytes-per-day", "mover-summary", "latencies",
                     "library-mounts", "sfa-stats"]
        opts, args = getopt.getopt(sys.argv[1:], short_args, long_args)
    except getopt.GetoptError, msg:
        print msg
        print "Failed to process arguments"
        usage(sys.argv[0])
        sys.exit(2)

    if len(opts) == 0 :
        usage(sys.argv[0])
        sys.exit(1)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage(sys.argv[0])
            sys.exit(1)

    f = enstore_plotter_framework.EnstorePlotterFramework()

    for o, a in opts:

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
        # drive hours per day, stacked by storage group
        if o in ("-D","--drive-hours"):
            aModule = drive_hours_plotter_module.DriveHoursPlotterModule("drive-hours")
            f.add(aModule)
        # drive hours per day, separately for each storage group
        if o in ("-H","--drive-hours-sep"):
            aModule = drive_hours_sep_plotter_module.DriveHoursSepPlotterModule("drive-hours-sep")
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
            aModule = file_family_analysis_plotter_module.FileFamilyAnalysisPlotterModule("file_family_analysis")
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
        # bytes per day
        if o in ("-b","--bytes-per-day"):
            aModule = bytes_per_day_plotter_module.BytesPerDayPlotterModule("bytes-per-day")
            f.add(aModule)
        # mover summary
        if o in ("-M","--mover-summary"):
            aModule = mover_summary_plotter_module.MoverSummaryPlotterModule("mover-summary")
            f.add(aModule)
        # latencies
        if o in ("-l","--latencies"):
            aModule = mount_latency_plotter_module.MountLatencyPlotterModule("latencies")
            f.add(aModule)
        # library mounts
        if o in ("-L","--library-mounts"):
            aModule = mounts_per_robot_plotter_module.MountsPerRobotPlotterModule("library-mounts")
            f.add(aModule)
        # file reads and writes per mount, stacked by storage group
        if o in ("-F","--files-rw"):
            aModule = files_rw_plotter_module.FilesRWPlotterModule("files-rw")
            f.add(aModule)
        # file reads and writes per mount, separately for each storage group
        if o in ("-W","--files-rw-sep"):
            aModule = files_rw_sep_plotter_module.FilesRWSepPlotterModule("files-rw-sep")
            f.add(aModule)
        if o in ("-S","--sfa-stats"):
            if f.csc.get("dispatcher"):
                import sfa_plotter_module
                SFA_Stats_Module = sfa_plotter_module.SFAStatsPlotterModule("SFA_Statistics")
                f.add(SFA_Stats_Module)
                pack_data_file = f.csc.get("SFA_Stats",{}).get("packaging_rates_data")
                unpack_data_file = f.csc.get("SFA_Stats",{}).get("unpackaging_rates_data")
                if pack_data_file:
                    SFA_Aggregation_rates_Module = sfa_plotter_module.SFATarRatesPlotterModule("SFA_pack_rates",
                                                                                               date=None,
                                                                                               data_file=pack_data_file,
                                                                                               grep_pattern="Finished tar to",
                                                                                               tmp_file="/tmp/pack_rates")
                    f.add(SFA_Aggregation_rates_Module)
                if unpack_data_file:
                    SFA_Stage_rates_Module = sfa_plotter_module.SFATarRatesPlotterModule("SFA_unpack_rates",
                                                                                         date=None,
                                                                                         data_file=unpack_data_file,
                                                                                         grep_pattern="Finished tar from",
                                                                                         tmp_file="/tmp/unpack_rates")
                    f.add(SFA_Stage_rates_Module)
            else:
                print "Small Files Configuration is not defined"
                sys.exit(1)

    f.do_work()
