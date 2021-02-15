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
from __future__ import print_function
import getopt
import sys

# enstore imports
import enstore_plotter_framework
import summary_burn_rate_plotter_module
import summary_bpd_plotter_module


def usage(cmd):
    print("Usage: %s [-t | --tapes-burn-rate] \n"
          "          [-b | --total-bytes-per-day]\n"
          "          [web_dir]\n" % (cmd,))


if __name__ == "__main__":
    try:
        short_args = "htb"
        long_args = ["help", "--tapes-burn-rate"]
        opts, args = getopt.getopt(sys.argv[1:], short_args, long_args)
    except getopt.GetoptError:
        print("Failed to process arguments")
        usage(sys.argv[0])
        sys.exit(2)

    f = enstore_plotter_framework.EnstorePlotterFramework()

    for o, a in opts:
        if o in ("-h", "--help"):
            usage(sys.argv[0])
            sys.exit(1)

        if o in ("-t", "--tapes-burn-rate"):
            aModule = summary_burn_rate_plotter_module.SummaryBurnRatePlotterModule(
                "summary_burn_rate")
            if len(args) > 0:
                aModule.add_parameter("web_dir", args[0])
            f.add(aModule)

        if o in ("-b", "--total-bytes-per-day"):
            aModule = summary_bpd_plotter_module.SummaryBpdPlotterModule(
                "summary_bpd")
            if len(args) > 0:
                aModule.add_parameter("web_dir", args[0])
            f.add(aModule)

    f.do_work()
