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

def usage(cmd):
    print "Usage: %s -s [--sleep=] "%(cmd,)
    print "\t --sleep : sampling interval in seconds"

    
if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hs:ms:", ["help","mounts="])
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

            aModule   = mounts_plot.MountsPlot("mounts")
            aModule.add_parameter("library","dlt");
            f.add(aModule)
            
            aModule   = mounts_plot.MountsPlot("mounts")
            aModule.add_parameter("library","CD-9940B");
            f.add(aModule)
            
            aModule   = mounts_plot.MountsPlot("mounts")
            aModule.add_parameter("library","9940");
            f.add(aModule)    
            
            aModule   = mounts_plot.MountsPlot("mounts")
            aModule.add_parameter("library","CD-LTO3");
            f.add(aModule)
        if o in ("-r","--rate"):
            aModule   = ratekeeper_plotter_module.RateKeeperPlotterModule("ratekeeper")
            f.add(aModule)
            aModule.setActive(False)

    f.do_work()
