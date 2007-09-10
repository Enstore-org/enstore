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

if __name__ == "__main__":
    f = enstore_plotter_framework.EnstorePlotterFramework()
    aModule   = ratekeeper_plotter_module.RateKeeperPlotterModule("ratekeeper")
    f.add(aModule)
    f.do_work()
