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

import configuration_client
import enstore_plotter_module
import enstore_functions2


class EnstorePlotterFramework:
    def __init__(self):
        self.module_list = []
        self.csc = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                             enstore_functions2.default_port()))

    def get_configuration_client(self):
        return self.csc

    def add(self, module):
        self.module_list.append(module)

    def book(self):
        for module in self.module_list:
            if module.isActive() == True:
                module.book(self)

    def fill(self):
        for module in self.module_list:
            if module.isActive() == True:
                module.fill(self)

    def plot(self):
        for module in self.module_list:
            if module.isActive() == True:
                module.plot()

    def do_work(self):
        self.book()
        self.fill()
        self.plot()
