#!/usr/bin/env python

import enstore_plotter_framework

class EnstorePlotterModule:
    def __init__(self,name,isActive=True):
        self.name=name
        self.is_active=isActive
        self.parameters = {}
    def isActive(self):
        return self.is_active
    def book(self,frame):
        print "Booking ",self.name
    def fill(self,frame):
        print "Filling ",self.name
    def plot(self):
        print "Plotting ",self.name
    def add_parameter(self,par_name,par_value):
        self.parameters[par_name]=par_value
    def get_parameter(self,name):
        return self.parameters.get(name)
    
