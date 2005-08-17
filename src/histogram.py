#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# Histogram class (equal binning)
# Author: Dmitry Litvintsev (litvinse@fnal.gov) 08/05
#
###############################################################################

import random
import math
import sys
import os
import time

class Histogram1D:

    def __init__(self, name, title, nbins=10, xlow=0, xhigh=1):
        self.name=name
        self.title=title
        self.nbins=int(nbins)
        self.low=xlow
        self.high=xhigh
        self.entries=0
        self.binarray = []
        self.underflow=0
        self.overflow=0
        self.mean=0
        self.mean_error=0
        self.rms=0
        self.rms_error=0
        self.sum=0
        self.sum2=0
        self.data_file_name="%s_data.pts"%(self.name,)
        self.bw=(self.high-self.low)/float(self.nbins)
        self.maximum=0
        self.minimum=0
        #
        # atributes
        #
        self.ylabel=""
        self.xlabel=""
        self.logy=False
        self.logx=False

        for i in range(self.nbins):
            self.binarray.append(0.)
    #
    # non trivial methods 
    #

    def find_bin(self,x):
        if ( x < self.low ):
            self.underflow=self.underflow+1
            return None
        elif ( x > self.high ):
            self.overflow=self.overflow+1
            return None
        return int (float(self.nbins)*(x-self.low)/(self.high-self.low));

    def fill(self,x,w=1.):
        bin = self.find_bin(x)
        if bin: 
            self.sum=self.sum+x
            self.sum2=self.sum2+x*x
            self.entries=self.entries+1
            count=self.binarray[bin]
            count=count+1.*w
            self.binarray[bin]=count
            self.mean=self.sum/float(self.entries)
            rms2=self.sum2-2.*self.sum*self.mean+float(self.entries)*self.mean*self.mean
            self.rms=math.sqrt(rms2/float(self.entries))
            self.mean_error=self.rms/math.sqrt(float(self.entries))
            self.rms_error=self.rms/math.sqrt(2.*float(self.entries))
            if ( count > self.maximum ) :
                self.maximum=count
            if ( count < self.minimum ) :
                self.minimum=count
    #
    # setters 
    #
                
    def set_title(self,txt):
        self.title=txt

    def set_name(self,txt):
        self.name=txt

    def set_data_file_name(self,txt):   # name of the file with data points
        self.data_file_name=txt

    #
    # Plotting features
    #

    def set_ylabel(self,txt):
        self.ylabel=txt

    def set_xlabel(self,txt):
        self.xlabel=txt

    def set_logy(self,yes=True):
        self.logy=yes

    def set_logx(self,yes=True):
        self.logx=yes

    #
    # getters
    #

    def n_entries(self):
        return self.entries

    def n_bins(self):
        return self.nbins

    def axis_low(self):
        return self.low
    
    def axis_high(self):
        return self.high

    def get_bin_content(self,bin):
        if ( bin >= self.nbins or bin < 0 ) :
            return None
        return self.binarray[bin]

    def get_bin_center(self,bin):
        if ( bin >= self.nbins or bin < 0 ) :
            return None
        return self.low+(bin+0.5)*(self.high-self.low)/float(self.nbins)

    def get_bin_width(self,bin):
        return self.bw

    def get_bin_low_edge(self,bin):
        if ( bin >= self.nbins or bin < 0 ) :
            return None
        return self.low+float(bin)*(self.high-self.low)/float(self.nbins)

    def get_bin_high_edge(self,bin):
        if ( bin >= self.nbins or bin < 0 ) :
            return None
        return self.low+(bin+1.)*(self.high-self.low)/float(self.nbins)

    #
    # Statistis
    #

    def get_mean(self):
        return self.mean

    def get_mean_error(self):
        return self.mean_error

    def get_rms(self):
        return self.rms

    def get_rms_error(self):
        return self.rms_error

    def get_maximum(self): # maximum bin content
        return self.maximum

    def get_minimum(self): # minimum bin content
        return self.minimum

    def get_logy(self):
        return self.logy

    def get_logx(self):
        return self.logx

    def get_title(self):
        return self.title

    def get_name(self):
        return self.name

    def get_ylabel(self):
        return self.ylabel

    def get_xlabel(self):
        return self.xlabel

    def get_data_file_name(self):
        return self.data_file_name;

    #
    # Write data to point file, format x dx y dy
    #

    def save_data(self,fname):
        data_file=open(fname,'w')
        for i in range(self.nbins):
            x = self.get_bin_center(i)
            y = self.get_bin_content(i)
            dy = math.sqrt(self.get_bin_content(i))
            dx = 0.5*self.bw
            data_file.write("%f %f %f %f\n"%(x,dx,y,dy))
        data_file.close()

    #
    # "plot" is the act of creating an image
    #

    def plot(self,dir="./"):
        full_file_name=dir+self.data_file_name
        self.save_data(full_file_name)
        gnu_file_name = "tmp_%s_gnuplot.cmd"%(self.name)
        gnu_cmd = open(gnu_file_name,'w')
        long_string="set output '"+self.name+".ps'\n"+ \
                     "set terminal postscript color solid\n"\
                     "set title '"+self.title+" %s'"%(time.strftime("%Y-%b-%d %H:%M:%S",time.localtime(time.time())))+"\n" \
                     "set style fill solid 2.000000 border\n" \
                     "set xrange [ : ]\n"+ \
                     "set size 1.5,1\n"+ \
                     "set ylabel '%s'\n"%(self.ylabel)+ \
                     "set xlabel '%s'\n"%(self.xlabel)+ \
                     "set grid\n"
        if ( self.get_logy() ) :
            long_string=long_string+"set logscale y\n"
        if ( self.get_logx() ) :
            long_string=long_string+"set logscale x\n"
        long_string=long_string+"set key right top Left samplen 20 title \""+\
                     "Mean : %.2e"%(self.mean)+"+-%.2e"%(self.mean_error)+\
                     "\\n RMS : %.2e"%(self.rms)+"+-%.2e"%(self.rms_error)+\
                     "\\nEntries : %d"%(self.entries)+\
                     "\\n Overflow : %d"%(self.overflow)+\
                     "\\n Underflow : %d"%(self.underflow)+"\"\n"+\
                     "plot '"+full_file_name+\
                     "' using 1:3 t '' with boxes\n "
        gnu_cmd.write(long_string)
        gnu_cmd.close()
        os.system("gnuplot %s"%(gnu_file_name))
        os.system("convert -rotate 90 -modulate 80 %s.ps %s.jpg"%(self.name,self.name))
        os.system("convert -rotate 90 -geometry 120x120 -modulate 80 %s.ps %s_stamp.jpg"%(self.name,self.name))
        os.system("rm -f %s"%full_file_name) # remove pts file
        os.system("rm -f %s"%gnu_file_name)  # remove gnu file
    
    def dump(self):
        print repr(self.__dict__)
        
if __name__ == "__main__":
    hist=Histogram1D("try","try",100,0,10)
    while ( hist.n_entries() < 10000 ) :
        x=random.gauss(5,0.5)
        hist.fill(x)
    hist.set_ylabel("Counts")
    hist.set_xlabel("x variable")
    hist.set_logy(True)
    hist.plot()
    os.system("display %s.jpg&"%(hist.get_name()))
    sys.exit(0)
    
    
