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

# system imports
import random
import math
import sys
import os
import time
import errno
import string

class Attribute:
    def __init__(self, name, title):
        self.name=name
        self.title=title
        self.data_file_name="%s_data.pts"%(self.name,)
        self.time_axis=False # time axis assumes unix time stamp
        self.opt_stat=False
        self.profile=False
        self.marker_type="points"
        self.marker_text=""
        self.additional_text=""
        self.commands=[]
        self.line_width=1
        self.line_color=1
        self.data_file=None  #open(self.data_file_name,"w");
        #
        # atributes
        #
        self.xlabel=""
        self.ylabel=""
        self.zlabel=""
        self.logy=False
        self.logz=False
        self.logx=False
        self.time_axis_format="%y-%m-%d"
        self.opt_stat=False
        self.profile=False
        self.delete_data_file=True

    #
    # setters
    #

    def set_delete_data_file(self,yes=True):
        self.delete_data_file=yes

    def set_title(self,txt):
        self.title=txt

    def set_name(self,txt):
        self.name=txt

    def set_data_file_name(self,txt):   # name of the file with data points
        self.data_file_name=txt

    def set_ylabel(self,txt):
        self.ylabel=txt

    def set_xlabel(self,txt):
        self.xlabel=txt

    def set_zlabel(self,txt):
        self.zlabel=txt

    def set_logy(self,yes=True):
        self.logy=yes

    def set_logx(self,yes=True):
        self.logx=yes

    def set_time_axis(self,yes=True):
        self.time_axis=yes
        self.xlabel="(year-month-day)"

    def set_time_axis_format(self,txt):
        self.time_axis_format=txt

    def set_opt_stat(self,yes=True):
        self.opt_stat=yes

    def set_profile(self,yes=True):
        self.profile=yes

    def add_text(self,txt):
        self.additional_text=self.additional_text+txt

    def add_command(self,txt):
        self.commands.append(txt)

    def set_marker_type(self,txt):
        self.marker_type=txt

    def set_marker_text(self,txt):
        self.marker_text=txt

    def set_line_color(self,color=1):
        self.line_color=color

    def set_line_width(self,w=1):
        self.line_width=w

    #
    # getters
    #

    def is_delete_data_file(self):
        return self.delete_data_file

    def get_text(self):
        return self.additional_text

    def get_marker_type(self):
        return self.marker_type

    def get_marker_text(self):
        return self.marker_text

    def get_line_color(self):
        return self.line_color

    def get_line_width(self):
        return self.line_width


    #
    # Statistis
    #


    def get_logy(self):
        return self.logy

    def get_time_axis(self):
        return self.time_axis

    def get_time_axis_format(self):
        return self.time_axis_format

    def get_opt_stat(self):
        return self.opt_stat

    def get_profile(self):
        return self.profile

    def get_logx(self):
        return self.logx

    def get_title(self):
        return self.title

    def get_name(self):
        return self.name

    def get_xlabel(self):
        return self.xlabel

    def get_ylabel(self):
        return self.ylabel

    def get_zlabel(self):
        return self.zlabel

    def get_data_file_name(self):
        return self.data_file_name;

    def get_data_file(self):
        if self.data_file == None:
            #When does this file ever get closed?
            self.data_file = open(self.data_file_name,"w");
        return self.data_file;

class BasicHistogram(Attribute):

     def __init__(self,
                  name,
                  title):
         Attribute.__init__(self, name, title)

         self.entries=0

     def n_entries(self):
         return self.entries

     def get_entries(self):
         return self.entries

     def plot(self,command=""):

         print "Plot function is not implemented by BasicHistogram"

     def fill(self,command=""):
         print "Fill function is not implemented by BasicHistogram"

     def remove(self, filename):
         #delete the specified file.
         try:
             os.remove(filename)
         except (OSError), msg:
             if msg.args[0] == errno.ENOENT:
                 pass
             else:
                 sys.stderr.write(str(msg) + "\n")

class Ntuple(BasicHistogram):

    def __init__(self, name, title):
        BasicHistogram.__init__(self, name, title)
    #
    # "plot" is the act of creating an image
    #

    def plot(self, command, directory="./"):
        #Get some filenames for the various files that get created.
        ps_file_name = os.path.join(directory, self.name + ".ps")
        jpg_file_name = os.path.join(directory, self.name + ".jpg")
        stamp_jpg_file_name = os.path.join(directory, self.name + "_stamp.jpg")
        gnu_file_name = "/tmp/%s_gnuplot.cmd" % (self.name)

        #Create the file that contains the commands for gnuplot to run.
        gnu_cmd = open(gnu_file_name,'w')
        long_string="set output '" + ps_file_name + "'\n"+ \
                     "set terminal postscript color solid\n"\
                     "set title '"+self.title+" %s'"%(time.strftime("%Y-%b-%d %H:%M:%S",time.localtime(time.time())))+"\n" \
                     "set xrange [ : ]\n"+ \
                     "set size 1.5,1\n"+ \
                     "set grid\n"+ \
                     "set ylabel '%s'\n"%(self.ylabel)+ \
                     "set xlabel '%s'\n"%(self.xlabel)+ \
                     string.join(self.commands,"\n")+"\n"
        long_string=long_string+" set pm3d; set palette; \n";
        if (  self.time_axis ) :
            long_string=long_string+"set xlabel '%s'\n"%(self.xlabel)+ \
                         "set xdata time\n"+ \
                         "set timefmt \"%Y-%m-%d %H:%M:%S\"\n"+ \
                         "set format x \""+self.time_axis_format+"\"\n"
            if ( self.get_logy() ) :
                long_string=long_string+"set logscale y\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logx() ) :
                long_string=long_string+"set logscale x\n"
            long_string=long_string+self.get_text()
            long_string=long_string+"plot '"+self.data_file_name+"' using "+command+" "
        else :
            if ( self.get_logy() ) :
                long_string=long_string+"set logscale y\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logx() ) :
                long_string=long_string+"set logscale x\n"
            long_string=long_string+self.get_text()
            long_string=long_string+"plot '"+self.data_file_name+"' using "+command+" "
        long_string=long_string+" t '"+self.get_marker_text()+"' with "\
                    +self.get_marker_type()+" lw "+str(self.get_line_width())+" lt "+str(self.get_line_color())+" \n "
        gnu_cmd.write(long_string)
        gnu_cmd.close()

        #Make the plot and convert it to jpg.
        os.system("gnuplot %s"%(gnu_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s"
                  % (ps_file_name, jpg_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s"
                  % (ps_file_name, stamp_jpg_file_name))

        #Cleanup the temporary files.
        self.remove(gnu_file_name)  # remove gnu file


    def __del__(self):
        if self.is_delete_data_file():
            self.remove(self.data_file_name)

    def dump(self):
        print repr(self.__dict__)

class Plotter:

    def __init__(self, name,title):
        self.histogram_list = []
        self.name = name
        self.title = title
        self.commands=[]

    def add_command(self,txt):
        self.commands.append(txt)

    def remove(self, filename):
        #delete the specified file.
        try:
            os.remove(filename)
        except (OSError), msg:
            if msg.args[0] == errno.ENOENT:
                pass
            else:
                sys.stderr.write(str(msg) + "\n")

    def get_histogram_list(self):
        return  self.histogram_list

    def get_name(self):
        return  self.name

    def get_title(self):
        return  self.title

    def add(self,h):
        self.histogram_list.append(h)

    def reshuffle(self):
        n = len(self.histogram_list)
        i = 0
        j = n - 1 - i
        while ( i < j  ) :
            tmp = self.histogram_list[i]
            self.histogram_list[i]=self.histogram_list[j]
            self.histogram_list[j]=tmp
            i = i + 1
            j = j - 1

    def plot(self, directory="./"):
        #Get some filenames for the various files that get created.
        ps_file_name = os.path.join(directory, self.name + ".ps")
        jpg_file_name = os.path.join(directory, self.name + ".jpg")
        stamp_jpg_file_name = os.path.join(directory, self.name + "_stamp.jpg")
        gnu_file_name = "/tmp/%s_gnuplot.cmd" % (self.name)

        #Create the file that contains the commands for gnuplot to run.
        gnu_cmd = open(gnu_file_name,'w')
        long_string="set output '" + ps_file_name + "'\n"+ \
                     "set terminal postscript color solid\n"\
                     "set title '"+self.title+" %s'"%(time.strftime("%Y-%b-%d %H:%M:%S",time.localtime(time.time())))+"\n" \
                     "set xrange [ : ]\n"+ \
                     "set size 1.5,1\n"+ \
                     "set grid\n"+ \
                     "set ylabel '%s'\n"%(self.histogram_list[0].get_ylabel(),)+ \
                     "set xlabel '%s'\n"%(self.histogram_list[0].get_xlabel(),)+ \
                     string.join(self.commands,"\n")+"\n"
        if ( self.histogram_list[0].get_logy() ) :
            long_string=long_string+"set logscale y\n"
            long_string=long_string+"set yrange [ 0.99  : ]\n"
        if ( self.histogram_list[0].get_logx() ) :
            long_string=long_string+"set logscale x\n"
        if (  self.histogram_list[0].get_time_axis()) :
            long_string=long_string+"set xlabel 'Date (year-month-day)'\n"+ \
                         "set xdata time\n"+ \
                         "set timefmt \"%Y-%m-%d %H:%M:%S\"\n"+ \
                         "set format x \""+self.histogram_list[0].time_axis_format+"\"\n"

#            if ( isinstance(hist,histogram.Histogram1D)) :

        for hist in self.histogram_list:
            pts_file_name=os.path.join(directory, hist.data_file_name)
            hist.save_data(pts_file_name)
            long_string=long_string+hist.get_text()

        long_string=long_string+"plot "
        comma = False

        for hist in self.histogram_list:
            pts_file_name = os.path.join(directory, hist.data_file_name)
            if ( comma ) :
                long_string=long_string+" , "
            else:
                comma = True
            if (hist.get_time_axis()):
                long_string=long_string+"'"+pts_file_name+"' using 1:4 "
            else :
                long_string=long_string+"'"+pts_file_name+"' using 1:3 "
            long_string=long_string+" t '"+hist.get_marker_text()+"' with "\
                     +hist.get_marker_type()+" lw "+str(hist.get_line_width())+" "
            if hist.get_line_color() != 0 :
                 long_string += " lt "+str(hist.get_line_color())+"  "
        gnu_cmd.write(long_string)
        gnu_cmd.close()

        #Make the plot and convert it to jpg.
        os.system("gnuplot %s"%(gnu_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s"
                  % (ps_file_name, jpg_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s"
                  % (ps_file_name, stamp_jpg_file_name))

        #Cleanup the temporary files.
        for hist in self.histogram_list:
            pts_file_name = os.path.join(directory, hist.data_file_name)
            if hist.is_delete_data_file():
                self.remove(pts_file_name) # remove pts file
        self.remove(gnu_file_name)  # remove gnu file

class Histogram1D(BasicHistogram):

    def __init__(self, name, title, nbins, xlow, xhigh):
        BasicHistogram.__init__(self, name, title)
        self.nbins=int(nbins)
        self.low=xlow         # value of the lowest bin in x
        self.high=xhigh       # value of the highest bin in x
        self.entries=0
        self.binarray = []
        self.sumarray = []
        self.underflow=0
        self.overflow=0
        self.mean=0.
        #
        # reduced_mean and reduced_rms2 are used as intermediate
        # quantities that are recalculated into real mean and rms2
        # after each call of fill function. Reduced_mean and reduced_rms2
        # are calculated by subtracting minimum displayed value, self.low,
        # from each element x_i of input data sample/set, {x_i-self.low}, to avoid issues
        # with large numbers that could lead to failure of standard
        # formula variance = <x^2>-<x>^2 (resulting in negative and incorrect
        # values of variance)
        #
        self.reduced_mean=0.
        self.reduced_rms2=0.
        self.mean_error=0.
        self.rms2=0.
        self.variance=0.
        self.variance_error=0.
        self.bw=(self.high-self.low)/float(self.nbins)
        self.maximum=0.
        self.minimum=0.
        self.opt_stat=False
        self.profile=False
        self.marker_type="boxes"
        self.marker_text=""
        self.additional_text=""
        self.line_width=1
        self.line_color=1
        #
        # atributes
        #
        self.ylabel=""
        self.xlabel=""
        self.logy=False
        self.logx=False
        self.time_axis_format="%y-%m-%d"
        for unused in range(self.nbins):
            self.binarray.append(0.)
            self.sumarray.append(0.)

    def __gt__(self,other):
        return sum(self.binarray)>sum(other.binarray)

    def copy(self) :
        other = Histogram1D(self.name,
                            self.title,
                            self.nbins,
                            self.low,
                            self.high)

        other.entries=self.entries
        other.binarray = []
        other.sumarray = []
        other.underflow=self.underflow
        other.overflow=self.overflow
        other.mean=self.mean
        other.mean_error=self.mean_error
        other.rms2=self.rms2
        other.variance=self.variance
        other.variance_error=self.variance_error
        other.data_file_name=self.data_file_name
        other.bw=self.bw
        other.maximum=self.maximum
        other.minimum=self.minimum
        other.time_axis=self.time_axis
        other.opt_stat=self.opt_stat
        other.profile=self.profile
        other.marker_type=self.marker_type
        other.marker_text=self.marker_text
        other.additional_text=self.additional_text
        other.line_width=self.line_width
        other.line_color=self.line_color
        for i in range(self.nbins):
            other.binarray.append(self.binarray[i])
            other.sumarray.append(self.sumarray[i])

        #
        # atributes
        #
        other.ylabel=self.ylabel
        other.xlabel=self.xlabel
        other.logy=self.logy
        other.logx=self.logx
        return other

    def __eq__(self, other):
        return (other is self) or (
            self.nbins == other.nbins and
            self.low == other.low and
            self.high == other.high)

    def __add__(self, other):
        if ( self == other ) :
            hist = Histogram1D("sum","sum",self.n_bins(),self.get_bin_low_edge(0),
                               self.get_bin_high_edge(self.n_bins()-1))
            hist.entries=self.entries+other.entries
            if ( hist.entries > 0 ) :
                hist.mean=(float(self.entries)*self.mean+float(other.entries)*other.mean)/float(hist.entries)
                hist.rms2=(self.rms2*float(self.entries)+other.rms2*float(other.entries))/float(hist.entries)
                hist.variance =hist.rms2-hist.mean*hist.mean
                hist.variance =math.sqrt(hist.variance)
                hist.mean_error=hist.variance/math.sqrt(float(hist.entries))
                hist.variance_error=hist.variance/math.sqrt(2.*float(hist.entries))
            else:
                hist.mean = 0.
                hist.rms2 = 0.
                hist.mean_error=0.
                hist.variance=0.
                hist.variance_error=0.
            for i in range(hist.n_bins()):
                hist.binarray[i]=self.binarray[i]+other.binarray[i]
                hist.sumarray[i]=self.sumarray[i]+other.sumarray[i]
                if (  hist.binarray[i] > hist.maximum ) :
                    hist.maximum = hist.binarray[i]
                    if (  hist.binarray[i] < hist.minimum ) :
                        hist.minimum  = hist.binarray[i]
            return hist
        else:
            low=0
            high=0
            if ( math.fabs( self.bw - other.bw ) < 1.e-16 ) :
                low = self.low
                if ( self.low < other.low ) :
                    low = self.low
                else:
                    low = other.low
                high = self.high
                if ( self.high > other.high ) :
                    high = self.high
                else:
                    high = other.high
            nbins = int((high-low)/self.bw)
            hist = Histogram1D("sum","sum",nbins,low,high)
            hist.entries=self.entries+other.entries
            if ( hist.entries > 0 ) :
                hist.mean=(float(self.entries)*self.mean+float(other.entries)*other.mean)/float(hist.entries)
                hist.rms2=(self.rms2*float(self.entries)+other.rms2*float(other.entries))/float(hist.entries)
                hist.variance =hist.rms2-hist.mean*hist.mean
                hist.variance =math.sqrt(hist.variance)
                hist.mean_error=hist.variance/math.sqrt(float(hist.entries))
                hist.variance_error=hist.variance/math.sqrt(2.*float(hist.entries))
            else:
                hist.mean = 0.
                hist.rms2 = 0.
                hist.mean_error=0.
                hist.variance=0.
                hist.variance_error=0.

            for i in range(hist.n_bins()):
                x    = hist.get_bin_center(i)
                bin1 = self.find_bin(x)
                bin2 = other.find_bin(x)
                c1 = 0
                c2 = 0
                s1 = 0
                s2 = 0
                if ( bin1 ) :
                    c1 = self.get_bin_content(bin1)
                    s1 = self.get_sum_content(bin1)
                if ( bin2 ) :
                    c2 = other.get_bin_content(bin2)
                    s2 = other.get_sum_content(bin2)
                hist.binarray[i] = c1+c2
                hist.sumarray[i] = s1+s2
                if (  hist.binarray[i] > hist.maximum ) :
                    hist.maximum = hist.binarray[i]
                    if (  hist.binarray[i] < hist.minimum ) :
                        hist.minimum  = hist.binarray[i]
            return hist


    def __sub__(self, other):
        if ( self == other ) :
            hist = Histogram1D("diff","diff",self.n_bins(),self.get_bin_low_edge(0),
                               self.get_bin_high_edge(self.n_bins()-1))
            hist.entries=self.entries+other.entries
            if ( hist.entries > 0 ) :
                hist.mean=(float(self.entries)*self.mean+float(other.entries)*other.mean)/float(hist.entries)
                hist.rms2=(self.rms2*float(self.entries)+other.rms2*float(other.entries))/float(hist.entries)
                hist.variance =hist.rms2-hist.mean*hist.mean
                hist.variance =math.sqrt(hist.variance)
                hist.mean_error=hist.variance/math.sqrt(float(hist.entries))
                hist.variance_error=hist.variance/math.sqrt(2.*float(hist.entries))
            else:
                hist.mean = 0.
                hist.rms2 = 0.
                hist.mean_error=0.
                hist.variance=0.
                hist.variance_error=0.

            for i in range(hist.n_bins()):
                hist.binarray[i]=self.binarray[i]-other.binarray[i]
                hist.sumarray[i]=self.sumarray[i]-other.sumarray[i]
                if (  hist.binarray[i] > hist.maximum ) :
                    hist.maximum = hist.binarray[i]
                    if (  hist.binarray[i] < hist.minimum ) :
                        hist.minimum  = hist.binarray[i]
            return hist
        else:
            if ( math.fabs( self.bw - other.bw ) < 1.e-16 ) :
                low = self.low
                if ( self.low < other.low ) :
                    low = self.low
                else:
                    low = other.low
                high = self.high
                if ( self.high > other.high ) :
                    high = self.high
                else:
                    high = other.high
            nbins = int((high-low)/self.bw)
            hist = Histogram1D("sum","sum",nbins,low,high)
            hist.entries=self.entries+other.entries
            if ( hist.entries > 0 ) :
                hist.mean=(float(self.entries)*self.mean+float(other.entries)*other.mean)/float(hist.entries)
                hist.rms2=(self.rms2*float(self.entries)+other.rms2*float(other.entries))/float(hist.entries)
                hist.variance =hist.rms2-hist.mean*hist.mean
                hist.variance =math.sqrt(hist.variance)
                hist.mean_error=hist.variance/math.sqrt(float(hist.entries))
                hist.variance_error=hist.variance/math.sqrt(2.*float(hist.entries))
            else:
                hist.mean = 0.
                hist.rms2 = 0.
                hist.mean_error=0.
                hist.variance=0.
                hist.variance_error=0.
            for i in range(hist.n_bins()):
                x    = hist.get_bin_center(i)
                bin1 = self.find_bin(x)
                bin2 = other.find_bin(x)
                c1 = 0
                c2 = 0
                s1 = 0
                s2 = 0
                if ( bin1 ) :
                    c1 = self.get_bin_content(bin1)
                    s1 = self.get_sum_content(bin1)
                if ( bin2 ) :
                    c2 = other.get_bin_content(bin2)
                    s2 = other.get_sum_content(bin2)
                hist.binarray[i] = c1-c2
                hist.sumarray[i] = s1-s2
                if (  hist.binarray[i] > hist.maximum ) :
                    hist.maximum = hist.binarray[i]
                    if (  hist.binarray[i] < hist.minimum ) :
                        hist.minimum  = hist.binarray[i]
            return hist

    #
    # non trivial methods
    #

    def reset(self) :
        self.entries=0
        self.underflow=0
        self.overflow=0
        self.mean=0.
        self.mean_error=0.
        self.rms2=0.
        self.variance=0.
        self.variance_error=0.
        self.maximum=0
        self.minimum=0
        for unused in range(self.nbins):
            self.binarray.append(0.)
            self.sumarray.append(0.)

    def integral(self,name="",title="",only_positive=True) :
        h=self.copy()
        h.reset()
        if ( name == "" ) :
            h.set_name(self.name+"_integral")
        else:
            h.set_name(name)
        if ( title == "" ) :
            h.set_title(self.title+"intergal")
        else:
            h.set_title(title)
        h.set_data_file_name("%s_data.pts"%(h.name,))
        r_sum = self.get_bin_content(0)
        stop = 0;
        for i in range(self.nbins):
            if (self.get_time_axis()) :
                if ( stop == 1 ) : continue
                next = i+1
                if ( next >= self.nbins) :
                    next=i
                if (0.5*(self.get_bin_center(next)+self.get_bin_center(i))>time.time()):
                    stop=1
            y      = self.get_bin_content(i)
            r_sum  = r_sum + y
            h.binarray[i]=r_sum
        return h

    def derivative(self,name="",title="",only_positive=True) :
        h=self.copy()
        h.reset()
        if ( name == "" ) :
            h.set_name(self.name+"_der")
        else:
            h.set_name(name)
        if ( title == "" ) :
            h.set_title(self.title+"derivative")
        else:
            h.set_title(title)
        h.set_data_file_name("%s_data.pts"%(h.name,))
        previous_bin=self.get_bin_content(0)
        for i in range(self.nbins):
            y = self.get_bin_content(i)
            if ( self.profile ) :
                if ( self.sumarray[i] > 0 ) :
                    y = y  / self.sumarray[i]
            dy_dx = (  y - previous_bin  )
            if ( only_positive and dy_dx<0 ):
                dy_dx=0.
            if ( previous_bin == 0 ):
                dy_dx=0.
            h.binarray[i]=dy_dx
            previous_bin=y

        return h


    def find_bin(self,x):
        if ( x < self.low ):
            self.underflow=self.underflow+1
            return None
        elif ( x > self.high ):
            self.overflow=self.overflow+1
            return None
        bin = int (float(self.nbins)*(x-self.low)/(self.high-self.low));
        if ( bin == self.nbins ) :
            bin = bin-1
        return bin

    def fill(self,x,w=1.):
        bin = self.find_bin(x)
        x = x - self.low
        if bin != None :
            if ( self.profile ) :
                summary=self.sumarray[bin]
                summary=summary+1
                self.sumarray[bin]=summary
            count=self.binarray[bin]
            count=count+1.*w
            self.binarray[bin]=count
            self.reduced_mean=(self.reduced_mean*float(self.entries)+x)/(self.entries+1.)
            self.reduced_rms2=(self.reduced_rms2*float(self.entries)+x*x)/(self.entries+1.)
            self.variance=self.reduced_rms2-self.reduced_mean*self.reduced_mean
            self.mean=self.reduced_mean+self.low
            self.rms2=self.mean*self.mean+self.variance
            try:
                self.variance=math.sqrt(self.variance)
            except ValueError, msg:
                print msg, x, self.entries, self.variance, self.rms2, self.mean
                self.variance=math.sqrt(math.fabs(self.variance))
                pass
            self.entries=self.entries+1
            self.mean_error=self.variance/math.sqrt(float(self.entries))
            self.variance_error=self.variance/math.sqrt(2.*float(self.entries))
            if ( count > self.maximum ) :
                self.maximum=count
            if ( count < self.minimum ) :
                self.minimum=count
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

    def get_sum_content(self,bin):
        if ( bin >= self.nbins or bin < 0 ) :
            return 0
        return self.sumarray[bin]


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

    def get_text(self):
        return self.additional_text

    def get_marker_type(self):
        return self.marker_type

    def get_marker_text(self):
        return self.marker_text

    def get_line_color(self):
        return self.line_color

    def get_line_width(self):
        return self.line_width

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


    #
    # Write data to point file, format x dx y dy
    #


    def save_data(self,fname):
        data_file=open(fname,'w')
        previous_bin=self.get_bin_content(0)
        if ( self.profile ) :
            if ( self.sumarray[0] > 0 ) :
                previous_bin=self.get_bin_content(0)/self.sumarray[0]
        for i in range(self.nbins):
            x = self.get_bin_center(i)
            y = self.get_bin_content(i)
            dy = math.sqrt(self.get_bin_content(i))
            dx = 0.5*self.bw
            if ( self.profile ) :
                if ( self.sumarray[i] > 0 ) :
                    y = y  / self.sumarray[i]
                    dy = dy / self.sumarray[i]
            dy_dx = (  y - previous_bin  )
            if ( dy_dx < 0 ) :
                dy_dx = 0
            if ( self.time_axis ) :
                data_file.write("%s %f %f %f %f \n"%(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(x)),dx,y,dy,dy_dx))
            else :
                data_file.write("%f %f %f %f %f\n"%(x,dx,y,dy,dy_dx))
            previous_bin=y
        data_file.close()

    #
    # "plot" is the act of creating an image
    #

#    def add_opt_stat(self) :
#        return "set key right top Left samplen 20 title \""+\
#               "Mean : %.2e"%(self.mean)+"+-%.2e"%(self.mean_error)+\
#               "\\n RMS : %.2e"%(self.rms)+"+-%.2e"%(self.rms_error)+\
#               "\\nEntries : %d"%(self.entries)+\
#               "\\n Overflow : %d"%(self.overflow)+\
#               "\\n Underflow : %d"%(self.underflow)+"\" box\n"

    def add_opt_stat(self) :
        return "set key right top Left samplen 20 title \""+\
               "Mean : %.2e"%(self.mean)+"+-%.2e"%(self.mean_error)+\
               "\\n std : %.2e"%(self.variance)+"+-%.2e"%(self.variance_error)+\
               "\\n Entries : %d"%(self.entries)+\
               "\\n Overflow : %d"%(self.overflow)+\
               "\\n Underflow : %d"%(self.underflow)+"\" box\n"

    def plot(self, directory="./"):
        #Get some filenames for the various files that get created.
        pts_file_name = os.path.join(directory, self.data_file_name)
        ps_file_name = os.path.join(directory, self.name + ".ps")
        jpg_file_name = os.path.join(directory, self.name + ".jpg")
        stamp_jpg_file_name = os.path.join(directory, self.name + "_stamp.jpg")
        gnu_file_name = "/tmp/%s_gnuplot.cmd"%(self.name)

        #Polulate the data file that gnuplot will plot.
        self.save_data(pts_file_name)

        #Create the file that contains the commands for gnuplot to run.
        gnu_cmd = open(gnu_file_name,'w')
        long_string="set output '" + ps_file_name + "'\n"+ \
                     "set terminal postscript color solid\n"\
                     "set title '"+self.title+" %s'"%(time.strftime("%Y-%b-%d %H:%M:%S",time.localtime(time.time())))+"\n" \
                     "set size 1.5,1\n"+ \
                     "set grid\n"+ \
                     "set ylabel '%s'\n"%(self.ylabel)+ \
                     "set xlabel '%s'\n"%(self.xlabel)+ \
                     string.join(self.commands,"\n")+"\n"
        if ( self.get_opt_stat() ) :
            long_string=long_string+self.add_opt_stat()
        if (  self.time_axis ) :
            long_string=long_string+"set xlabel 'Date (year-month-day)'\n"+ \
                         "set xdata time\n"+ \
                         "set timefmt \"%Y-%m-%d %H:%M:%S\"\n"+ \
                         "set xrange [ \""+\
                         time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(self.get_bin_low_edge(0))) + \
                         "\":\""+ \
                         time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(self.get_bin_high_edge(self.n_bins()-1))) + \
                         "\"]\n"+ \
                         "set format x \""+self.time_axis_format+"\"\n"
            if ( self.get_logy() ) :
                long_string=long_string+"set logscale y\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logx() ) :
                long_string=long_string+"set logscale x\n"
            long_string=long_string+self.get_text()
            long_string=long_string+"plot '"+pts_file_name+"' using 1:4 "
        else :
            if ( self.get_logy() ) :
                long_string=long_string+"set logscale y\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logx() ) :
                long_string=long_string+"set logscale x\n"
            long_string=long_string+self.get_text() +\
                         "set xrange ["+\
                         str(self.get_bin_low_edge(0)) + \
                         ":"+ \
                         str(self.get_bin_high_edge(self.n_bins()-1)) + \
                         "]\n"
            long_string=long_string+"plot '"+pts_file_name+"' using 1:3 "
        long_string=long_string+" t '"+self.get_marker_text()+"' with "\
                    +self.get_marker_type()+" lw "+str(self.get_line_width())+" lt "+str(self.get_line_color())+" \n "
        gnu_cmd.write(long_string)
        gnu_cmd.close()

        #Make the plot and convert it to jpg.
        os.system("gnuplot %s" % (gnu_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s"
                  % (ps_file_name, jpg_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s"
                  % (ps_file_name, stamp_jpg_file_name))

        #Cleanup the temporary files.
        if self.is_delete_data_file():
            self.remove(pts_file_name)
        self.remove(gnu_file_name)

    def plot2(self, h, reflect=False, directory="./"):
        #Get some filenames for the various files that get created.
        pts_file_name = os.path.join(directory, self.data_file_name)
        pts_file_name1 = os.path.join(directory, h.get_data_file_name())
        ps_file_name = os.path.join(directory, self.name + ".ps")
        jpg_file_name = os.path.join(directory, self.name + ".jpg")
        stamp_jpg_file_name = os.path.join(directory, self.name + "_stamp.jpg")
        gnu_file_name = "tmp_%s_gnuplot.cmd"%(self.name)

        #Polulate the data file that gnuplot will plot.
        self.save_data(pts_file_name)
        h.save_data(pts_file_name1)

        #Create the file that contains the commands for gnuplot to run.
        gnu_cmd = open(gnu_file_name,'w')
        long_string="set output '" + ps_file_name + "'\n"+ \
                     "set terminal postscript color solid\n"\
                     "set title '"+self.title+" %s'"%(time.strftime("%Y-%b-%d %H:%M:%S",time.localtime(time.time())))+"\n" \
                     "set xrange [ : ]\n"+ \
                     "set size 1.5,1\n"+ \
                     "set grid\n"+ \
                     "set ylabel '%s'\n"%(self.ylabel)+ \
                     "set xlabel '%s'\n"%(self.xlabel)+ \
                     string.join(self.commands,"\n")+"\n"
        if ( self.get_opt_stat() ) :
            long_string=long_string+self.add_opt_stat()
        if (  self.time_axis ) :
            long_string=long_string+"set xlabel 'Date (year-month-day)'\n"+ \
                         "set xdata time\n"+ \
                         "set timefmt \"%Y-%m-%d %H:%M:%S\"\n"+ \
                         "set format x \""+self.time_axis_format+"\"\n"
            if ( self.get_logy() ) :
                long_string=long_string+"set logscale y\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logx() ) :
                long_string=long_string+"set logscale x\n"
            long_string=long_string+self.get_text()
            long_string=long_string+"plot '"+pts_file_name+"' using 1:4 "
        else :
            if ( self.get_logy() ) :
                long_string=long_string+"set logscale y\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logx() ) :
                long_string=long_string+"set logscale x\n"
            long_string=long_string+self.get_text()
            long_string=long_string+"plot '"+pts_file_name+"' using 1:3 "
        long_string=long_string+" t '"+self.get_marker_text()+"' with "\
                     +self.get_marker_type()+" lw "+str(self.get_line_width())+"  lt "+str(self.get_line_color())+"  "
        if (  self.time_axis ) :
            if (reflect) :
                long_string=long_string+",  '"+pts_file_name1+"' using 1:(-$4) "
            else :
                long_string=long_string+",  '"+pts_file_name1+"' using 1:4 "

        else:
            if (reflect) :
                long_string=long_string+",  '"+pts_file_name1+"' using 1:(-$3)"
            else :
                long_string=long_string+",  '"+pts_file_name1+"' using 1:3 "

        long_string=long_string+" t '"+h.get_marker_text()+"' with "\
                    +h.get_marker_type()+" lw "+str(h.get_line_width())+" lt "+str(h.get_line_color())+" \n "
        gnu_cmd.write(long_string)
        gnu_cmd.close()

        #Make the plot and convert it to jpg.
        os.system("gnuplot %s" % (gnu_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s"
                  % (ps_file_name, jpg_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s"
                  % (ps_file_name, stamp_jpg_file_name))

        #Cleanup the temporary files.
        if self.is_delete_data_file():
            self.remove(pts_file_name) # remove pts file
        self.remove(pts_file_name1) # remove pts file
        self.remove(pts_file_name) # remove pts file
        self.remove(gnu_file_name)  # remove gnu file

    def plot_derivative(self, directory="./"):
        #Get some filenames for the various files that get created.
        pts_file_name = os.path.join(dir, self.data_file_name)
        ps_file_name = os.path.join(directory, self.name + ".ps")
        jpg_file_name = os.path.join(directory, self.name + ".jpg")
        stamp_jpg_file_name = os.path.join(directory, self.name + "_stamp.jpg")
        gnu_file_name = "/tmp/%s_gnuplot.cmd"%(self.name)

        #Polulate the data file that gnuplot will plot.
        self.save_data(pts_file_name)

        #Create the file that contains the commands for gnuplot to run.
        gnu_cmd = open(gnu_file_name,'w')
        long_string="set output '" + ps_file_name + "'\n"+ \
                     "set terminal postscript color solid\n"\
                     "set title '"+self.title+" %s'"%(time.strftime("%Y-%b-%d %H:%M:%S",time.localtime(time.time())))+"\n" \
                     "set xrange [ : ]\n"+ \
                     "set size 1.5,1\n"+ \
                     "set grid\n"+ \
                     "set ylabel '%s'\n"%(self.ylabel)+ \
                     "set xlabel '%s'\n"%(self.xlabel)+ \
                     string.join(self.commands,"\n")+"\n"
        if ( self.get_opt_stat() ) :
            long_string=long_string+"set key right top Left samplen 20 title \""+\
                         "Mean : %.2e"%(self.mean)+"+-%.2e"%(self.mean_error)+\
                         "\\n RMS : %.2e"%(self.rms)+"+-%.2e"%(self.rms_error)+\
                         "\\nEntries : %d"%(self.entries)+\
                         "\\n Overflow : %d"%(self.overflow)+\
                         "\\n Underflow : %d"%(self.underflow)+"\"\n"
        if (  self.time_axis ) :
            long_string=long_string+"set xlabel 'Date (year-month-day)'\n"+ \
                         "set xdata time\n"+ \
                         "set timefmt \"%Y-%m-%d %H:%M:%S\"\n"+ \
                         "set format x \""+self.time_axis_format+"\"\n"
            if ( self.get_logy() ) :
                long_string=long_string+"set logscale y\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logx() ) :
                long_string=long_string+"set logscale x\n"
            long_string=long_string+self.get_text()
            long_string=long_string+"plot '"+pts_file_name+"' using 1:6 "
        else :
            if ( self.get_logy() ) :
                long_string=long_string+"set logscale y\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logx() ) :
                long_string=long_string+"set logscale x\n"
            long_string=long_string+self.get_text()
            long_string=long_string+"plot '"+pts_file_name+"' using 1:5 "
        long_string=long_string+" t '"+self.get_marker_text()+"' with "\
                    +self.get_marker_type()+" lw "+str(self.get_line_width())+" lt "+str(self.get_line_color())+" \n "
        gnu_cmd.write(long_string)
        gnu_cmd.close()

        #Make the plot and convert it to jpg.
        os.system("gnuplot %s"%(gnu_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s"
                  % (ps_file_name, jpg_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s"
                  % (ps_file_name, stamp_jpg_file_name))

        #Cleanup the temporary files.
        self.remove(pts_file_name) # remove pts file
        self.remove(gnu_file_name)  # remove gnu file

    def dump(self):
        print repr(self.__dict__)

class Histogram2D(Histogram1D):

    def __init__(self, name, title, nbinsX, xlow, xhigh, nbinsY, ylow, yhigh):
        Histogram1D.__init__(self,name,title,int(nbinsX*nbinsY),xlow,xhigh)
        self.ylow=ylow
        self.yhigh=yhigh
        self.nbins_x=nbinsX
        self.nbins_y=nbinsY
        self.logz=False
        self.zlabel=""
        self.marker_type="points"

    def set_zlabel(self,txt):
        self.zlabel=txt

    def set_logz(self,yes=True):
        self.logz=yes

    def get_logz(self):
        return self.logz;

    def get_zlabel(self):
        return self.zlabel;

    def copy(self) :
        other = Histogram2D(self.name,
                            self.title,
                            self.nbins_x,
                            self.low,
                            self.high,
                            self.nbins_y,
                            self.ylow,
                            self.yhigh)
        other.ylow=self.ylow
        other.yhigh=self.yhigh
        other.nbins_y=self.nbins_y
        other.nbins_x=self.nbins_x


        other.entries=self.entries
        other.binarray = []
        other.sumarray = []
        other.underflow=self.underflow
        other.overflow=self.overflow
        other.mean=self.mean
        other.mean_error=self.mean_error
        other.rms2=self.rms2
        other.variance_error=self.variance_error
        other.variance=self.variance
        other.data_file_name=self.data_file_name
        other.bw=self.bw
        other.maximum=self.maximum
        other.minimum=self.minimum
        other.time_axis=self.time_axis
        other.opt_stat=self.opt_stat
        other.profile=self.profile
        other.marker_type=self.marker_type
        other.marker_text=self.marker_text
        other.additional_text=self.additional_text
        other.line_width=self.line_width
        other.line_color=self.line_color
        for i in range(self.nbins):
            other.binarray.append(self.binarray[i])
            other.sumarray.append(self.sumarray[i])

        #
        # atributes
        #
        other.ylabel=self.ylabel
        other.xlabel=self.xlabel
        other.logy=self.logy
        other.logz=self.logz
        other.logx=self.logx
        return other

    def find_bin_x(self,x):
        if ( float(x) < self.low ):
            self.underflow=self.underflow+1
            return None
        elif ( float(x) > self.high ):
            self.overflow=self.overflow+1
            return None
        bin = int (float(self.nbins_x)*(float(x)-self.low)/(self.high-self.low));
        if ( bin == self.nbins_x ) :
            bin = bin-1
        return bin


    def find_bin_y(self,y):
        if ( float(y) < self.ylow ):
            self.underflow=self.underflow+1
            return None
        elif ( float(y) > self.yhigh ):
            self.overflow=self.overflow+1
            return None
        bin = int (float(self.nbins_y)*(float(y)-self.ylow)/(self.yhigh-self.ylow));
        if ( bin == self.nbins_y ) :
            bin = bin-1
        return bin

    def fill(self,x,y,w=1.):
        binx = self.find_bin_x(x)
        biny = self.find_bin_y(y)
        if (binx != None and biny != None) :
            bin = self.nbins_x*biny+binx
            self.entries=self.entries+1
            count=self.binarray[bin]
            count=count+1.*w
            self.binarray[bin]=count
            if ( count > self.maximum ) :
                self.maximum=count
            if ( count < self.minimum ) :
                self.minimum=count

    def get_bin_center(self,bin):
        ny = int(bin/self.nbins_x)
        nx = bin%self.nbins_x
        return self.low+(nx+0.5)*(self.high-self.low)/float(self.nbins_x), self.ylow+(ny+0.5)*(self.yhigh-self.ylow)/float(self.nbins_y),

    def save_data(self,fname):
        data_file=open(fname,'w')
        for i in range(self.nbins):
            x,y = self.get_bin_center(i)
            z = float(self.get_bin_content(i));
            dz = math.sqrt(self.get_bin_content(i))
#            if ( self.entries > 0 ) :
#                z =  z / float(self.entries) * 100.
#                dz = dz / float(self.entries) * 100.
            if ( self.time_axis ) :
                data_file.write("%s %f %f %f\n"%(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(x)),y,z,dz,))
            else :
                data_file.write("%f %f %f %f\n"%(x,y,z,dz,))
        data_file.close()

    #
    # "plot" is the act of creating an image
    #

    def plot(self, directory="./"):
        #Get some filenames for the various files that get created.
        pts_file_name = os.path.join(directory, self.data_file_name)
        ps_file_name = os.path.join(directory, self.name + ".ps")
        jpg_file_name = os.path.join(directory, self.name + ".jpg")
        stamp_jpg_file_name = os.path.join(directory, self.name + "_stamp.jpg")
        gnu_file_name = "/tmp/%s_gnuplot.cmd" % (self.name)

        #Polulate the data file that gnuplot will plot.
        self.save_data(pts_file_name)

        #Create the file that contains the commands for gnuplot to run.
        gnu_cmd = open(gnu_file_name,'w')
        long_string="set output '" + ps_file_name + "'\n"+ \
                     "set terminal postscript color solid\n"\
                     "set title '"+self.title+" %s'"%(time.strftime("%Y-%b-%d %H:%M:%S",time.localtime(time.time())))+"\n" \
                     "set view map \n"+\
                     "set palette defined ( 0 0.05 0.05 0.2, 0.1 0 0 1, 0.25 0.7 0.85 0.9, 0.4 0 0.75 0, 0.5 1 1 0, 0.7 1 0 0, 0.9 0.6 0.6 0.6, 1 0.95 0.95 0.95 )\n"+\
                     "set xrange [ : ]\n"+ \
                     "set zrange [ 0.001 : ]\n"+ \
                     "set size 1.5,1\n"+ \
                     "set grid\n"+ \
                     "set pm3d at b \n"+\
                     "set ylabel '%s'\n"%(self.ylabel)+ \
                     "set zlabel '%s'\n"%(self.zlabel)+ \
                     "set xlabel '%s'\n"%(self.xlabel)+ \
                     string.join(self.commands,"\n")+"\n"
        if (  self.time_axis ) :
            long_string=long_string+"set xlabel 'Date (year-month-day)'\n"+ \
                         "set xdata time\n"+ \
                         "set timefmt \"%Y-%m-%d %H:%M:%S\"\n"+ \
                         "set format x \"%y-%m-%d\"\n"
            if ( self.get_logy() ) :
                long_string=long_string+"set logscale y\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logz() ) :
                long_string=long_string+"set logscale z\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logx() ) :
                long_string=long_string+"set logscale x\n"
            long_string=long_string+self.get_text()
            long_string=long_string+"splot '"+pts_file_name+"' using 1:3:4 "
        else :
            #                     "set style fill solid 1.000000 \n" (not working:)
            if ( self.get_logy() ) :
                long_string=long_string+"set logscale y\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logz() ) :
                long_string=long_string+"set logscale z\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logx() ) :
                long_string=long_string+"set logscale x\n"
            long_string=long_string+self.get_text()
            long_string=long_string+"splot '"+pts_file_name+"' using 1:2:3 "
        long_string=long_string+" t '"+self.get_marker_text()+"' with "\
                     +"points pt 5 ps 2  palette\n"

#                    +self.get_marker_type()+" lw "+str(self.get_line_width())+" "+str(self.get_line_color())+" 1\n "
        gnu_cmd.write(long_string)
        gnu_cmd.close()

        #Make the plot and convert it to jpg.
        os.system("gnuplot %s" % (gnu_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s"
                  % (ps_file_name, jpg_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s"
                  % (ps_file_name, stamp_jpg_file_name))

        #Cleanup the temporary files.
        self.remove(pts_file_name) # remove pts file
        self.remove(gnu_file_name)  # remove gnu file


    def plot_ascii(self, directory="./"):
        #Get some filenames for the various files that get created.
        pts_file_name = os.path.join(directory, self.data_file_name)
        ps_file_name = os.path.join(directory, self.name + ".ps")
        jpg_file_name = os.path.join(directory, self.name + ".jpg")
        stamp_jpg_file_name = os.path.join(directory, self.name + "_stamp.jpg")
        gnu_file_name = "/tmp/%s_gnuplot.cmd" % (self.name)

        #Polulate the data file that gnuplot will plot.
        self.save_data(pts_file_name)

        #Create the file that contains the commands for gnuplot to run.
        gnu_cmd = open(gnu_file_name,'w')
        long_string="set output '" + ps_file_name + "'\n"+ \
                     "set terminal postscript color solid\n"\
                     "set title '"+self.title+" %s'"%(time.strftime("%Y-%b-%d %H:%M:%S",time.localtime(time.time())))+"\n" \
                     "set xrange [  :  ]\n"+\
                     "set size 1.5,1\n"+ \
                     "set grid\n"+ \
                     "set ylabel '%s'\n"%(self.ylabel)+ \
                     "set zlabel '%s'\n"%(self.zlabel)+ \
                     "set xlabel '%s'\n"%(self.xlabel)+ \
                     string.join(self.commands,"\n")+"\n"
        for bin in range(self.nbins) :
            x,y=self.get_bin_center(bin)
            count=self.get_bin_content(bin)
            if (count>0):
                if (  self.time_axis ) :
                    self.add_text("set label \"%5d\" at \"%s\",%f center font \"Helvetica,10\"\n"%(count,time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(x)),
                                                                                               y),)
                else:
                    self.add_text("set label \"%5d\" at %f,%f center font \"Helvetica,10\"\n"%(count,x,y,))
        if (  self.time_axis ) :
            long_string=long_string+"set xlabel 'Date (year-month-day)'\n"+ \
                         "set xdata time\n"+ \
                         "set timefmt \"%Y-%m-%d %H:%M:%S\"\n"+ \
                         "set format x \"%y-%m-%d\"\n"
            if ( self.get_logy() ) :
                long_string=long_string+"set logscale y\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logz() ) :
                long_string=long_string+"set logscale z\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logx() ) :
                long_string=long_string+"set logscale x\n"
            long_string=long_string+self.get_text()
            long_string=long_string+"plot '"+pts_file_name+"' using 1:3 "
        else :
            #                     "set style fill solid 1.000000 \n" (not working:)
            if ( self.get_logy() ) :
                long_string=long_string+"set logscale y\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logz() ) :
                long_string=long_string+"set logscale z\n"
                long_string=long_string+"set yrange [ 0.99  : ]\n"
            if ( self.get_logx() ) :
                long_string=long_string+"set logscale x\n"
            long_string=long_string+self.get_text()
            long_string=long_string+"plot '"+pts_file_name+"' using 1:2 "
        long_string=long_string+" t '"+self.get_marker_text()+"' with "\
                    +self.get_marker_type()+" lw "+str(self.get_line_width())+" lt  "+str(self.get_line_color())+" \n "
        gnu_cmd.write(long_string)
        gnu_cmd.close()

        #Make the plot and convert it to jpg.
        os.system("gnuplot %s" % (gnu_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -modulate 80 %s %s"
                  % (ps_file_name, jpg_file_name))
        os.system("convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s %s"
                  % (ps_file_name, stamp_jpg_file_name))

        #Cleanup the temporary files.
        self.remove(pts_file_name) # remove pts file
        self.remove(gnu_file_name)  # remove gnu file


if __name__ == "__main__":
    ntuple = Ntuple("gauss2D","gauss2D")
    h1=Histogram1D("try","try",100,0,10)
    h2=Histogram1D("try1","try1",100,0,10)
    h3=Histogram1D("try2","try2",1000,0,100)
    hh=Histogram2D("2d","2d",100,0,5,100,0,5)

    while ( h1.n_entries() < 10000 ) :
            x=random.gauss(2,0.5)
            y=random.gauss(2,0.5)
            ntuple.get_data_file().write("%f %f\n"%(x,y));
            hh.fill(x,y)
            h1.fill(x)
            x=random.gauss(7,0.5)
            h2.fill(x)
            x=random.gauss(77,0.5)
            h3.fill(x)
    ntuple.get_data_file().close()
    ntuple.set_line_color(2)
    ntuple.set_line_width(5)
    ntuple.plot("1:2")
    hh.plot()
    h1.set_ylabel("Counts / %s"%(h1.get_bin_width(0)))
    h1.set_xlabel("x variable")
    h1.set_marker_text("blah")
    h1.set_marker_type("impulses")
    h1.set_opt_stat(True)
    h1.set_line_width(10)
    t = time.ctime(time.time())
    h1.add_text("set label \"Plotted %s \" at graph .99,0 rotate font \"Helvetica,10\"\n"% (t,))
    h1.add_text("set label \"Should %s, Done %s(%3.1f%%), Not Done %s.\" at graph .05,.90\n" % (100,100,0.7,100))
    derivative = h1.derivative()
    derivative.plot()
    integral = h1.integral("integral","integral",True);
    integral.plot();
    h1.plot()

    #
    # this is how we plot two histograms together
    #

    h2.set_ylabel("Counts / %s"%(h2.get_bin_width(0)))
    h2.set_xlabel("x variable")
    h2.set_marker_text("blah")
    h2.set_marker_type("impulses")
    h2.set_logy(False)
    h2.set_opt_stat(True)
    h2.set_line_width(10)
    h2.set_line_color(3)
    t = time.ctime(time.time())
    h2.add_text("set label \"Plotted %s \" at graph .99,0 rotate font \"Helvetica,10\"\n"% (t,))
    h2.add_text("set label \"Should %s, Done %s(%3.1f%%), Not Done %s.\" at graph .05,.90\n" % (100,100,0.7,100))
    h2.plot2(h1,True)

    #
    # this is how we stack two histograms
    #
    plotter = Plotter("plotter","test plotter")
    sum_p=h1+h3
    sum_p.plot()
    plotter.add(h1)
    plotter.add(h2)
    plotter.add(sum_p)
    plotter.add_command("set key outside")
    plotter.plot()

    #
    # time axis example
    #

    now    = int(time.time())
    then   = now - 30*3600*24
    middle = now - 15*3600*24
    width  = 4*3600*24
    h11=Histogram1D("time","time",100,then,now)
    ntuple1 = Ntuple("time_ntuple","time_ntuple")
    ntuple1.set_marker_type("impulses")
    ntuple1.set_time_axis_format("%m-%d");
    ntuple1.set_ylabel("time")
    ntuple1.set_xlabel("(hour:minute)")
    ntuple1.set_time_axis()

    while ( h11.n_entries() < 10000 ) :
        x=random.gauss(float(middle),float(width))
        h11.fill(x)
        y=random.gauss(2,0.5)
        ntuple1.get_data_file().write("%s %f\n"%(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(x)),y))

    h11.set_time_axis(True)
    h11.set_opt_stat()
    h11.plot()
    ntuple1.get_data_file().close();
    ntuple1.plot("1:3")

    sys.exit(0)

