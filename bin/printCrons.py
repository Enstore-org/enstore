#!/usr/bin/env python

import os

# arrange all the cron plots in postscript files, 6 plots per page so
# they can be printed without destroying entire rainforests.

# this value should be obtained from the configuration server
topdir = "/fnal/ups/prd/www_pages/enstore"

crondir = "%s/CRONS"%(topdir,)
epsdir = "%s/EPS"%(topdir,)
PSFILE = ".ps"
INDEX = -len(PSFILE)
plots_per_page = 6
all_plots = "all_cron_plots.ps"
plot_print_file = "plots_to_print.ps"

dirs = os.listdir(crondir)
# make sure the eps directory exists
if not os.path.isdir(epsdir):
    os.mkdir(epsdir)
else:
    try:
        os.system("rm -rf %s/*"%(epsdir,))
    except:
        pass

# for each .ps file under the CRONS directory, create an eps file
for dir in dirs:
    newdir = "%s/%s"%(crondir, dir)
    files = os.listdir(newdir);
    for file in files:
        if file[INDEX:] == PSFILE:
            oldfile = "%s/%s"%(newdir, file)
            filename = file[:INDEX]
            newfile = "%s/%s-%s.eps"%(epsdir, dir, filename)
            ##os.system("convert %s %s"%(oldfile, newfile))
            os.system("ps2epsi %s %s" % (oldfile, newfile))
            

# concatenate all the eps files together
os.system("epscat %s/*.eps > %s/%s"%(epsdir, epsdir, all_plots))

# make them be 6 images per page
os.system("psnup -d -%s %s/%s > %s/%s"%(plots_per_page, epsdir, all_plots, 
                                        epsdir, plot_print_file))
print "\nprintCrons: Created %s/%s, please print this file.\n"%(epsdir,
                                                               plot_print_file)
