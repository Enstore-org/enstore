#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import os
import sys
import string
import time

# enstore imports
import enstore_plots
import enstore_html
import enstore_files
import enstore_functions
import enstore_functions2
import enstore_constants
import generic_client
import option
import configuration_client

"""
The first part of code is the old way this script worked.  It is used from
under the do_work() function.  The second part of the code works with
do_work2(), this is a different way of doing this (general idea copied from
plotter.py).  I don't believe that the old way is capable of creating
all the sub-directories worth of plots now created.  This file was previously
last touched in June of 2004 when the plethora of plot sub-directories
did not exist like they do now (May 2008).

The only reason the old way is still around is because it had more support
for overriding things on the command line; in case someone still cares.  The
enstore_html command uses the new way.

MZ - May 23rd, 2008
"""

#########################################################################
#  START OF OLD WAY
#########################################################################

TMP = ".tmp"

CRON = "cron"
QUOTA = "quota"


# in order to add support for another plot page add the following -
#
#    - a class that inherits from PlotPage
#    - a elif clause in do_work
#


ENGLISH_TITLES = {
    "ADICDrvBusy": "(e)Get ADIC Drive Info",
    "aml2logs": "(e)Get AML2 Logs",
                   "aml2mirror": "(e)Mirror AML2 Disk",
                   "backup": "(e)Backup Enstore Db",
                   "backupsystem2Tape": "(e)Backup tarit Output To Tape",
                   "backup2Tape": "(e)Back up Metadata To Tape Daily",
                   "checkdb": "(e)Scan Db keys",
                   "chkcrons": "(r)Make CRON plots",
                   "chk_prod_code": "(e)Check Production Code Consistency",
                   "cleaning_report": "(e)Get # Cleanings/Tapes",
                   "copy_ran_file": "(e)Read 3 Random Files",
                   "delfile": "(r)PNFS delete",
                   "dailyblanks": "(e)STK Tape/Blank Rpt",
                   "d0-aml2-drives": "(e)Check D0 AML/2 Drive Status",
                   "enstoreNetwork": "(e)Measure Network Encp Speeds",
                   "enstoreSystem": "(e)Make SAAG Page",
                   "enstore_overall_status": "(r)Make Operator SAAG Page",
                   "failedX": "(e)Get Failed Transfers",
                   "fix_url": "(e)Fix CGI URLs",
                   "getcons": "(r)Get Console Logs",
                   "getnodeinfo": "(r)Get Node Info",
                   "inqPlotUpdate": "(e)Make Inq Plots",
                   "log-stash": "(e)Save Logs to history",
                   "make_cron_plot_page": "(r)Make CRON Plot Page",
                   "make_overall_plot": "(r)Make Total BPD Plot",
                   "makeplot": "(e)Make Rate Plots",
                   "noaccess": "(e)Make VOLUME/NOACCESS Page",
                   "offline_inventory": "(e)Get Tape Inv Info",
                   "PNFSRATE": "(e)Get PNFS Rate Info",
                   "plog": "(r)Rollover PNFS Log",
                   "pnfsExports": "(r)Make PNFS Exports Page",
                   "pnfsFastBackup": "(r)Backup PNFS Files",
                   "quickcheck": "(r)Get Netperf Rates",
                   "quickquota": "(e)Get Volume Quotas",
                   "raidcheck": "(r)Check Raid Disk",
                   "readDcache": "(e)Read From Disk Cache",
                   "rdist-log": "(e)Backup Enstore Logs to srv3",
                   "robot_inventory": "(e)Get AML Volume Status",
                   "STKDrvBusy": "(r,e)Get STK Drive Info",
                   "STKlog": "(e)Get STK Logs",
                   "STKquery": "(e)Measure STK Response Time",
                   "STKrobot_inventory": "(e)Get STK Volume Info",
                   "Sdr": "(r)Get Node Temps/Fan",
                   "Sel": "(r)Get System Event Log",
                   "selbit": "(r)Clear System Event Log",
                   "sgPlotUpdate": "(e)Make Storage Group Plot",
                   "silocheck": "(e)Find Non-Standard Tapes",
                   "stk_response_time": "(e)STK Response Time",
                   "syncit": "(r)Rsync Mover Nodes",
                   "tarit": "(r)Backup Host Files",
                   "udpclog": "(r)Get UDP Clog Info",
                   "user_bytes": "(e)Get User Bytes on Tape",
                   "volmap": "(r)Chmod volmap Dirs"}

# find all the files under the current directory that are jpg files.
# (*.jpg). then create a smaller version of each file (if it does not
# exist) with the name *_stamp.jpg, to serve as a stamp file on
# the web page and create a ps file (if it does not exist) with the
# name *.ps.


def find_jpg_files(xxx_todo_changeme, dirname, names):
    (jpgs, stamps, pss, input_dir, url) = xxx_todo_changeme
    ignore = []
    (tjpgs, tstamps, tpss) = enstore_plots.find_files(names, dirname, ignore)
    dir_name = string.split(dirname, input_dir)[-1]
    # when we add the found file to the list of files, we need to add the
    # directory that it was found in to the name
    for filename in tjpgs:
        # add the last modification time of the file
        jpgs.append(("%s%s/%s" % (url, dir_name, filename[0]), filename[1]))
    for filename in tstamps:
        stamps.append(("%s%s/%s" % (url, dir_name, filename[0]), filename[1]))
    for filename in tpss:
        pss.append(("%s%s/%s" % (url, dir_name, filename[0]), filename[1]))


def do_the_walk(input_dir, url):
    # walk the directory tree structure and return a list of all jpg, stamp
    # and ps files
    jpgs = []
    stamps = []
    pss = []
    # make sure the input directory and url contain the ending / in them
    if input_dir[-1] != "/":
        input_dir = "%s/" % (input_dir,)
    if url[-1] != "/":
        url = "%s/" % (url,)
    os.path.walk(input_dir, find_jpg_files, (jpgs, stamps, pss,
                                             input_dir, url))
    jpgs.sort()
    stamps.sort()
    pss.sort()
    return (jpgs, stamps, pss)


class PlotPage(enstore_html.EnPlotPage):

    def __init__(self, title, gif, description, url, outofdate=0):
        self.url = url
        enstore_html.EnPlotPage.__init__(self, title, gif, description)
        self.source_server = "Enstore"
        self.help_file = ""
        self.outofdate = outofdate
        if self.outofdate:
            today = time.time()
            day_secs = 26. * 60. * 60.
            self.yesterday = today - day_secs
        else:
            self.yesterday = 0.0
        self.english_titles = {}


class CronPlotPage(PlotPage):

    def __init__(self, title, gif, description, url, outofdate=0):
        PlotPage.__init__(self, title, gif, description, url, outofdate)
        self.help_file = "cronHelp.html"
        self.english_titles = ENGLISH_TITLES

    def find_label(self, text):
        l = len(self.url)
        # get rid of the .jpg ending and the url at the beginning and _stamp
        text_lcl = text[l:-10]
        # translate this text to more understandable english
        # currently it has this format - node/cronjob. make this node/text.
        nodeNName = string.split(text_lcl, "/")
        text = self.english_titles.get(nodeNName[1], nodeNName[1])
        return "%s<BR>%s<BR>" % (text_lcl, text)


class QuotaPlotPage(PlotPage):

    def __init__(self, title, gif, description, url, outofdate=0):
        PlotPage.__init__(self, title, gif, description, url, outofdate)
        self.english_titles = {}

    def find_label(self, text):
        l = len(self.url)
        # get rid of the .jpg ending and the url at the beginning and _stamp
        text_lcl = text[l:-10]
        # get rid of the // and then use this
        text_lcl = string.replace(text_lcl, "/", "")
        return text_lcl


class PlotPageInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        self.description = "Graphical representation of the exit status of Enstore cron jobs."
        self.title = "Enstore Cron Processes Output"
        self.title_gif = "en_cron_pics.gif"
        self.dir = enstore_functions.get_html_dir()
        self.input_dir = "%s/CRONS" % (self.dir,)
        self.html_file = "%s/cron_pics.html" % (self.dir,)
        self.url = "CRONS/"
        self.plot = ""
        self.outofdate = 0
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    plot_options = {
        option.INPUT_DIR: {option.HELP_STRING: "directory containing plot image files",
                           option.VALUE_TYPE: option.STRING,
                           option.VALUE_USAGE: option.REQUIRED,
                           option.VALUE_LABEL: "directory",
                           option.USER_LEVEL: option.ADMIN,
                           },
        option.DESCRIPTION: {option.HELP_STRING: "description for html page",
                             option.VALUE_TYPE: option.STRING,
                             option.VALUE_USAGE: option.REQUIRED,
                             option.VALUE_LABEL: "text",
                             option.USER_LEVEL: option.ADMIN,
                             },
        option.TITLE: {option.HELP_STRING: "title for html page",
                       option.VALUE_TYPE: option.STRING,
                       option.VALUE_USAGE: option.REQUIRED,
                       option.VALUE_LABEL: "text",
                       option.USER_LEVEL: option.ADMIN,
                       },
        option.HTML_FILE: {option.HELP_STRING: "html file to create",
                           option.VALUE_TYPE: option.STRING,
                           option.VALUE_USAGE: option.REQUIRED,
                           option.VALUE_LABEL: "filename",
                           option.USER_LEVEL: option.ADMIN,
                           },
        option.TITLE_GIF: {option.HELP_STRING: "gif image containing title of html page",
                           option.VALUE_TYPE: option.STRING,
                           option.VALUE_USAGE: option.REQUIRED,
                           option.VALUE_LABEL: "gif_file",
                           option.USER_LEVEL: option.ADMIN,
                           },
        option.URL: {option.HELP_STRING: "url to use for plot images",
                     option.VALUE_TYPE: option.STRING,
                     option.VALUE_USAGE: option.REQUIRED,
                     option.VALUE_LABEL: "url",
                     option.USER_LEVEL: option.ADMIN,
                     },
        option.PLOT: {option.HELP_STRING: "type of html plot page to create",
                      option.VALUE_TYPE: option.STRING,
                      option.VALUE_USAGE: option.REQUIRED,
                      option.VALUE_LABEL: "type",
                      option.USER_LEVEL: option.ADMIN,
                      },
        option.OUTOFDATE: {option.HELP_STRING: "if the plot is older than 26 hours, flag it",
                           option.DEFAULT_TYPE: option.INTEGER,
                           option.DEFAULT_VALUE: option.DEFAULT,
                           option.DEFAULT_NAME: "outofdate",
                           option.VALUE_USAGE: option.IGNORED,
                           option.USER_LEVEL: option.ADMIN,
                           }
    }

    def valid_dictionaries(self):
        return (self.help_options, self.plot_options)


def do_work(intf):
    # this is where the work is really done
    # get the list of stamps and jpg files
    if intf.plot == CRON:
        html_page = CronPlotPage(intf.title, intf.title_gif, intf.description,
                                 intf.url, intf.outofdate)
    elif intf.plot == QUOTA:
        html_page = QuotaPlotPage(intf.title, intf.title_gif, intf.description,
                                  intf.url, intf.outofdate)
    else:
        html_page = None

    if html_page:
        (jpgs, stamps, pss) = do_the_walk(intf.input_dir, intf.url)
        html_page.body(jpgs, stamps, pss)
        # open the temporary html file and output the html text to it
        tmp_html_file = "%s%s" % (intf.html_file, TMP)
        html_file = enstore_files.EnFile(tmp_html_file)
        html_file.open()
        html_file.write(html_page)
        html_file.close()
        os.rename(tmp_html_file, intf.html_file)

#########################################################################
#  END OF OLD WAY
#########################################################################

#########################################################################
#  START OF NEW WAY
#########################################################################

# full_subdir_path : Absolute path to the directory containing plots
#                    to have the a plot page created.
# url_gif_dir : URL path for finding the gifs for things like the
#               page background.
# plot_name : A string describing the contents of the plots in the
#             plot directory.
# links_l : Only the top page should specify this.  It is a list of
#           tuples containing the sub-plot-directories and the description
#           of the plots.


def make_plot(full_subdir_path, url_gif_dir, plot_name, links_l=None):

    # Append the plots filename that will be created in this directory.
    plot_file = os.path.join(full_subdir_path,
                             enstore_files.plot_html_file_name())

    # For all the link dirs, we need to append the plots filename that
    # will be created in this directory.
    use_links_l = []
    if links_l:
        for link_dir, link_name in links_l:
            use_links_l.append((os.path.join(link_dir,
                                             enstore_files.plot_html_file_name()),
                                link_name))

    # Override use of system_tag to contain the name of the plot page.
    system_tag = plot_name
    html_of_plots = enstore_files.HTMLPlotFile(plot_file, system_tag, url_gif_dir,
                                               url_gif_dir=url_gif_dir)
    html_of_plots.open()
    # get the list of stamps and jpg files
    (jpgs, stamps, pss) = enstore_plots.find_jpg_files(full_subdir_path)
    mount_label = ""  # ???
    html_of_plots.write(jpgs, stamps, pss, mount_label, use_links_l)
    html_of_plots.close()
    html_of_plots.install()


def do_work2(intf):
    # Get the configuration server.
    csc = configuration_client.ConfigurationClient((intf.config_host,
                                                    intf.config_port))
    # Get directory information we are going to need.
    crons_dict = csc.get('crons', {})
    html_dir = crons_dict.get('html_dir', None)
    url_dir = crons_dict.get('url_dir', "")

    if not html_dir:
        sys.stderr.write("Unable to determine html_dir.\n")
        return

    plots_subdir = os.path.join(html_dir, enstore_constants.PLOTS_SUBDIR)

    # Is there a better place for this list?
    subdir_description_list = [
        (enstore_constants.MOUNT_PLOTS_SUBDIR,
         "Number of Mounts per Media Type"),
        (enstore_constants.RATEKEEPER_PLOTS_SUBDIR,
         "Instantaneous Encp Rates"),
        (enstore_constants.DRIVE_UTILIZATION_PLOTS_SUBDIR,
         "Tape Drives in Use per Drive Type"),
        (enstore_constants.DRIVE_HOURS_PLOTS_SUBDIR,
         "Tape Drive usage hours per drive type, stacked by storage group"),
        (enstore_constants.DRIVE_HOURS_SEP_PLOTS_SUBDIR,
         "Tape Drive usage hours per drive type, separately for each storage group"),
        (enstore_constants.SLOT_USAGE_PLOTS_SUBDIR,
         "Tape Slot usage per Robot"),
        (enstore_constants.PNFS_BACKUP_TIME_PLOTS_SUBDIR,
         "Time to Backup PNFS DB"),
        (enstore_constants.FILE_FAMILY_ANALYSIS_PLOT_SUBDIR,
         "Tape occupancies per Storage Group Plots"),
        (enstore_constants.FILES_RW_PLOTS_SUBDIR,
         "Files read and written per mount per drive type, stacked by storage group"),
        (enstore_constants.FILES_RW_SEP_PLOTS_SUBDIR,
         "Files read and written per mount per drive type, separately for each storage group"),
        (enstore_constants.ENCP_RATE_MULTI_PLOTS_SUBDIR,
         "Encp rates per Storage Group Plots"),
        (enstore_constants.QUOTA_PLOTS_SUBDIR,
         "Quota per Storage Group Plots"),
        (enstore_constants.TAPES_BURN_RATE_PLOTS_SUBDIR,
         "Bytes Written per Storage Group Plots"),
        (enstore_constants.BPD_PER_MOVER_PLOTS_SUBDIR,
         "Bytes/Day per Mover Plots"),
        (enstore_constants.XFER_SIZE_PLOTS_SUBDIR,
         "Xfer size per Storage Group Plots"),
        (enstore_constants.MIGRATION_SUMMARY_PLOTS_SUBDIR,
         "Migration/Duplication Summary Plots per Media Type"),
        (enstore_constants.MOVER_SUMMARY_PLOTS_SUBDIR,
         "Mover Plots"),
        (enstore_constants.MOUNT_LATENCY_SUMMARY_PLOTS_SUBDIR,
         "Mount Latency plots"),
        (enstore_constants.MOUNTS_PER_ROBOT_PLOTS_SUBDIR,
         "Mounts/day per tape library"),
    ]

    if csc.get("dispatcher"):
        # Append link only if SFA is in configuration
        subdir_description_list.append((enstore_constants.SFA_STATS_PLOTS_SUBDIR,
                                        "Small Files Aggregation Statistics"))

        use_subdir_list = []
    # Loop over all the plot subdirs making pages.
    for subdir, plot_name in subdir_description_list:
        full_subdir_path = os.path.join(plots_subdir, subdir)
        # The top plots directory needs some additional attention.
        if full_subdir_path == plots_subdir:
            continue
        # Skip if the directory does not exist.
        if not os.path.isdir(full_subdir_path):
            continue

        make_plot(full_subdir_path, url_dir, plot_name)

        use_subdir_list.append((subdir, plot_name))

    # Create the top plot page.
    make_plot(plots_subdir, url_dir, "Enstore Plots", use_subdir_list)

#########################################################################
#  END OF NEW WAY
#########################################################################


if __name__ == "__main__":

    intf = PlotPageInterface(user_mode=0)

    do_work(intf)
    # do_work2(intf)
