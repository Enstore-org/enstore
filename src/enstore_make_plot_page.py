import os
import sys
import string
import stat

import enstore_plots
import enstore_html
import enstore_files
import generic_client

TMP = ".tmp"

ENGLISH_TITLES = { 
		   "ADICDrvBusy" : "(e)Get ADIC Drive Info",
		   "aml2logs" : "(e)Get AML2 Logs",
		   "aml2mirror" : "(e)Mirror AML2 Disk",
		   "backup" : "(e)Backup Enstore Db",
		   "backupsystem2Tape" : "(e)Backup tarit Output To Tape",
		   "backup2Tape" : "(e)Back up Metadata To Tape Daily",
		   "checkPNFS" : "(r)Check PNFS for Errors",
		   "checkdb" : "(e)Scan Db keys",
		   "chkcrons" : "(r)Make CRON plots",
		   "cleaning_report" : "(e)Get # Cleanings/Tapes",
		   "copy_ran_file" : "(e)Read 3 Random Files",
		   "delfile" : "(r)PNFS delete",
		   "dailyblanks" : "(e)STK Tape/Blank Rpt",
		   "d0-aml2-drives" : "(e)Check D0 AML/2 Drive Status",
		   "enstoreNetwork" : "(e)Measure Network Encp Speeds",
		   "enstoreSystem" : "(e)Make SAAG Page",
		   "enstore_overall_status" : "(r)Make Operator SAAG Page",
		   "failedX" : "(e)Get Failed Transfers",
		   "fix_url" : "(e)Fix CGI URLs",
		   "getcons" : "(r)Get Console Logs",
		   "getnodeinfo" : "(r)Get Node Info",
		   "inqPlotUpdate" : "(e)Make Inq Plots",
		   "log-stash" : "(e)Save Logs to history",
		   "make_cron_plot_page" : "(r)Make CRON Plot Page",
		   "make_overall_plot" : "(r)Make Total BPD Plot",
		   "makeplot" : "(e)Make Rate Plots",
		   "noaccess" : "(e)Make VOLUME/NOACCESS Page",
		   "offline_inventory" : "(e)Get Tape Inv Info",
		   "PNFSRATE" : "(e)Get PNFS Rate Info",
		   "plog" : "(r)Rollover PNFS Log",
		   "pnfsExports" : "(r)Make PNFS Exports Page",
		   "pnfsFastBackup" : "(r)Backup PNFS Files",
		   "quickcheck" : "(r)Get Netperf Rates",
		   "quickquota" : "(e)Get Volume Quotas",
		   "raidcheck" : "(r)Check Raid Disk",
		   "readDcache" : "(e)Read From Disk Cache",
		   "rdist-log" : "(e)Backup Enstore Logs to srv3",
		   "robot_inventory" : "(e)Get AML Volume Status",
		   "STKDrvBusy" : "(r,e)Get STK Drive Info",
		   "STKlog" : "(e)Get STK Logs",
		   "STKquery" : "(e)Measure STK Response Time",
		   "STKrobot_inventory" : "(e)Get STK Volume Info",
		   "Sdr" : "(r)Get Node Temps/Fan",
		   "Sel" : "(r)Get System Event Log",
		   "selbit" : "(r)Clear System Event Log",
		   "sgPlotUpdate" : "(e)Make Storage Group Plot",
		   "silocheck" : "(e)Find Non-Standard Tapes",
		   "stk_response_time" : "(e)STK Response Time",
		   "syncit" : "(r)Rsync Mover Nodes",
                   "tarit" : "(r)Backup Host Files",
		   "udpclog" : "(r)Get UDP Clog Info",
		   "user_bytes" : "(e)Get User Bytes on Tape"
		   "volmap" : "(r)Chmod volmap Dirs" }

# find all the files under the current directory that are jpg files.
# (*.jpg). then create a smaller version of each file (if it does not 
# exist) with the name *_stamp.jpg, to serve as a stamp file on 
# the web page and create a ps file (if it does not exist) with the
# name *.ps.
def find_jpg_files((jpgs, stamps, pss, input_dir, url), dirname, names):
    ignore = []
    (tjpgs, tstamps, tpss) = enstore_plots.find_files(names, dirname, ignore)
    dir = string.split(dirname, input_dir)[-1]
    # when we add the found file to the list of files, we need to add the
    # directory that it was found in to the name
    for file in tjpgs:
	# add the last modification time of the file
        jpgs.append(("%s%s/%s"%(url, dir, file[0]), file[1]))
    for file in tstamps:
        stamps.append(("%s%s/%s"%(url, dir, file[0]), file[1]))
    for file in tpss:
        pss.append(("%s%s/%s"%(url, dir, file[0]), file[1]))

def do_the_walk(input_dir, url):
    # walk the directory tree structure and return a list of all jpg, stamp
    # and ps files
    jpgs = []
    stamps = []
    pss = []
    # make sure the input directory and url contain the ending / in them
    if input_dir[-1] != "/":
        input_dir = "%s/"%(input_dir,)
    if url[-1] != "/":
        url = "%s/"%(url,)
    os.path.walk(input_dir, find_jpg_files, (jpgs, stamps, pss, 
					     input_dir, url))
    jpgs.sort()
    stamps.sort()
    pss.sort()
    return (jpgs, stamps, pss)

class CronPlotPage(enstore_html.EnPlotPage):

    def __init__(self, title, gif, description, url):
        self.url = url
        enstore_html.EnPlotPage.__init__(self, title, gif, description)
	self.source_server = "Enstore"
	self.help_file = "cronHelp.html"

    def find_label(self, text):
        l = len(self.url)
        # get rid of the .jpg ending and the url at the beginning and _stamp
	text_lcl = text[l:-10]
	# translate this text to more understandable english
        # currently it has this format - node/cronjob. make this node/text.
        nodeNName = string.split(text_lcl, "/")
        text = ENGLISH_TITLES.get(nodeNName[1], nodeNName[1])
	return "%s<BR>%s<BR>"%(text_lcl, text)

class PlotPageInterface(generic_client.GenericClientInterface):

    def __init__(self, flag=1, args=sys.argv, user_mode=1):
	# fill in the defaults for the possible options
        self.do_parse = flag
	self.description = "Graphical representation of the exit status of Enstore cron jobs."
	self.title = "Enstore Cron Processes Output"
	self.title_gif = "en_cron_pics.gif"
	self.dir = "/fnal/ups/prd/www_pages/enstore"
	self.input_dir = "%s/CRONS"%(self.dir,)
	self.html_file = "%s/cron_pics.html"%(self.dir,)
        self.url = "CRONS/"
	generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def options(self):
	return self.help_options() +\
	       ["input_dir=", "description=", "title=",
		"html_file=", "title_gif=", "url="]

def do_work(intf):
    # this is where the work is really done
    # get the list of stamps and jpg files
    (jpgs, stamps, pss) = do_the_walk(intf.input_dir, intf.url)
    html_page = CronPlotPage(intf.title, intf.title_gif, intf.description,
                             intf.url)
    html_page.body(jpgs, stamps, pss)
    # open the temporary html file and output the html text to it
    tmp_html_file = "%s%s"%(intf.html_file, TMP)
    html_file = enstore_files.EnFile(tmp_html_file)
    html_file.open()
    html_file.write(html_page)
    html_file.close()
    os.rename(tmp_html_file, intf.html_file)

if __name__ == "__main__" :

    intf = PlotPageInterface(user_mode=0)

    do_work(intf)
