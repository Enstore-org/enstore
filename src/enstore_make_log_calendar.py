#!/fnal/ups/prd/python/v1_5_2/Linux+2/bin/python
######################################################################
# src/$RCSfile$   $Revision$
#
import time
import os

import enstore_html
import enstore_files
import inquisitor
import generic_client

CAPTION_DEFAULT = "Log Files"
LPFILE = "./log_file.html"
TMP = ".tmp"

# return a tuple containing todays year, month and date as integers)
def get_today():
    date = time.localtime(time.time())
    return (date[0], date[1], date[2])

class LogPage(enstore_html.EnLogPage):

    def __init__(self, title, description, prefix, refresh=-1):
	enstore_html.EnLogPage.__init__(self, refresh)
	self.title = title
	self.prefix = prefix
	self.script_title_gif = "en_olog.gif"
	self.description = description

    def body(self, logfile_dir, web_host, caption_title=CAPTION_DEFAULT):
	table = self.table_top()
	logs = inquisitor.get_file_list(logfile_dir, self.prefix)
	self.generate_months(table, logs, web_host, caption_title)
	self.append(table)

class Aml2LogPage(LogPage):

    # get the date of the log file from the name of it
    def logfile_date(self, log):
	plen = len(self.prefix)
	day = int(log[plen:plen+2])
	month = int(log[plen+2:plen+4])
	# the year is not encoded in the aml log file names (bogus!!). so we must try to
	# figure out what year this log is for.  if the date is greater than todays date, then
	# assume it was last year.  else, assume it is this year.
	(tyear, tmonth, tday) = get_today()
	if month > tmonth:
	    # this was last year
	    year = tyear - 1
	elif month < tmonth:
	    # this is this year
	    year = tyear
	else:
	    # the month is the same so need to look at the day
	    if day > tday:
		# this was last year
		year = tyear - 1
	    else:
		# this is this year.  we are making an assumption here. that the log file with
		# todays date belongs to the current year.  in order to do this, this program 
		# must be run after the log file is brought over.
		year = tyear
	return (self.prefix, year, month, day)

class LogPageInterface(generic_client.GenericClientInterface):

    def __init__(self, flags=1, opts=[]):
	# fill in the defaults for the possible options
	self.logfile_dir = "./"
	self.prefix = "log"
	self.web_host = str(os.uname()[1])
	self.caption_title = CAPTION_DEFAULT
	self.description = "List of existing Log files"
	self.title = "Enstore User Logs"
	self.output = LPFILE
	generic_client.GenericClientInterface.__init__(self)

    def options(self):
	return self.help_options() +\
	       ["logfile_dir=", "prefix=", "web_host=", "caption_title=",
		"description=", "title=", "output="]

def aml2_do_work(intf):
    # this is where the work is really done
    # parse the data and create the html
    lp = Aml2LogPage(intf.title, intf.description, intf.prefix)
    lp.body(intf.logfile_dir, intf.web_host, intf.caption_title)
    # open the temporary html file and output the html text to it
    tmp_lp_filename = "%s%s"%(intf.output, TMP)
    lp_file = enstore_files.EnFile(tmp_lp_filename)
    lp_file.open()
    lp_file.filedes.write(str(lp))
    lp_file.close()
    os.rename(tmp_lp_filename, intf.output)

if __name__ == "__main__" :

    intf = LogPageInterface()

    aml2_do_work(intf)
