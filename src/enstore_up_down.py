#!/usr/bin/env python

import os
import sys
import string
import tempfile
import time
import errno

import e_errors
import timeofday

import configuration_client
import log_client
import alarm_client
import inquisitor_client
import file_clerk_client
import volume_clerk_client
import library_manager_client
import media_changer_client
import mover_client
import generic_client
import enstore_constants
import enstore_functions
import enstore_files

timeout=30
tries=1
is_down = enstore_constants.UP
mail_sent = 0
prefix = ""
outage_d = {}
offline_d = {}

# states of a mover meaning 'alive but not available for work'
BADMOVERSTATUS = ['offline', 'draining']
# states of a library manager meaning 'alive but not available for work'
BADLMSTATUS = ['ignore', 'draining']

class UpDownInterface(generic_client.GenericClientInterface):
 
    def __init__(self, flag=1, opts=[]):
        self.do_parse = flag
        self.restricted_opts = opts
	self.summary = 0
	self.html = 0
	generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.help_options() + ["summary", "html"]
                                           
def sendmail(subject, server):
    global mail_sent
    if not mail_sent:
	# see if this server is known to be down, if so, then do not send mail
	if not offline_d.has_key(server):
	    # first get a tempfile
	    file = tempfile.mktemp()
	    os.system("echo ' Message from enstore_up_down.py:\n\n\tPlease check the full Enstore software system.\n\n" + \
		      "See the Status-at-a-Glance Web Page\n\n' > %s"%(file,))
	    os.system("date >> %s"%(file,))
	    os.system("/usr/bin/Mail -s '%s' $ENSTORE_MAIL < %s"%(subject, file))
	    os.system("rm %s"%(file,))
	    mail_sent = 1

def set_down(subject, server, status=None):
    global is_down
    # only set the status to what was passed in, if it is not already marked as down
    if status and not is_down == enstore_constants.DOWN:
	is_down = status
    else:
	is_down = enstore_constants.DOWN
    if subject:
	sendmail(subject, server)

# the third parameter is used to determine the state of enstore if this server is 
# considered down.  some servers being down will mark enstore as down, others will
# not. 'rtn' records the state of the server.  'status' records the state of enstore
# as a result of the DOWN state of this server.
def check_ticket(server, ticket, status, summary, summary_d, config_host):
    if not 'status' in ticket.keys():
	if not summary:
	    print prefix, timeofday.tod(),server,' NOT RESPONDING'
	if not status == enstore_constants.UP:
	    set_down("%s is not alive (config node - %s)"%(server, config_host), server, status)
	rtn = enstore_constants.DOWN
    elif ticket['status'][0] == e_errors.OK:
	if not summary:
	    print prefix, timeofday.tod(), server, ' ok'
        rtn = enstore_constants.UP
    else:
	if not summary:
	    print prefix, timeofday.tod(), server, ' BAD STATUS',ticket['status']
	if not status == enstore_constants.UP:
	    set_down("%s is not alive (config node - %s)"%(server, config_host), server, status)
	rtn = enstore_constants.DOWN
    summary_d[server] = rtn
    return rtn

# we need to get the state of the mover.  if the mover is in a draining or an offline
# state, then it does not count towards the total available movers.  we get the mover
# state by sending the mover a 'status' command, not an alive.  if the mover answers
# we know it is alive and we have the status.  if it does not answer, it is not alive
# and the state is DOWN. 'rtn' records if the mover is available for work.
def check_mover_ticket(server, ticket, summary, summary_d, config_host):
    if not 'status' in ticket.keys():
	if not summary:
	    print prefix, timeofday.tod(),server,' NOT RESPONDING'
	sendmail("%s is not alive (config node - %s)"%(server, config_host), server)
	summary_d[server] = enstore_constants.DOWN
	rtn = enstore_constants.DOWN
    elif ticket['status'][0] == e_errors.OK:
	if ticket['state'] == BADMOVERSTATUS[0]:
	    # the mover is not in a good state mark it as bad
	    summary_d[server] = enstore_constants.DOWN
	    if not summary:
		print prefix, timeofday.tod(), server, "in a %s state"%(ticket['state'],)
	    rtn = enstore_constants.DOWN
	elif ticket['state'] == BADMOVERSTATUS[1]:
	    # the mover is not in a good state mark it as yellow
	    summary_d[server] = enstore_constants.WARNING
	    if not summary:
		print prefix, timeofday.tod(), server, "in a %s state"%(ticket['state'],)
	    rtn = enstore_constants.DOWN
	else:
	    if not summary:
		print prefix, timeofday.tod(), server, ' ok'
	    rtn = enstore_constants.UP
	    summary_d[server] = enstore_constants.UP
    else:
	if not summary:
	    print prefix, timeofday.tod(), server, ' BAD STATUS',ticket['status']
	sendmail("%s is not alive (config node - %s)"%(server, config_host), server)
	summary_d[server] = enstore_constants.DOWN
	rtn = enstore_constants.DOWN
    return rtn

# we need to get the state of the library manager.  if the lm is in a draining or an ignor
# state, then mark it as yellow.  we get the lm
# state by sending the lm a 'status' command, not an alive.  if the mover answers
# we know it is alive and we have the status.  if it does not answer, it is not alive
# and the state is DOWN. 'rtn' records if the lm is available for work.
def check_lm_ticket(server, ticket, summary, summary_d, config_host):
    if not 'status' in ticket.keys():
	if not summary:
	    print prefix, timeofday.tod(),server,' NOT RESPONDING'
	sendmail("%s is not alive (config node - %s)"%(server, config_host), server)
	summary_d[server] = enstore_constants.DOWN
    elif ticket['status'][0] == e_errors.OK:
	if ticket['state'] in BADLMSTATUS:
	    # the lm is not in a good state mark it as yellow
	    summary_d[server] = enstore_constants.WARNING
	    if not summary:
		print prefix, timeofday.tod(), server, "in a %s state"%(ticket['state'],)
	else:
	    if not summary:
		print prefix, timeofday.tod(), server, ' ok'
	    summary_d[server] = enstore_constants.UP
    else:
	if not summary:
	    print prefix, timeofday.tod(), server, ' BAD STATUS',ticket['status']
	sendmail("%s is not alive (config node - %s)"%(server, config_host), server)
	summary_d[server] = enstore_constants.DOWN

def sortit(adict):
    keys = adict.keys()
    keys.sort()
    return keys

def known_down(summary, summary_d, server, enstore_state=None):
    global is_down
    summary_d[server] = enstore_constants.DOWN
    if enstore_state:
	is_down = enstore_state
    if not summary:
	print prefix, timeofday.tod(), server, ' known down'

def do_real_work(summary):
    global prefix, offline_d, outage_d, is_down
    config_port = string.atoi(os.environ.get('ENSTORE_CONFIG_PORT', 7500))
    config_host = os.environ.get('ENSTORE_CONFIG_HOST', "localhost")
    config=(config_host,config_port)

    html_dir = enstore_functions.get_html_dir()
    # check if the html_dir is accessible
    if os.path.exists(html_dir):
	sfile = enstore_files.ScheduleFile(html_dir, enstore_constants.OUTAGEFILE)
	outage_d, offline_d = sfile.read()

    summary_d = {enstore_constants.TIME: enstore_functions.format_time(time.time())}

    if not summary:
	print prefix, timeofday.tod(), 'Checking Enstore on',config,'with timeout of',timeout,'and tries of',tries
    
    csc = configuration_client.ConfigurationClient(config)
    if not offline_d.has_key(enstore_constants.CONFIGS):
	# see if the server is alive
	check_ticket(enstore_constants.CONFIGS, csc.alive(timeout,tries), 
		     enstore_constants.DOWN, summary, summary_d, config_host)
    else:
	# the server is marked as known down, do not check on it
	known_down(summary, summary_d, enstore_constants.CONFIGS, enstore_constants.DOWN)

    if not offline_d.has_key(enstore_constants.LOGS):
	# see if the server is alive
	lcc = log_client.LoggerClient(csc, "LOG_CLIENT", "log_server")
	check_ticket(enstore_constants.LOGS, lcc.alive('log_server',timeout,tries), 
		     enstore_constants.DOWN, summary, summary_d, config_host)
    else:
	# the server is marked as known down, do not check on it
	known_down(summary, summary_d, enstore_constants.LOGS, enstore_constants.DOWN)

    if not offline_d.has_key(enstore_constants.ALARMS):
	# see if the server is alive
	acc = alarm_client.AlarmClient(csc, timeout,tries)
	check_ticket(enstore_constants.ALARMS, acc.alive('alarm_server',timeout,tries), 
		     enstore_constants.DOWN, summary, summary_d, config_host)
    else:
	# the server is marked as known down, do not check on it
	known_down(summary, summary_d, enstore_constants.ALARMS, enstore_constants.DOWN)

    if not offline_d.has_key(enstore_constants.INQ):
	# see if the server is alive
	ic = inquisitor_client.Inquisitor(csc)
	# the inquisitor doesn't respond promptly in all cases, wait a little longer for it.
	check_ticket(enstore_constants.INQ, ic.alive('inquisitor',timeout*3,tries), 
		     enstore_constants.WARNING, summary, summary_d, config_host)
    else:
	# the server is marked as known down, do not check on it
	known_down(summary, summary_d, enstore_constants.INQ, enstore_constants.WARNING)

    if not offline_d.has_key(enstore_constants.FILEC):
	# see if the server is alive
	fcc = file_clerk_client.FileClient(csc, 0)
	check_ticket(enstore_constants.FILEC, fcc.alive('file_clerk',timeout,tries), 
		     enstore_constants.DOWN, summary, summary_d, config_host)
    else:
	# the server is marked as known down, do not check on it
	known_down(summary, summary_d, enstore_constants.FILEC, enstore_constants.DOWN)

    if not offline_d.has_key(enstore_constants.VOLC):
	# see if the server is alive
	vcc = volume_clerk_client.VolumeClerkClient(csc)
	check_ticket(enstore_constants.VOLC, vcc.alive('volume_clerk',timeout,tries), 
		     enstore_constants.DOWN, summary, summary_d, config_host)
    else:
	# the server is marked as known down, do not check on it
	known_down(summary, summary_d, enstore_constants.VOLC, enstore_constants.DOWN)

    lib_man_d = csc.get_library_managers({})
    library_managers = sortit(lib_man_d)

    meds = {}

    for lm in library_managers:
	print lib_man_d
	lm_name = lib_man_d[lm]['name']
	lmc = library_manager_client.LibraryManagerClient(csc,lm_name)
	if not offline_d.has_key(lm_name):
	    # see if the server is alive
	    check_ticket(lm_name, lmc.get_lm_state(timeout,tries), 
			 enstore_constants.DOWN, summary, summary_d, config_host)
	else:
	    # the server is marked as known down, do not check on it
	    known_down(summary, summary_d, lm_name, enstore_constants.DOWN)

	meds[csc.get_media_changer(lm_name,timeout,tries)] = 1 # no duplicates in dictionary
	movs = {}
	mov=csc.get_movers(lm_name)
	for m in mov:
	    movs[(m['mover'])] = 1 # no duplicates in dictionary
	movers = sortit(movs)
	num_movers = 0
	bad_movers = 0
	for mov in movers:
	    num_movers=num_movers+1
	    if not offline_d.has_key(mov):
		# see if the server is alive
		mvc = mover_client.MoverClient(csc,mov)
		try:
		    mstatus = (mvc.status(timeout*2,tries),)
		except errno.errorcode[errno.ETIMEDOUT]:
		    mstatus = ({},)
		bad_movers = bad_movers + check_mover_ticket(mov, mstatus[0],
							     summary, summary_d, 
							     config_host)
	    else:
		# the server is marked as known down, do not check on it
		known_down(summary, summary_d, mov)
		bad_movers = bad_movers + 1
		
	if bad_movers*2 > num_movers:
	    if not summary:
		print prefix, timeofday.tod(), 'LOW CAPACITY: Found', bad_movers, 'of', num_movers, 'not responding'
	    set_down("Found LOW CAPACITY movers for %s"%(lm,), lm_name)
	    summary_d[lm_name] = enstore_constants.WARNING
	elif bad_movers > 0:
	    set_down("", "", enstore_constants.WARNING)
	else:
	    if not summary:
		print prefix, timeofday.tod(), 'Sufficient capacity of movers for',lm, num_movers-bad_movers, 'of', num_movers, 'responding'

    media_changers = sortit(meds)

    for med in media_changers:
	if not offline_d.has_key(med):
	    # see if the server is alive
	    mcc = media_changer_client.MediaChangerClient(csc,med)
	    check_ticket(med, mcc.alive(med,timeout,tries), enstore_constants.DOWN,
			 summary, summary_d, config_host)
	else:
	    # the server is marked as known down, do not check on it
	    known_down(summary, summary_d, med, enstore_constants.DOWN)

    summary_d[enstore_constants.ENSTORE] = is_down

    if is_down == enstore_constants.DOWN:
	stat = "DOWN"
	rtn = 1
    else:
	stat = "UP"
	rtn = 0

    if not summary:
	print prefix, timeofday.tod(),'Finished checking Enstore... system is defined to be %s'%(stat,)
    return (rtn, summary_d)

def do_work(intf):
    global prefix

    # see if we are supposed to output well-formed html or not
    if intf.html:
	prefix = "<LI>"

    rtn, summary_d = do_real_work(intf.summary)
    return (rtn)

if __name__ == "__main__" :

    # fill in interface
    intf = UpDownInterface()
 
    rtn = do_work(intf)
    sys.exit(rtn)
