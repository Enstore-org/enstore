#!/usr/bin/env python

import os
import sys
import string
import tempfile
import time
import errno
import threading
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

DEFAULT = "default"
# default number of times in a row a server can be down before mail is sent
DEFAULTALLOWEDDOWN = [2, 15]
MOVERALLOWEDDOWN = [7, 120]
mail_sent = 0
prefix = ""
do_output = 0
SYSTEM = 'system'
ALLOWED_DOWN = 'allowed_down'
TRIES = 1

def sortit(adict):
    keys = adict.keys()
    keys.sort()
    return keys

def enprint(text):
    if do_output:
	print prefix, timeofday.tod(), text

def get_allowed_down_index(server, allowed_down, index):
    if allowed_down.has_key(server):
	rtn = allowed_down[server][index]
    elif enstore_functions.is_mover(server):
	rtn = allowed_down.get(enstore_constants.MOVER,
                               MOVERALLOWEDDOWN)[index]
    elif enstore_functions.is_library_manager(server):
	rtn = allowed_down.get(enstore_constants.LIBRARY_MANAGER,
                               DEFAULTALLOWEDDOWN)[index]
    elif enstore_functions.is_media_changer(server):
	rtn = allowed_down.get(enstore_constants.MEDIA_CHANGER,
                               DEFAULTALLOWEDDOWN)[index]
    else:
	rtn = allowed_down.get(DEFAULT, DEFAULTALLOWEDDOWN)[index]
    return rtn

def is_allowed_down(server, allowed_down):
    return get_allowed_down_index(server, allowed_down, 0)

def get_timeout(server, allowed_down):
    return get_allowed_down_index(server, allowed_down, 1)

def enstore_state(status):
    # given the status accumulated from all of the servers, determine the state of enstore
    if status == enstore_constants.UP:
	rtn = status
    elif status & enstore_constants.DOWN:
	rtn = enstore_constants.DOWN
    elif status & enstore_constants.WARNING:
	rtn = enstore_constants.WARNING
    else:
	rtn = enstore_constants.SEEN_DOWN
    return rtn

def get_allowed_down_dict():
    cdict = enstore_functions.get_config_dict()
    return cdict.configdict.get(SYSTEM, {}).get(ALLOWED_DOWN, {})

class EnstoreServer:

    def __init__(self, name, format_name, offline_d, seen_down_d, allowed_down_d,
		 en_status, cs=None, mailer=None):
	self.name = name
	self.format_name = format_name
	self.offline_d = offline_d
	self.seen_down_d = seen_down_d
	self.allowed_down = is_allowed_down(self.name, allowed_down_d)
	self.timeout = get_timeout(self.name, allowed_down_d)
	self.tries = TRIES
	self.status = enstore_constants.UP
        self.mail_file = None
	# if self.status is not UP, then enstore is the following
	self.en_status = en_status
	if cs:
	    self.csc = cs.csc
	    self.config_host = cs.config_host
	else:
	    self.csc = None

    def is_really_down(self):
	return self.seen_down_d[self.format_name] % self.allowed_down

    def writemail(self, message):
        # we only send mail if the server has been seen down more times than it is allowed
        # to be down in a row.
        if self.seen_down_d.has_key(self.format_name) and self.is_really_down():
            # see if this server is known to be down, if so, then do not send mail
            if not self.offline_d.has_key(self.format_name):
                subject = "Please check Enstore System (config node - %s)" % (self.config_host,)
                # first get a tempfile
                self.mail_file = tempfile.mktemp()
                os.system("date >> %s"%(self.mail_file,))
                os.system('echo "\t%s" >> %s' % (message, self.mail_file))

    def remove_mail(self):
        if self.mail_file:
            os.system("rm %s"%(self.mail_file,))
            

    def set_status(self, status):
	self.status = status
	if status == enstore_constants.DOWN:
	    if self.seen_down_d.has_key(self.format_name):
		self.seen_down_d[self.format_name] = self.seen_down_d[self.format_name] + 1
	    else:
		self.seen_down_d[self.format_name] = 1
	    if not self.is_really_down():
		self.status = enstore_constants.SEEN_DOWN
	elif status == enstore_constants.UP:
	    if self.seen_down_d.has_key(self.format_name):
		del self.seen_down_d[self.format_name]

    def is_alive(self):
	enprint("%s ok"%(self.format_name,))
	self.set_status(enstore_constants.UP)

    # the third parameter is used to determine the state of enstore if this server is 
    # considered down.  some servers being down will mark enstore as down, others will
    # not. 'rtn' records the state of the server.
    def check(self, ticket):
	if not 'status' in ticket.keys():
	    # error during alive
	    enprint("%s NOT RESPONDING"%(self.format_name,))
	    self.set_status(enstore_constants.DOWN)
	    self.writemail("%s is not alive. Down counter %s"%(self.format_name, 
                                                               self.seen_down_d[self.format_name]))
	elif ticket['status'][0] == e_errors.OK:
	    self.is_alive()
	else:
	    if ticket['status'][0] == e_errors.TIMEDOUT:
		enprint("%s NOT RESPONDING"%(self.format_name,))
		self.set_status(enstore_constants.DOWN)
                self.writemail("%s is not alive. Down counter %s"%(self.format_name, 
                                                                   self.seen_down_d[self.format_name]))
	    else:
		enprint("%s  BAD STATUS %s"%(self.format_name, ticket['status']))
		self.set_status(enstore_constants.DOWN)
		self.writemail("%s  BAD STATUS %s. Down counter %s"%(self.format_name,
                                                                     ticket['status'],
                                                                     self.seen_down_d[self.format_name]))

    def known_down(self):
	self.status = enstore_constants.DOWN
	enprint("%s known down"%(self.format_name,))

    def get_enstore_state(self, state):
	if self.status == enstore_constants.DOWN:
	    # en_status records the state of enstore when the server is done
	    return state | self.en_status
	elif self.status == enstore_constants.WARNING:
	    return state | enstore_constants.WARNING
	elif self.status == enstore_constants.SEEN_DOWN:
	    return state | enstore_constants.SEEN_DOWN
	else:
	    return state

class LogServer(EnstoreServer):

    def __init__(self, csc, offline_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, "log_server", enstore_constants.LOGS,
			       offline_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN, csc)
        self.event = None

    def check(self):
	if not self.offline_d.has_key(self.format_name):
	    self.lcc = log_client.LoggerClient(self.csc, "LOG_CLIENT", self.name)
	    EnstoreServer.check(self, self.lcc.alive(self.name, self.timeout, self.tries))
	else:
	    self.known_down()
	if self.event: self.event.set()

class AlarmServer(EnstoreServer):

    def __init__(self, csc, offline_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, "alarm_server", enstore_constants.ALARMS,
			       offline_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN, csc)
        self.event = None

    def check(self):
	if not self.offline_d.has_key(self.format_name):
	    self.acc = alarm_client.AlarmClient(self.csc, self.timeout, self.tries)
	    EnstoreServer.check(self, self.acc.alive(self.name, self.timeout, self.tries))
	else:
	    self.known_down()
	if self.event: self.event.set()

class ConfigServer(EnstoreServer):

    def __init__(self, offline_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, "configuration_server", 
			       enstore_constants.CONFIGS, offline_d,
			       seen_down_d, allowed_down_d,
			       enstore_constants.DOWN)
	self.config_port = string.atoi(os.environ.get('ENSTORE_CONFIG_PORT', 7500))
	self.config_host = os.environ.get('ENSTORE_CONFIG_HOST', "localhost")
	self.csc = configuration_client.ConfigurationClient((self.config_host, 
							     self.config_port))
        self.event = None
	enprint("Checking Enstore on %s with variable timeout and tries "%((self.config_host,
									    self.config_port),))

    def check(self):
	if not self.offline_d.has_key(self.format_name):
	    EnstoreServer.check(self, self.csc.alive(self.timeout, self.tries))
	else:
	    self.known_down()
	if self.event: self.event.set()

class FileClerk(EnstoreServer):

    def __init__(self, csc, offline_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, "file_clerk", enstore_constants.FILEC,
			       offline_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN, csc)
        self.event = None

    def check(self):
	if not self.offline_d.has_key(self.format_name):
	    self.fcc = file_clerk_client.FileClient(self.csc, 0)
	    EnstoreServer.check(self, self.fcc.alive(self.name, self.timeout, self.tries))
	else:
	    self.known_down()
	if self.event: self.event.set()

class Inquisitor(EnstoreServer):

    def __init__(self, csc, offline_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, "inquisitor", enstore_constants.INQ,
			       offline_d, seen_down_d, allowed_down_d,
			       enstore_constants.WARNING, csc)
        self.event = None

    def check(self):
	if not self.offline_d.has_key(self.format_name):
	    self.ic = inquisitor_client.Inquisitor(self.csc)
	    EnstoreServer.check(self, self.ic.alive(self.name, self.timeout, self.tries))
	else:
	    self.known_down()
	if self.event: self.event.set()

class VolumeClerk(EnstoreServer):

    def __init__(self, csc, offline_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, "volume_clerk", enstore_constants.VOLC,
			       offline_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN, csc)
        self.event = None

    def check(self):
	if not self.offline_d.has_key(self.format_name):
	    self.vcc = volume_clerk_client.VolumeClerkClient(self.csc)
	    EnstoreServer.check(self, self.vcc.alive(self.name, self.timeout, self.tries))
	else:
	    self.known_down()
	if self.event: self.event.set()

class LibraryManager(EnstoreServer):

    # states of a library manager meaning 'alive but not available for work'
    BADSTATUS = ['ignore', 'locked', 'pause', 'unknown']

    def __init__(self, csc, name, offline_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, name, name, offline_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN, csc)
	self.postfix = enstore_constants.LIBRARY_MANAGER
        self.event = None

    def is_alive(self):
	if self.lstate in self.BADSTATUS:
	    # the lm is not in a good state mark it as yellow
	    enprint("%s in a %s state"%(self.format_name, self.lstate))
	    self.set_status(enstore_constants.WARNING)
            if self.lstate != 'unknown':
                self.writemail("%s is in %s state. Down counter %s"%(self.format_name,
                                                                     self.lstate,
                                                                     self.seen_down_d[self.format_name]))
            else:
                self.writemail("%s is in %s state. "%(self.format_name,
                                                      self.lstate))
                
	else:
	    EnstoreServer.is_alive(self)

    # we need to get the state of the library manager.  if the lm is in a draining or an ignore
    # state, then mark it as yellow.  we get the lm
    # state by sending the lm a 'status' command, not an alive.  if the lm answers
    # we know it is alive and we have the status.  if it does not answer, it is not alive
    # and the state is DOWN.
    def check(self):
	if not self.offline_d.has_key(self.format_name):
	    self.lmc = library_manager_client.LibraryManagerClient(self.csc, self.format_name)
	    try:
		ticket = self.lmc.get_lm_state(self.timeout, self.tries)
	    except errno.errorcode[errno.ETIMEDOUT]:
		ticket = {'state': {}}
	    self.lstate = ticket.get('state', 'unknown')
	    EnstoreServer.check(self, ticket)
	else:
	    self.known_down()
	if self.event: self.event.set()

class MediaChanger(EnstoreServer):

    def __init__(self, csc, name, offline_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, name, name, offline_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN, csc)
	self.postfix = enstore_constants.MEDIA_CHANGER
        self.event = None

    def check(self):
	if not self.offline_d.has_key(self.format_name):
	    self.mcc = media_changer_client.MediaChangerClient(self.csc, self.format_name)
	    EnstoreServer.check(self, self.mcc.alive(self.name, self.timeout, self.tries))
	else:
	    self.known_down()
	if self.event: self.event.set()

class Mover(EnstoreServer):

    # states of a mover meaning 'alive but not available for work'
    BADSTATUS = {'ERROR' : enstore_constants.DOWN, 
		 'OFFLINE' : enstore_constants.WARNING,
		 'DRAINING' : enstore_constants.WARNING}

    def __init__(self, csc, name, offline_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, name, name, offline_d, seen_down_d, allowed_down_d,
			       enstore_constants.WARNING, csc)
	self.postfix = enstore_constants.MOVER
        self.event = None
        self.check_result = 0

    def is_alive(self):
	# now check to see if the mover is in a bad state
	keys = self.BADSTATUS.keys()
	if self.mstate in keys:
	    # the mover is not in a good state mark it as bad
	    enprint("%s in a %s state"%(self.format_name, self.mstate))
	    self.set_status(self.BADSTATUS[self.mstate])
            self.writemail("%s is in a %s state. Down Counter %s"%(self.format_name,
                                                                   self.mstate,
                                                                   self.seen_down_d[self.format_name]))
	else:
	    EnstoreServer.is_alive(self)

    # we need to get the state of the mover.  if the mover is in a draining or an offline
    # state, then it does not count towards the total available movers.  we get the mover
    # state by sending the mover a 'status' command, not an alive.  if the mover answers
    # we know it is alive and we have the status.  if it does not answer, it is not alive
    # and the state is DOWN. 'rtn' records if the mover is available for work.
    def check(self):
	if not self.offline_d.has_key(self.format_name):
	    self.mvc = mover_client.MoverClient(self.csc, self.format_name)
	    try:
		mstatus = (self.mvc.status(self.timeout, self.tries),)
	    except errno.errorcode[errno.ETIMEDOUT]:
		mstatus = ({},)
	    self.mstate = mstatus[0].get('state', {})
	    EnstoreServer.check(self, mstatus[0])
	else:
	    self.known_down()
	# we need to return 1 if this is a bad mover (bad, bad mover).
	if self.status == enstore_constants.UP:
	    self.check_result = 0
	else:
	    self.check_result = 1
	if self.event: self.event.set()

class UpDownInterface(generic_client.GenericClientInterface):
 
    def __init__(self, flag=1, opts=[]):
        self.do_parse = flag
        self.restricted_opts = opts
	self.summary = do_output
	self.html = 0
	generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.help_options() + ["summary", "html"]

def do_real_work():
    html_dir = enstore_functions.get_html_dir()
    # check if the html_dir is accessible
    sfile = None
    if os.path.exists(html_dir):
	sfile = enstore_files.ScheduleFile(html_dir, enstore_constants.OUTAGEFILE)
	outage_d, offline_d, seen_down_d = sfile.read()
    else:
	outage_d = {}
	offline_d = {}
	seen_down_d = {}

    summary_d = {enstore_constants.TIME: enstore_functions.format_time(time.time())}

    allowed_down_d = get_allowed_down_dict()

    # create all objects
    cs = ConfigServer(offline_d, seen_down_d, allowed_down_d)
    lcc = LogServer(cs, offline_d, seen_down_d, allowed_down_d)
    acc = AlarmServer(cs, offline_d, seen_down_d, allowed_down_d)
    ic = Inquisitor(cs, offline_d, seen_down_d, allowed_down_d)
    fcc = FileClerk(cs, offline_d, seen_down_d, allowed_down_d)
    vcc = VolumeClerk(cs, offline_d, seen_down_d, allowed_down_d)
    lib_man_d = cs.csc.get_library_managers({})
    library_managers = sortit(lib_man_d)

    meds = {}
    total_servers = [cs, lcc, acc, ic, fcc, vcc]

    libs = {}
    total_movers = []
    for lm in library_managers:
	lm_name = lib_man_d[lm]['name']
        lmc = LibraryManager(cs, lm_name, offline_d, seen_down_d, allowed_down_d)
        total_servers.append(lmc) 
        libs[lm_name] = {}
        libs[lm_name]['client'] = lmc
	#total_servers.append(lmc)

	# no duplicates in dict
	meds[cs.csc.get_media_changer(lm_name, lmc.timeout, lmc.tries)] = 1 
	movs = {}
	mov=cs.csc.get_movers(lm_name)
	for m in mov:
	    movs[(m['mover'])] = 1 # no duplicates in dictionary
	movers = sortit(movs)
        mover_objects = []
        mover_events = []
        for mov in movers:
            mvc = Mover(cs, mov, offline_d, seen_down_d, allowed_down_d)
            mvc.event = threading.Event()
            mover_objects.append(mvc)
            mover_events.append(mvc.event)
        libs[lm_name]['objects'] = mover_objects
        libs[lm_name]['events'] = mover_events
        libs[lm_name]['bad_movers'] = 0
        libs[lm_name]['total_movers'] = len(movers)
        total_movers = total_movers + mover_objects
            

    media_changers = sortit(meds)

    for med in media_changers:
	if med:
	    total_servers.append(MediaChanger(cs, med, offline_d, seen_down_d, allowed_down_d))


    # create events
    server_events = []
    for server in total_servers:
        server.event = threading.Event()
        server_events.append(server.event)

    total_servers = total_servers + total_movers
    # start check
    thread_count = 0
    for object in total_servers:
        thread_name = 'check %s'%(object.name,)
        thread = threading.Thread(group=None, target=object.check,
                                  name=thread_name, args=(), kwargs={})
        try:
            thread.start()
            thread_count = thread_count + 1
        except:
            exc, detail, tb = sys.exc_info()
            enprint ("Error starting thread %s: %s" % (thread_name, detail))


    # event loop
    # wait for events
    cnt = 0
    while cnt < thread_count:
        got_it = 0
        # check server events
        for event in server_events:
            event.wait(1.)
            if event.isSet():
                got_it = 1
                break
        if got_it:
            event.clear()
            server_events.remove(event)
            cnt = cnt + 1

        # chek mover events
        for lib in libs.keys():
            got_it = 0
            for event in libs[lib]['events']:
                event.wait(1.)
                if event.isSet():
                    got_it = 1
                    break
            if got_it:
                event.clear()
                libs[lib]['events'].remove(event)
                # find corresponding mover object
                for mov in libs[lib]['objects']:
                    if event == mov.event:
                        libs[lib]['bad_movers'] = libs[lib]['bad_movers'] + mov.check_result
                        break
                if mov in libs[lib]['objects']:
                    libs[lib]['objects'].remove(mov)
                cnt = cnt + 1

    for lib in libs.keys():
        if libs[lib]['bad_movers']*2 > libs[lib]['total_movers']:
	    enprint("LOW CAPACITY: Found, %s of %s not responding"%(libs[lib]['bad_movers'], libs[lib]['total_movers']))
            libs[lib]['client'].writemail("Found LOW CAPACITY movers for %s"%(lib,))
	    libs[lib]['client'].status = enstore_constants.WARNING
	    summary_d[lm_name] = enstore_constants.WARNING
	elif libs[lib]['bad_movers'] != 0:
	    enprint("Sufficient capacity of movers for %s, %s of %s responding"%(lib, 
									  libs[lib]['total_movers'],
                                                                          libs[lib]['total_movers']- libs[lib]['bad_movers']))
    # rewrite the schedule file as we keep track of how many times something has been down
    if sfile:
	sfile.write(outage_d, offline_d, seen_down_d)

    # now figure out the state of enstore based on the state of the servers
    estate = enstore_constants.UP
    for server in total_servers:
	estate = server.get_enstore_state(estate)
	summary_d[server.format_name] = server.status
    else:
	summary_d[enstore_constants.ENSTORE] = enstore_state(estate)

    if summary_d[enstore_constants.ENSTORE] == enstore_constants.DOWN:
	stat = "DOWN"
	rtn = 1
    else:
	stat = "UP"
	rtn = 0

    # send summary mail if needed
    need_to_send = 0
    summary_file = tempfile.mktemp()
    subject = "Please check Enstore System (config node - %s)" % (cs.config_host,)
    os.system("echo ' Message from enstore_up_down.py:\n\n\tPlease check the full Enstore software system.\n\n" + \
              "See the Status-at-a-Glance Web Page\n\n' > %s"%(summary_file,))
    for server in total_servers:
        if server.mail_file:
            need_to_send = 1
            os.system('cat "%s" >> "%s"' % (server.mail_file, summary_file))
            server.remove_mail()
    if need_to_send:
        os.system("/usr/bin/Mail -s \"%s\" $ENSTORE_MAIL < %s"%(subject, summary_file))
    os.system("rm %s"%(summary_file,))
    
    enprint("Finished checking Enstore... system is defined to be %s"%(stat,))
    return (rtn, summary_d)

def do_work(intf):
    global prefix, do_output

    # see if we are supposed to output well-formed html or not
    if intf.html:
	prefix = "<LI>"

    do_output = intf.summary

    rtn, summary_d = do_real_work()
    return (rtn)

if __name__ == "__main__" :

    # fill in interface
    intf = UpDownInterface()
 
    rtn = do_work(intf)
    sys.exit(rtn)
