#!/usr/bin/env python
###############################################################################
#
# $Id$
#
###############################################################################

import os
import sys
import string
import tempfile
import time
import select
import timeofday

import alarm_client
import generic_client
import inquisitor_client
import enstore_constants
import enstore_functions
import enstore_functions2
import enstore_files
import enstore_erc_functions
import event_relay_client
import event_relay_messages
import Trace
import e_errors
import option

DEFAULT = "default"
# default number of times in a row a server can be down before mail is sent
DEFAULTALLOWEDDOWN = [2, 15]
MOVERALLOWEDDOWN = [7, 120]
mail_sent = 0
prefix = ""
do_output = 0
no_mail = 0
fd = None
SYSTEM = 'system'
ALLOWED_DOWN = 'allowed_down'
TRIES = 1
NOUPDOWN = "noupdown"
WAIT_THIS_AMOUNT = 120
ALIVE_INTERVAL = 10
UNKNOWN = -1

LOW_CAPACITY = 0
SUFFICIENT_CAPACITY = 1

REMEDY_TYPE_D = { 'st' : "STK Enstore",
                  'd0' : "D0 Enstore",
                  'cd' : "CDF Enstore" }

def sortit(adict):
    keys = adict.keys()
    keys.sort()
    return keys

# print to a file  branded with the date and time
def enprint(text):
    global fd
    if do_output:
	print prefix, timeofday.tod(), text
        if not fd:
            filename = '/tmp/ENSTORE_UP_DOWN-%s'%(time.time(),)
            fd = open(filename, 'w')
	if fd:
	    fd.write("%s %s %s\n"%(prefix, timeofday.tod(), text))

def too_long(start):
    now = time.time()
    if now - start > WAIT_THIS_AMOUNT:
	# we have waited long enough
	rtn = 1
    else:
	rtn = 0
    return rtn

def get_media_changer(cdict, config_d, config_d_keys, lm):
    mc = ''
    movers = cdict.get_movers_internal({'library' : lm})
    for m in movers:
	mc = config_d[m['mover']].get(enstore_constants.MEDIA_CHANGER, '')
	if mc:
	    break
    return mc

def get_library_managers(config_d_keys):
    lms = []
    for key in config_d_keys:
	if enstore_functions2.is_library_manager(key):
	    lms.append(key)
    return lms

def get_udp_proxy_servers(config_d_keys):
    ups = []
    for key in config_d_keys:
	if enstore_functions2.is_udp_proxy_server(key):
	    ups.append(key)
    return ups

def get_migrators(config_d_keys):
    ups = []
    for key in config_d_keys:
	if enstore_functions2.is_migrator(key):
	    ups.append(key)
    return ups

def get_allowed_down_index(server, allowed_down, index):
    if allowed_down.has_key(server):
	rtn = allowed_down[server][index]
    elif enstore_functions2.is_mover(server):
	rtn = allowed_down.get(enstore_constants.MOVER,
                               MOVERALLOWEDDOWN)[index]
    elif enstore_functions2.is_library_manager(server):
	rtn = allowed_down.get(enstore_constants.LIBRARY_MANAGER,
                               DEFAULTALLOWEDDOWN)[index]
    elif enstore_functions2.is_media_changer(server):
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
    elif status and (status & enstore_constants.DOWN):
        # if status is None or empty then expression:
        # status & enstore_constants.DOWN
        # causes TypeError exception
	rtn = enstore_constants.DOWN
    elif status and (status & enstore_constants.WARNING):
	rtn = enstore_constants.WARNING
    else:
	rtn = enstore_constants.SEEN_DOWN
    return rtn

def get_allowed_down_dict():
    cdict = enstore_functions.get_config_dict()
    return cdict.configdict.get(SYSTEM, {}).get(ALLOWED_DOWN, {})

class EnstoreServer:

    def real_status(self, status):
	if self.override:
	    self.status = enstore_functions2.override_to_status(self.override)
	else:
	    self.status = status

    def __init__(self, name, format_name, offline_d, override_d, seen_down_d, allowed_down_d,
		 en_status, mailer=None):
	self.name = name
	self.format_name = format_name
	self.offline_d = offline_d
	self.seen_down_d = seen_down_d
	self.override = override_d.get(format_name, None)
	self.allowed_down = is_allowed_down(self.name, allowed_down_d)
	self.timeout = get_timeout(self.name, allowed_down_d)
	self.tries = TRIES
	self.real_status(enstore_constants.UP)
        self.mail_file = None
	self.in_bad_state = 0
        self.reason_down = ""
        self.movers = None
	# if self.status is not UP, then enstore is the following
	self.en_status = en_status
	# we need to see if this server should be monitored by up_down.  this 
	# info is in the config file.
	flag = enstore_functions.get_from_config_file(name, NOUPDOWN, None)
	if flag:
	    self.noupdown = True
	else:
	    self.noupdown = False

    def is_really_down(self):
        rc = 0
        if self.seen_down_d.get(self.format_name, 0) > self.allowed_down:
            rc = 1
        return rc

    def need_to_send_mail(self):
        rc = 0
        if (self.seen_down_d.get(self.format_name, 0) % self.allowed_down) == 0:
            rc = 1
        return rc

    def writemail(self, message):
        # we only send mail if the server has been seen down more times than it is allowed
        # to be down in a row.
        if self.seen_down_d.has_key(self.format_name) and self.need_to_send_mail():
            # see if this server is known to be down, if so, then do not send mail
            if not self.offline_d.has_key(self.format_name):
                # first get a tempfile
                self.mail_file = tempfile.mktemp()
                os.system("date >> %s"%(self.mail_file,))
                os.system('echo "\t%s" >> %s' % (message, self.mail_file))

    def remove_mail(self):
        if self.mail_file:
            os.system("rm %s"%(self.mail_file,))
            
    def set_status(self, status):
	# do not set the status if we have an override value which will be the status
	self.real_status(status)
	if status == enstore_constants.DOWN:
	    self.seen_down_d[self.format_name] = self.seen_down_d.get(self.format_name, 0) + 1
	    if not self.in_bad_state and not self.is_really_down():
		self.status = enstore_constants.SEEN_DOWN
	elif status == enstore_constants.WARNING:
	    self.seen_down_d[self.format_name] = self.seen_down_d.get(self.format_name, 0) + 1
	elif status == enstore_constants.UP:
	    if self.seen_down_d.has_key(self.format_name):
		del self.seen_down_d[self.format_name]

    def is_alive(self):
	enprint("%s ok"%(self.format_name,))
	self.set_status(enstore_constants.UP)

    def is_dead(self):
	enprint("%s NOT RESPONDING"%(self.format_name,))
	self.writemail("%s is not alive. Down counter %s"%(self.format_name, 
							   self.seen_down_d.get(self.format_name, 0)))
	self.set_status(enstore_constants.DOWN)

    def known_down(self):
	self.real_status(enstore_constants.DOWN)
	if self.status == enstore_constants.DOWN:
	    enprint("%s known down"%(self.format_name,))

    def set_reason(self, reason):
	if self.en_status & enstore_constants.DOWN:
	    # this status means enstore is down, record a reason
	    if self.reason_down:
		reason.append(self.reason_down)

    def get_enstore_state(self, state, reason):
	if not self.offline_d.has_key(self.format_name):
	    if self.status == enstore_constants.DOWN:
		# en_status records the state of enstore when the server is done
		self.set_reason(reason)
		return state | self.en_status
	    elif self.status == enstore_constants.WARNING:
		return state | enstore_constants.WARNING
	    elif self.status == enstore_constants.SEEN_DOWN:
		return state | enstore_constants.SEEN_DOWN
	    else:
		return state
	else:
	    # this server is known down, so for the sake of enstore, we don't care
	    # about its real state, say that it is up
	    return state

    # the third parameter is used to determine the state of enstore if this server is 
    # considered down.  some servers being down will mark enstore as down, others will
    # not. 'rtn' records the state of the server.
    def check(self, ticket):
	if not 'status' in ticket.keys():
	    # error during alive
	    self.is_dead()
	elif ticket['status'][0] == e_errors.OK:
	    self.is_alive()
	else:
	    if ticket['status'][0] == e_errors.TIMEDOUT:
		self.is_dead()
	    else:
		enprint("%s  BAD STATUS %s"%(self.format_name, ticket['status']))
		self.set_status(enstore_constants.DOWN)
		self.writemail("%s  BAD STATUS %s. Down counter %s"%(self.format_name,
								     ticket['status'],
							   self.seen_down_d.get(self.format_name, 0)))

    def handle_general_exception(self):
        exc, msg = sys.exc_info()[:2]
	EnstoreServer.check(self, {'status': (str(exc), str(msg))})
	raise exc, msg

    def get_real_status(self):
        # return whether the server is up or down or unknown.  it is unknown if we
        # are not checking it or it is overridden to anything
        if self.override:
            # it is overridden
            rtn = UNKNOWN
        elif self.noupdown:
            # we are not monitoring this one
            rtn = UNKNOWN
        else:
            rtn = self.status
        return rtn


class LogServer(EnstoreServer):

    def __init__(self, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, enstore_constants.LOG_SERVER,
                               enstore_constants.LOGS,
			       offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN)
	self.reason_down = "log_server down"

class AlarmServer(EnstoreServer):

    def __init__(self, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, enstore_constants.ALARM_SERVER,
                               enstore_constants.ALARMS,
			       offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN)
	self.reason_down = "alarm_server down"

class ConfigServer(EnstoreServer):

    def __init__(self, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, enstore_constants.CONFIGURATION_SERVER, 
			       enstore_constants.CONFIGS, offline_d,
			       override_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN)
	self.reason_down = "config_server down"
	self.config_port = string.atoi(os.environ.get('ENSTORE_CONFIG_PORT', 7500))
	self.config_host = os.environ.get('ENSTORE_CONFIG_HOST', "localhost")
	enprint("Checking Enstore on %s with variable timeout and tries "%((self.config_host,
									    self.config_port),))

class FileClerk(EnstoreServer):

    def __init__(self, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, enstore_constants.FILE_CLERK,
                               enstore_constants.FILEC,
			       offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN)
	self.reason_down = "file_clerk down"

class Inquisitor(EnstoreServer):

    def __init__(self, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, enstore_constants.INQUISITOR,
                               enstore_constants.INQ,
			       offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.WARNING)
	self.reason_down = None

class AccountingServer(EnstoreServer):

    def __init__(self, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, enstore_constants.ACCOUNTING_SERVER,
                               enstore_constants.ACCS,
			       offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.WARNING)
	self.reason_down = None

class DrivestatServer(EnstoreServer):

    def __init__(self, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, enstore_constants.DRIVESTAT_SERVER,
                               enstore_constants.DRVS,
			       offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.WARNING)
	self.reason_down = None

class InfoServer(EnstoreServer):

    def __init__(self, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, enstore_constants.INFO_SERVER,
                               enstore_constants.INFO,
			       offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.WARNING)
	self.reason_down = None

class VolumeClerk(EnstoreServer):

    def __init__(self, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, enstore_constants.VOLUME_CLERK,
                               enstore_constants.VOLC,
			       offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN)
	self.reason_down = "volume_clerk down"

class LMD(EnstoreServer): # library manager director

    def __init__(self, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, enstore_constants.LM_DIRECTOR,
                               enstore_constants.LMD,
			       offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN)
	self.reason_down = "lm_director down"

class Dispatcher(EnstoreServer): # dispatcher

    def __init__(self, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, enstore_constants.DISPATCHER,
                               enstore_constants.DISPR,
			       offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN)
	self.reason_down = "dispatcher down"

class LibraryManager(EnstoreServer):

    # states of a library manager meaning 'alive but not available for work'
    BADSTATUS = ['ignore', 'locked', 'pause', 'unknown']
    BROKENSTATUS = [e_errors.BROKEN]
    DEFAULT_MOVER_DOWN_PERCENTAGE = 50

    def __init__(self, name, config, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, name, name, offline_d, override_d, seen_down_d, 
			       allowed_down_d, enstore_constants.DOWN)
	self.reason_down = "%s down"%(name,)
	self.postfix = enstore_constants.LIBRARY_MANAGER
	self.server_state = ""
	self.in_bad_state = 0
	self.mover_down_percentage = config.get(enstore_constants.MOVER_DOWN_PERCENTAGE,
						self.DEFAULT_MOVER_DOWN_PERCENTAGE)

    # return the number of movers we know about that have a good status, and those with a bad
    # status
    def mover_status(self):
	ok_movers = 0
	bad_movers = 0
	for mover in self.movers:
	    # ignore this mover if it is marked known down
	    if not self.offline_d.has_key(mover.format_name):
		if mover.status == enstore_constants.UP or \
		   mover.status == enstore_constants.SEEN_DOWN:
		    ok_movers = ok_movers + 1
		else:
		    bad_movers = bad_movers + 1
	total_movers = bad_movers + ok_movers
	if total_movers > 0 and \
	   float(bad_movers)/float(total_movers) * 100 > self.mover_down_percentage:
	    return LOW_CAPACITY, bad_movers, ok_movers
	else:
	    return SUFFICIENT_CAPACITY, bad_movers, ok_movers

    def is_alive(self):
	# now that we know this lm is alive we need to examine its state
	if self.server_state in self.BADSTATUS:
	    self.in_bad_state = 1
	    # the lm is not in a good state mark it as yellow
	    enprint("%s in a %s state"%(self.format_name, self.server_state))
	    self.set_status(enstore_constants.WARNING)
            if self.server_state == 'unknown':
                self.writemail("%s is in %s state. Down counter %s"%(self.format_name,
                                                  self.server_state,
                                                  self.seen_down_d.get(self.format_name, 0)))
	elif self.server_state in self.BROKENSTATUS:
	    self.in_bad_state = 1
	    # the lm is broken, mark it as red
	    enprint("%s in a %s state"%(self.format_name, self.server_state))
	    self.set_status(enstore_constants.DOWN)
	else:
	    self.in_bad_state = 0
	    EnstoreServer.is_alive(self)

    def get_enstore_state(self, state, reason):
	if not self.offline_d.has_key(self.format_name):
            # check mover status only in library manager is not scheduled down
            if self.mover_status()[0] == LOW_CAPACITY:
                reason.append("Insufficient Movers for %s"%(self.name,))
                return state | enstore_constants.DOWN
            else:
                return EnstoreServer.get_enstore_state(self, state, reason)
        else:
	    # this server is known down, so for the sake of enstore, we don't care
	    # about its real state, say that it is up
            return state

class MediaChanger(EnstoreServer):

    def __init__(self, name, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, name, name, offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN)
	self.reason_down = "%s down"%(name,)
	self.postfix = enstore_constants.MEDIA_CHANGER

class Mover(EnstoreServer):

    # states of a mover meaning 'alive but not available for work'
    BADSTATUS = {'ERROR' : enstore_constants.DOWN, 
		 'OFFLINE' : enstore_constants.WARNING,
		 'DRAINING' : enstore_constants.WARNING}

    def __init__(self, name, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, name, name, offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.WARNING)
	self.reason_down = None
	self.postfix = enstore_constants.MOVER
	self.server_state = ""
        self.check_result = 0
	self.in_bad_state = 0

    def is_alive(self):
	# check to see if the mover is in a bad state
	keys = self.BADSTATUS.keys()
	if self.server_state in keys:
	    self.in_bad_state = 1
	    # the mover is not in a good state mark it as bad
	    enprint("%s in a %s state"%(self.format_name, self.server_state))
	    self.set_status(self.BADSTATUS[self.server_state])
            self.writemail("%s is in a %s state. Down Counter %s"%(self.format_name,
                                                                   self.server_state,
                                                           self.seen_down_d.get(self.format_name, 0)))
	else:
	    EnstoreServer.is_alive(self)
	    self.in_bad_state = 0

class UDPProxyServer(EnstoreServer):

    def __init__(self, name, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, name, name, offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN)
	self.reason_down = "%s down"%(name,)
	self.postfix = enstore_constants.UDP_PROXY_SERVER

class Migrator(EnstoreServer):

    def __init__(self, name, offline_d, override_d, seen_down_d, allowed_down_d):
	EnstoreServer.__init__(self, name, name, offline_d, override_d, seen_down_d, allowed_down_d,
			       enstore_constants.DOWN)
	self.reason_down = "%s down"%(name,)
	self.postfix = enstore_constants.MIGRATOR




class UpDownInterface(generic_client.GenericClientInterface):
 
    def __init__(self, args=sys.argv, user_mode=1):
        #self.do_parse = flag
        #self.restricted_opts = opts
	self.summary = do_output
	self.no_mail = 0
	self.make_html = 0
	generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.updown_options)

    updown_options = {
        option.MAKE_HTML:{option.HELP_STRING:"format output as html",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN,
                              },
        option.NO_MAIL:{option.HELP_STRING:
                        "do net send e-mail in case of errors",
                        option.DEFAULT_TYPE:option.INTEGER,
                        option.DEFAULT_VALUE:option.DEFAULT,
                        option.VALUE_USAGE:option.IGNORED,
                        option.USER_LEVEL:option.ADMIN,
                              },
        option.SUMMARY:{option.HELP_STRING:"print (stdout) server states",
                        option.DEFAULT_TYPE:option.INTEGER,
                        option.DEFAULT_VALUE:option.DEFAULT,
                        option.VALUE_USAGE:option.IGNORED,
                        option.USER_LEVEL:option.ADMIN,
                        },
        }


def no_override(server, okeys):
    if server.format_name in okeys:
	return 0
    else:
	return 1

def do_real_work():
    sfile, outage_d, offline_d, override_d = enstore_files.read_schedule_file()
    dfile, seen_down_d = enstore_files.read_seen_down_file()

    summary_d = {enstore_constants.TIME: enstore_functions2.format_time(time.time())}

    allowed_down_d = get_allowed_down_dict()
    override_d_keys = override_d.keys()

    cdict = enstore_functions.get_config_dict()
    config_d = cdict.configdict
    config_d_keys = config_d.keys()

    # create all objects
    ###################################################
    #  Mandatory enstore servers
    ###################################################

    cs = ConfigServer(offline_d, override_d, seen_down_d, allowed_down_d)
    log = LogServer(offline_d, override_d, seen_down_d, allowed_down_d)
    alarm = AlarmServer(offline_d, override_d, seen_down_d, allowed_down_d)
    inquisitor = Inquisitor(offline_d, override_d, seen_down_d, allowed_down_d)
    server_list = [cs, log, alarm, inquisitor,	    
                   FileClerk(offline_d, override_d, seen_down_d, allowed_down_d),
                   VolumeClerk(offline_d, override_d, seen_down_d, allowed_down_d),
                   AccountingServer(offline_d, override_d, seen_down_d, allowed_down_d),
                   InfoServer(offline_d, override_d, seen_down_d, allowed_down_d),
                   DrivestatServer(offline_d, override_d, seen_down_d, allowed_down_d),
                   ]
    ###################################################
    
    ###################################################
    #  Optional SFA Servers
    ###################################################

    lm_director = config_d.get("lm_director", None)
    if lm_director:
       server_list.append(LMD(offline_d, override_d, seen_down_d, allowed_down_d))
    dispatcher = config_d.get("dispatcher", None)
    if dispatcher:
       server_list.append(Dispatcher(offline_d, override_d, seen_down_d, allowed_down_d))
    ###################################################
    
    library_managers = get_library_managers(config_d_keys)
    upd_proxy_servers = get_udp_proxy_servers(config_d_keys)
    migrators = get_migrators(config_d_keys)
    meds = {}
    total_other_servers = []
    total_servers_names = []
    # do not look for servers that have the noupdown keyword in the config file
    for server in server_list:
        if server.noupdown == False:
            if no_override(server, override_d_keys):
                total_servers_names.append(server.name)
            total_other_servers.append(server)
    total_lms = []
    total_movers = []
    for lm in library_managers:
        lmc = LibraryManager(lm, config_d[lm], offline_d, override_d, seen_down_d, 
			     allowed_down_d)
	if lmc.noupdown == False:
	    total_lms.append(lmc) 
	    if no_override(lmc, override_d_keys):
		total_servers_names.append(lmc.name)

	# no duplicates in dict
	meds[get_media_changer(cdict, config_d, config_d_keys, lm)] = 1 
	movs = {}
	mov=cdict.get_movers_internal({'library' : lm})
	for m in mov:
	    movs[(m['mover'])] = 1 # no duplicates in dictionary
	movers = sortit(movs)
        mover_objects = []
        for mov in movers:
            mvc = Mover(mov, offline_d, override_d, seen_down_d, allowed_down_d)
	    if mvc.noupdown == False:
		mover_objects.append(mvc)
		if no_override(mvc, override_d_keys):
		    total_servers_names.append(mvc.name)
        lmc.movers = mover_objects
	lmc.num_movers = len(mover_objects)
        total_movers = total_movers + mover_objects
    media_changers = sortit(meds)
    for med in media_changers:
	if med:
	    mc = MediaChanger(med, offline_d, override_d, seen_down_d, allowed_down_d)
	    if mc.noupdown == False:
		total_other_servers.append(mc)
		# do not monitor the server if it has an override value
		if no_override(mc, override_d_keys):
		    total_servers_names.append(mc.name)

    for udp_px_s in upd_proxy_servers:
        upc = UDPProxyServer(udp_px_s, offline_d, override_d, seen_down_d, 
			     allowed_down_d)
	if upc.noupdown == False:
	    total_other_servers.append(upc) 
	    if no_override(upc, override_d_keys):
		total_servers_names.append(upc.name)
        
    for migrator in migrators:
        mgc = Migrator(migrator, offline_d, override_d, seen_down_d, 
                       allowed_down_d)
	if mgc.noupdown == False:
	    total_other_servers.append(mgc) 
	    if no_override(mgc, override_d_keys):
		total_servers_names.append(mgc.name)
        

    total_servers = total_other_servers + total_movers + total_lms

    # we will get all of the info from the event relay.
    erc = event_relay_client.EventRelayClient()
    erc.start([event_relay_messages.ALIVE,])
    # event loop - wait for events
    start = time.time()
    did_not_append = 1
    got_one = 0          # used to measure if the event relay is up
    while 1:
	readable, junk, junk = select.select([erc.sock], [], [], 15)
	if not readable:
	    # timeout occurred - we will only wait a certain amount of
	    # time before giving up on listening for alive messages
	    if too_long(start):
		break
	    else:
		# if we have not received any messages from the eent relay, try subscribing 
		# again.  maybe it is up now.
		if not got_one:
		    erc.subscribe()
		erc.send_one_heartbeat(enstore_constants.UP_DOWN)
		# send our heartbeat to the event relay process
		if did_not_append:
		    total_servers_names.append(enstore_constants.UP_DOWN)
		    did_not_append = 0
		continue
	msg = enstore_erc_functions.read_erc(erc)
	if msg and msg.server in total_servers_names:
	    total_servers_names.remove(msg.server)
	    got_one = 1
	    if enstore_functions2.is_mover(msg.server):
		# we also got it's state in the alive msg, save it
		for mv in total_movers:
		    if msg.server == mv.name:
			mv.server_state = msg.opt_string
	    elif enstore_functions2.is_library_manager(msg.server):
		# we also got it's state in the alive msg, save it
		for lm in total_lms:
		    if msg.server == lm.name:
			lm.server_state = msg.opt_string
	    if len(total_servers_names) == 0:
		# we have got em all
		break
	else:
	    # don't wait forever
	    if too_long(start):
		break
	    else:
		continue

    # close the socket
    erc.unsubscribe()
    erc.sock.close()

    # now, see what we have got
    for server in total_other_servers + total_movers:
	if not server.name in total_servers_names:
	    server.is_alive()
	else:
	    # server did not get back to us, assume it is dead
	    server.is_dead()

    # warnings need to be generated if more than 50% of a library_managers movers are down.
    for server in total_lms:
	if not server.name in total_servers_names:
	    server.is_alive()
	    state, bad_movers, ok_movers = server.mover_status()
	    if state == LOW_CAPACITY:
		enprint("LOW CAPACITY: Found, %s of %s movers not responding or in a bad state"%(bad_movers, 
									server.num_movers))
		server.writemail("Found LOW CAPACITY movers for %s"%(server.name,))
		server.real_status(enstore_constants.DOWN)
	    elif bad_movers != 0:
		enprint("Sufficient capacity of movers for %s, %s of %s responding"%(server.name,
										     ok_movers,
									           server.num_movers))
		server.real_status(enstore_constants.WARNING)
	    elif bad_movers == 0 and ok_movers == 0 and len(server.movers) > 0:
		# there are no movers, all are known down, flag a warning.
		server.real_status(enstore_constants.WARNING)

	else:
	    # server did not get back to us, assume it is dead
	    server.is_dead()

    # for any server/mover for which no heartbeat message was received, ask the
    # inquisitor when the last heartbeat was recived.  if it was in the last 5
    # minutes, mark that server/mover as up.  it may be too busy now to answer
    # us and we cannot wait for it.  only do this if the config server and
    # inquisitor are up.
    servers_to_mark_up = []
    if cs.get_real_status() == enstore_constants.UP and \
       inquisitor.get_real_status() == enstore_constants.UP:
        inq = inquisitor_client.Inquisitor((cs.config_host, cs.config_port))
        # if the name is in the total_servers_names list, then no heartbeat was received
        if len(total_servers_names) > 0:
            enprint("Looking for heartbeat info from inq for %s"%(total_servers_names,))
            rtn_ticket = inq.get_last_alive(total_servers_names)
            enprint("Got heartbeat info from inq for %s"%(rtn_ticket['servers']))
            # inq will return a hash of servers and their alive times. if a server
            # is not present in the list, then there was no recorded alive time
            servers_l = rtn_ticket['servers'].keys()
            now_seconds = time.time()
            seconds_in_5mins = 5 * 60
            for server in servers_l:
                if now_seconds - rtn_ticket['servers'][server] < seconds_in_5mins:
                    # server was alive within the last 5 minutes.  mark it as needing
                    # to be marked up.  we will mark it up in a few lines when we
                    # walk through each server
                    servers_to_mark_up.append(server)

    # keep tabs on the event relay too, if we received anyones alive, then the event relay
    # is alive.  otherwise mark it as dead
    if got_one:
	summary_d[enstore_constants.EV_RLY] = enstore_constants.UP
    else:
	summary_d[enstore_constants.EV_RLY] = enstore_constants.DOWN

    # now figure out the state of enstore based on the state of the servers
    estate = enstore_constants.UP
    reason = []
    for server in total_servers:
        # check if we need to mark this server up based on the inq receiving a
        # recent heartbeat
        if server.name in servers_to_mark_up:
            server.is_alive()
        estate = server.get_enstore_state(estate, reason)
	summary_d[server.format_name] = server.status
    else:
	summary_d[enstore_constants.ENSTORE] = enstore_state(estate)
    # now check if there is an override set to make sure that enstore is marked down
    if enstore_constants.ENSTORE in override_d_keys:
	summary_d[enstore_constants.ENSTORE] = enstore_functions2.override_to_status(\
	    override_d[enstore_constants.ENSTORE])
	if summary_d[enstore_constants.ENSTORE] == enstore_constants.DOWN:
	    # it was overridden
	    reason.append("overriden to %s"%(override_d[enstore_constants.ENSTORE]))

    if summary_d[enstore_constants.ENSTORE] == enstore_constants.DOWN:
	stat = "DOWN"
	rtn = 1
        # see if this enstore is known to be down, if so, then do not send mail or
        # generate a ticket
        if not offline_d.has_key(enstore_constants.ENSTORE):
            # only raise an alarm if we got a message from the alarm server and the log_server
            # because the alarm will be logged
            if summary_d.get(log.format_name, "") == enstore_constants.UP and \
                   summary_d.get(alarm.format_name, "") == enstore_constants.UP and \
                   summary_d.get(cs.format_name, "") == enstore_constants.UP:
                # determine remedy type based on config node
                remedy_type = REMEDY_TYPE_D.get(cs.config_host[0:2], cs.config_host)
                # the following line will set the alarm function
                alc = alarm_client.AlarmClient((cs.config_host, cs.config_port))
                Trace.init("Enstore_Up_Down")
                Trace.alarm(e_errors.INFO, e_errors.ENSTOREBALLRED, {'Reason':repr(reason)}) 
                print "%s - Ticket Generated %s, %s, %s"%(e_errors.ENSTOREBALLRED,
                                                          {'Reason':repr(reason)}, "RedBall",
                                                          remedy_type)
                Trace.alarm(e_errors.INFO, "%s - Ticket Generated"%(e_errors.ENSTOREBALLRED,),
                            {'Reason':repr(reason)}, "RedBall", remedy_type)
            else:
                # we could not generate a ticket in the normal way because the alarm/log/config
                # server is down.  fake it so the error is not missed.
                host = os.uname()[1]
                remedy_type = REMEDY_TYPE_D.get(host[0:2], "??")
                import alarm
                anAlarm = alarm.Alarm(host, e_errors.sevdict[e_errors.ERROR],
                                      e_errors.ENSTOREBALLRED, 42, "UP_DOWN", "UP_DOWN",
                                      "RedBall", remedy_type, {"Reason":reason})
                anAlarm.ticket()
                enprint("%s - LOG, ALARM, or CONFIG server not running, a ticket will NOT be generated!!!"%(e_errors.ENSTOREBALLRED,))
        else:
            enprint("%s - Enstore is marked down, a ticket will NOT be generated!!!"%(e_errors.ENSTOREBALLRED,))
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
    if (not no_mail) and need_to_send:
        if not offline_d.has_key(enstore_constants.ENSTORE):
            os.system("/usr/bin/Mail -s \"%s\" $ENSTORE_MAIL < %s"%(subject, summary_file))
        else:
            enprint("Enstore is marked down, no mail will be sent")
    os.system("rm %s"%(summary_file,))
    
    # rewrite the seen down file as we keep track of how many times something has 
    # been down
    servers = summary_d.keys()
    if dfile:
	# get rid of any servers no longer being monitored
	for srvr in seen_down_d.keys():
	    for server in servers:
		#if enstore_constants.SERVER_NAMES.has_key(server):
		    # get the real name of the server
		    #server = enstore_constants.SERVER_NAMES[server]
		if srvr == server:
		    # this is a legitimate server in seen_down_d
		    break
	    else:
		# we did not find a match for the server listed in seen_down_d with
		# the servers that we know about.  assume this is an old one and
		# remove it from the the seen_down_d
		del seen_down_d[srvr]
        # write it with updated seen_down_d
	dfile.write(seen_down_d)

    enprint("Finished checking Enstore... system is defined to be %s"%(stat,))
    return (rtn, summary_d)

def do_work(intf):
    global prefix, do_output, no_mail

    # see if we are supposed to output well-formed html or not
    if intf.make_html:
	prefix = "<LI>"

    do_output = intf.summary
    no_mail = intf.no_mail

    rtn, summary_d = do_real_work()
    return (rtn)

if __name__ == "__main__":

    # fill in interface
    intf = UpDownInterface(user_mode=0)
 
    rtn = do_work(intf)
    sys.exit(rtn)
