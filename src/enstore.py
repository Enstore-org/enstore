#!/usr/bin/env python

###############################################################################
# src/$RCSfile$   $Revision$
#
# in order to add support for a new 'client', add a section to the following
# structures -
#             import the appropriate file
#             server_functions
###############################################################################

# system imports
#
import sys
import re
import os
import grp
import pwd
#import string
#import errno

# enstore imports
#import setpath

#import e_errors
import alarm_client
import configuration_client
#import configuration_server
import file_clerk_client
import inquisitor_client
import library_manager_client
import log_client
import event_relay_client
import media_changer_client
import migrator_client
import mover_client
import monitor_client
import option
import volume_clerk_client
import info_client
import enstore_up_down
import enstore_saag
import enstore_saag_network
import ratekeeper_client
import namespace
import enstore_start
import enstore_stop
import enstore_restart
import backup
import udp_proxy_client
import lm_director_client
import dispatcher_client

try:
	import quota
except:
	import fake_quota
	quota = fake_quota

try:
    import scan
except:
    pass

# define in 1 place all the hoary pieces of the command needed to access an
# entire enstore system.
# Yes, all those blasted backslashes are needed and I agree it is insane. We should
# loop on rsh and dump rgang


## Some of the backslash-itis is cured by using Python raw strings.
CMDa = "(F=~/\\\\\\`hostname\\\\\\`."
CMDb = ";echo >>\\\\\\$F 2>&1;date>>\\\\\\$F 2>&1;. /usr/local/etc/setups.sh>>\\\\\\$F 2>&1; setup enstore>>\\\\\\$F 2>&1;"
CMDc = ";echo >>\\\\\\$F 2>&1;date>>\\\\\\$F 2>&1;. /usr/local/etc/setups.sh>>\\\\\\$F 2>&1; setup enstore efb>>\\\\\\$F 2>&1;"
# the tee is not robust - need to add code to check if we can write to tty (that is connected to console server)
CMD2 = " 2>&1 |tee /dev/console>>\\\\\\$F 2>&1;date>>\\\\\\$F 2>&1) 1>&- 2>&- <&- &"

CMD1 = "%s%s%s"%(CMDa, "startup", CMDb)
#CMD1 = "%s%s%s"%(CMDa, "startup", CMDc)

#DEFAULT_AML2_NODE = "rip10"
ERROR = "ERROR"
HELP = "HELP"
HELP_OPTS = ["--help", "-h", "--hel", "--he"]

#The format of the dictionary server_functions{} is as follows:
# The keys of the dictionary are the command names that can be passed to the
# enstore command.  Most of these will correspond to a specific enstore
# server.  The value for each entry is a list with three fields.
# field 1) The instance of the interface class inherited from option.Interface.
# field 2) The "main" function that does the work.
# field 3) Either option.ADMIN or option.USER.  Used to hide admin commands
#          from general users.
server_functions = {
    "alarm" : [alarm_client.AlarmClientInterface,
               alarm_client.do_work, option.ADMIN],
    "configuration" : [configuration_client.ConfigurationClientInterface,
                       configuration_client.do_work, option.ADMIN],
    "event_relay" : [event_relay_client.EventRelayClientInterface,
                     event_relay_client.do_work, option.ADMIN],
    "file" : [file_clerk_client.FileClerkClientInterface,
              file_clerk_client.do_work, option.USER],
    "lmdirector" : [lm_director_client.LMDClientInterface,
		    lm_director_client.do_work, option.ADMIN],
    "dispatcher" : [dispatcher_client.DispatcherClientInterface,
		    dispatcher_client.do_work, option.ADMIN],
    "udp_proxy" : [udp_proxy_client.ProxyClientInterface,
		    udp_proxy_client.do_work, option.ADMIN],
    "inquisitor" : [inquisitor_client.InquisitorClientInterface,
                    inquisitor_client.do_work, option.ADMIN],
    "library" : [library_manager_client.LibraryManagerClientInterface,
                 library_manager_client.do_work, option.USER],
    "log" : [log_client.LoggerClientInterface,
             log_client.do_work, option.ADMIN],
    "media" : [media_changer_client.MediaChangerClientInterface,
               media_changer_client.do_work, option.ADMIN],
    "migrator" : [migrator_client.MigratorClientInterface,
		  migrator_client.do_work, option.ADMIN],
    "monitor" : [monitor_client.MonitorServerClientInterface,
                 monitor_client.do_work, option.USER],
    "mover" : [mover_client.MoverClientInterface,
               mover_client.do_work, option.ADMIN],
    "network" : [enstore_saag_network.SaagNetworkInterface,
                 enstore_saag_network.do_work, option.ADMIN],
    "pnfs" : [namespace.NamespaceInterface,
	     namespace.do_work, option.USER],
    "sfs" : [namespace.NamespaceInterface,
	     namespace.do_work, option.USER],
    "up_down" : [enstore_up_down.UpDownInterface,
                 enstore_up_down.do_work, option.ADMIN],
    "system" : [enstore_saag.SaagInterface,
                enstore_saag.do_work, option.ADMIN],
    "schedule" : [inquisitor_client.InquisitorClientInterface,
                  inquisitor_client.do_work, option.ADMIN],
    "volume" : [volume_clerk_client.VolumeClerkClientInterface,
                volume_clerk_client.do_work, option.USER],
    "quota" : [quota.Interface,
               quota.do_work, option.ADMIN],
    "info" : [info_client.InfoClientInterface,
              info_client.do_work, option.USER],
    "ratekeeper" : [ratekeeper_client.RatekeeperClientInterface,
                    ratekeeper_client.do_work, option.ADMIN],
    "start" : [enstore_start.EnstoreStartInterface,
               enstore_start.do_work, option.ADMIN],
    "stop" : [enstore_stop.EnstoreStopInterface,
              enstore_stop.do_work, option.ADMIN],
    "restart" : [enstore_restart.EnstoreRestartInterface,
                 enstore_restart.do_work, option.ADMIN],
    "backup" : [backup.BackupInterface,
                backup.do_work, option.ADMIN],
    }

try:
    server_functions['scan'] = [scan.ScanInterface,
                                scan.do_work, option.ADMIN]
except:
    pass

def get_farmlet(default):
    if len(sys.argv) > 1:
        return sys.argv[1]
    else:
        return default

def get_argv2(default=" "):
    if len(sys.argv) > 2:
        return sys.argv[2]
    else:
        return default

def get_argv3(default=" "):
    if len(sys.argv) > 3:
        return sys.argv[3]
    else:
        return default

#START_HELP = "start   [--just server --ping --asynch --nocheck]"
#STOP_HELP = "stop    [--just server --xterm server]"

local_scripts = {
    #"enstore-start":[START_HELP, ("enstore-start", sys.argv[2:])],
    #"start":[START_HELP, ("enstore-start", sys.argv[2:])],
    #"enstore-stop":[STOP_HELP, ("enstore-stop", sys.argv[2:])],
    #"stop":[STOP_HELP, ("enstore-stop", sys.argv[2:])],
    #"restart":["restart [--just server --xterm server]",
    #	    ("enstore-stop", sys.argv[2:]),
    #            ("enstore-start --nocheck", sys.argv[2:])],
    #"ping":["ping    [timeout-seconds]",
    #	 ("enstore-ping", sys.argv[2:])],
    #"qping":["qping   [timeout-seconds]",
    #	  ("quick-ping", sys.argv[2:])],
    #"backup":["backup        (backup file and volume databases)",
    #("python $ENSTORE_DIR/src/backup.py",sys.argv[2:])],
    #"aml2":["aml2            (lists current mount state & queue list on aml2 robot)",
    #	 ('enrsh %s "sh -c \'. /usr/local/etc/setups.sh;setup enstore;dasadmin listd2 | grep rip;dasadmin list rip1\'"',
    #         [],
    #         "self.node"),
    #        ],
    #"ps":["ps                (list enstore related processes)",
    #      ("EPS", sys.argv[2:])],
    }

# the sys.argv contains the whole thing
VERIFY = "verify"
PROMPT = "prompt"
remote_scripts = {
    #"Estart":["Estart  [farmlet]  (Enstore start on all/specified farmlet nodes)",
    #          ("enstore",
    #           ("%s enstore-start " % (CMD1,), get_argv3("enstore"), CMD2),
    #           VERIFY)],
    #"Estop":["Estop   [farmlet]  (Enstore stop on all/specified farmlet nodes)",
    #         ("enstore-down",
    #          ("%s enstore-stop " % (CMD1,),
    #           get_argv3("enstore-down"), CMD2),
    #          PROMPT, VERIFY), ],
    "Estart":["Estart  [farmlet]  (Enstore start on all/specified farmlet nodes)",
              ("enstore ",
               ("%s enstore start " % (CMD1,), get_argv3(""), CMD2),
               VERIFY)],
    "Estop":["Estop   [farmlet]  (Enstore stop on all/specified farmlet nodes)",
             ("enstore-down",
              ("%s enstore stop " % (CMD1,),
               get_argv3(""), CMD2),
              PROMPT, VERIFY), ],
    "EPS":["EPS  [farmlet]   (Enstore-associated ps on all/specified farmlet nodes)",
           ("enstore",
            ("source /usr/local/etc/setups.sh;setup enstore;", "EPS"))],
    "ls":["ls  [farmlet]   (ls of cwd on all/specified farmlet nodes)",
          ("enstore",
           ("ls %s" % (os.getcwd(),),))],
    }


# these general functions perform various system functions
def call_function(executable, argv):
    # pull out the arguments from argv and create a string that can be
    # passed to os.system.
    tmp_str = executable
    for arg in argv:
        tmp_str = "%s %s"%(tmp_str, arg)
    return os.system(tmp_str)>>8

def prompt_user(command="", node=""):
    sys.stdout.write("Please confirm: %s enstore on %s [y/n def:n] : "%(command,
									node))
    return sys.stdin.readline()

def no_argv_num(num):
    if len(sys.argv) < num:
	return 1
    else:
	return None

def no_argv2():
    return no_argv_num(2)

def no_argv3():
    return no_argv_num(3)

# this fuction is almost the same as in enstore_start
# except setting is done for real not effective ids
def check_user():
    #If in a cluster system...
    if enstore_start.is_in_cluster():
        #First determine if running as root, if so become enstore and restart.
        if os.geteuid() == 0:
            try:
                enstore_gid = grp.getgrnam("enstore")[2]
                #os.setegid(enstore_gid)
                os.setgid(enstore_gid)
            except (OSError, KeyError, IndexError):
                print "Should be running as group enstore, " \
                      "but the enstore group is not found."
                sys.exit(1)
            try:
                enstore_uid = pwd.getpwnam("enstore")[2]
                #os.seteuid(enstore_uid)
                os.setuid(enstore_uid)
            except (OSError, KeyError, IndexError):
                print "Should be running as user enstore, " \
                      "but the enstore user is not found."
                sys.exit(1)

        #Extract the user name.
	try:
            name = pwd.getpwuid(os.geteuid())[0]
	except (KeyError, IndexError):
	    name = ""
        #Check if running as user enstore.
        if name != "enstore":
            print "You should run this as user enstore."
            sys.exit(1)

def do_rgang_command(fdefault, command):
    if (command.find("enstore start") != -1 or
        command.find("enstore stop") != -1):
        check_user()

    farmlet = get_farmlet(fdefault)
    print 'rgang %s \"%s\"'%(farmlet, command)
    return os.system('rgang %s \"%s\"'%(farmlet, command))

class EnstoreInterface:

    # the __init__ method is below

    """
    # get a new server interface and store it to use later if needed
    def get_server_intf(self, skey, flag):
        functions = server_functions.get(skey, None)
        if functions:
            self.server_intf = functions[0](args=[],
					    user_mode=self.user_mode)
            return self.server_intf
        else:
            return None
    """

    def find_server_match(self, servers):
        total_matches = 0
        try:
            pattern = "^%s"%(sys.argv[1],)
            for server in servers:
	        #If we find an exact match, we are done.
	        if server == sys.argv[1]:
		    total_matches = 1
		    self.matched_server = server
		    break

		#Attempt to find any partial matches.
                match = re.match(pattern, server, re.I)
                if not match is None:
                    total_matches = total_matches + 1
                    self.matched_server = server

	    #Return the number of matches.  This will be one for an
	    # exact server name match.
            return total_matches
        except (TypeError, IndexError):
            total_matches = 0
            return total_matches

    def get_valid_servers(self):
	servers = server_functions.keys()
	servers.sort()
	if self.user_mode:
	    allowed_servers = []
	    for server in servers:
		if server_functions[server][2] == option.USER:
		    # users cannot talk to this server
		    allowed_servers.append(server)
	else:
	    allowed_servers = servers
        return allowed_servers

    def get_all_servers(self):
	if not self.user_mode:
	    scripts = local_scripts.keys() + remote_scripts.keys()
	    scripts.sort()
	else:
	    scripts = []
	return self.get_valid_servers() + scripts

    def print_valid_servers(self, txt=ERROR):
	self.error = 1
        servers = self.get_all_servers()
        print "\n%s: Allowed servers/commands are : "%(txt,)
        for server in servers:
            print "\t%s"%(server,)

    def match_server(self):
        # the user can enter the least amount of text that uniquely
        # identifies the desired server. (e.g. - i for inquisitor).  so get
        # the full server name here.
        all_servers = self.get_all_servers()
        total_matches = self.find_server_match(all_servers)
        if total_matches == 1:
            # remove the 'enstore' name from sys.argv
            del sys.argv[0]
	else:
            # we did not match anything or matched too many things
	    self.print_valid_servers()

    def got_help(self):
	if len(sys.argv) >= 2 and sys.argv[1] in HELP_OPTS:
	    self.print_valid_servers(HELP)
	    return 1
	else:
	    return None

    def __init__(self, user_mode):
        self.user_mode = user_mode
	self.error = None
	if not self.got_help():
	    self.match_server()
	else:
	    # not really an error but we only got a help.
	    self.error = 2


class Enstore:

    timeout = 2
    retry = 2
    mc_type = ("type", "AML2_MediaLoader")

    def __init__(self, intf):
        self.user_mode = intf.user_mode
        self.matched_server = intf.matched_server
        self.intf = intf
	#self.node = ""
    """
    # try to get the configuration information from the config server
    def get_config_from_server(self):
        rtn = 0
	port = enstore_functions2.default_port()
	host = enstore_functions2.default_host()
        if port and host:
	    csc = configuration_client.ConfigurationClient((host, port))
	    try:
		t = csc.get_dict_entry(self.mc_type, self.timeout,
				       self.retry)
		servers = t.get("servers", "")
		if servers:
		    # there may be more than one, we will use the first
		    t = csc.get(servers[0], self.timeout, self.retry)
		    # if there is no specified host, use the default
		    self.node = t.get("host", DEFAULT_AML2_NODE)
		else:
		    # there were no media changers of that type, use the
		    # default node
		    self.node = DEFAULT_AML2_NODE
		rtn = 1
	    except (socket.error, select.error, e_errors.EnstoreError), msg:
		pass
	    except errno.errorcode[errno.ETIMEDOUT]:
		pass
        return rtn

    # try to get the configuration information from the config file
    def get_config_from_file(self):
        rtn = 0
        # first look to see if $ENSTORE_CONFIG_FILE points to the config file
        cfile = os.environ.get("ENSTORE_CONFIG_FILE", "")
        if cfile:
            dict = configuration_server.ConfigurationDict()
            if dict.read_config(cfile) == (e_errors.OK, None):
                # get the list of media changers of the correct type
                slist = dict.get_dict_entry(self.mc_type)
                if slist:
                    # only use the first one
                    server_dict = dict.configdict[slist[0]]
                    # if there is no specified host use the default
                    self.node = server_dict.get("host", DEFAULT_AML2_NODE)
		else:
		    # there were no media changers of that type, use the
		    # default node
		    self.node = DEFAULT_AML2_NODE
		rtn = 1
        return rtn
    """
    # make sure the user wanted to start d0en nodes while on stken and vice versa
    def verify_node(self,node, command):
	if VERIFY in command:
	    if len(node)<=3:
		return
	    # 1st three letters return the "production" cluster, almost
	    gang = node[0:3]
	    # there are just 4 clusters we deal with right now... (code this better?)
	    clusters = ('stk','d0e','rip','cdf')
	    if gang in clusters:
		thisnode = os.uname()[1]
		thisgang = thisnode[0:3]
		if thisgang not in clusters:
		    # we are not on an enstore installed node
		    return 1
		# if we are trying to execute a command from a node in the same cluster, just do it
		if thisgang == gang:
		    return 1
		# rip9 and rip10 are special cases
                # NOT ANYMORE 3/26/03
		#if thisgang == 'stk':
		#    if node[0:4] == 'rip9' or node[0:5] == 'rip10':
		#	return 1
		# need to confirm if user really wanted to do this
		print "You want to execute a command on",node,"but you are running on",thisnode
		print "This doesn't seem right."
		answer = prompt_user("Is this want you want to do - execute ",node)
		if answer[0] == "y" or answer[0] == "Y":
		    return 1
		else:
		    print 'command canceled'
		    return 0
	    else:
		return 1
	else:
	    return 1

    def prompt(self, command, action):
        #command is a tuple
        # [0] is the default farmlet
        # [1] is the command to rgang
        # [2] arguments to assign dynamicaly
        answer = "y"
	if PROMPT in command:
	    if no_argv2():
		answer = prompt_user(command = action,
				     node = "all nodes")
	    elif no_argv3():
		answer = prompt_user(command = action,
			    node = "farmlet %s" % get_farmlet(""))
        return answer


    def got_help(self, help):
	for arg in sys.argv:
	    if arg in HELP_OPTS:
		# the user asked for help, give it to him and do nothing else
		print "\nenstore %s \n"%(help,)
		return 1
	else:
	    return None

    # this is where all the work gets done
    def do_work(self):
        arg1 = self.matched_server

        #execute local scripts
        # this node is used if the aml2 local script is being invoked
	# find the node to rsh to.  this node is the one associated with
        # the media changer of type "AML2_MediaLoader"
        #   if the configuration server is running, get the information
        #   from it.  if not, just read the config file pointed to by
        #   $ENSTORE_CONFIG_FILE.  if neither of these works, assume node
        #   in DEFAULT_AML2_NODE.
	# this info is used if the command is an aml2 command
        #if not self.get_config_from_server() and \
        #   not self.get_config_from_file():
        #    self.node = DEFAULT_AML2_NODE

	rtn = 0
        if arg1 in local_scripts.keys():
	    l_script = local_scripts[arg1]
	    if not self.got_help(l_script[0]):
		#l_script contains a list of tuples.  the first element is the help,
		# skip it.
		for command in l_script[1:]:
		    #each tuple in l_script is two items long.
                    print "local command",command[1]
                    if (command[1].find("enstore start") != -1 or
                        command[1].find("enstore stop") != -1):
                        check_user()
		    try:
			executable = command[0] % command[2:]
		    except (IndexError, TypeError):
			executable = command[0]
		    rtn = call_function(executable, command[1])

        #handles new interface style
        elif arg1 in server_functions.keys():
            intf = server_functions[arg1][0](args=sys.argv[:],
					     user_mode=self.user_mode)
            rtn = server_functions[arg1][1](intf)

        #execute remote scripts
        elif arg1 in remote_scripts.keys():
	    r_script = remote_scripts[arg1]
	    if not self.got_help(r_script[0]):
		#r_script contains a list of tuples.  the first element is the help,
		# skip it.
		for command in r_script[1:]:
		    if self.verify_node(get_farmlet(command[0]), command):
			#if command contains the string "prompt" then it
			# prompts the user for confirmation under some cases.
			# If no prompt is necessary returns "y".
			answer = self.prompt(command, arg1)
			if answer[0] == "y" or answer[0] == "Y":
			    executable = ""
			    for subcmd in command[1]:
				executable = "%s%s"%(executable, subcmd)
			    rtn = do_rgang_command(command[0], executable)

        # arg1 == "help" or arg1 == "--help" or arg1 == '':
        else:
            rtn = 0
            self.intf.print_help()

        sys.exit(rtn)
