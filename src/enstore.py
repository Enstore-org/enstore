###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
#
import sys
import re
import os
import string
import errno

# enstore imports
import e_errors
import alarm_client
import configuration_client
import configuration_server
import file_clerk_client
import inquisitor_client
import library_manager_client
import log_client
import media_changer_client
import mover_client
import volume_clerk_client
import dbs

CMD1 = "%s%s%s"%(dbs.CMDa, "startup", dbs.CMDb)
#CMD1 = "%s%s%s"%(dbs.CMDa, "startup", dbs.CMDc)

DEFAULT_AML2_NODE = "rip10"

server_functions = { "alarm" : [alarm_client.AlarmClientInterface,
                                alarm_client.do_work],
                     "configuration" : [configuration_client.ConfigurationClientInterface,
                                        configuration_client.do_work],
                     "file" : [file_clerk_client.FileClerkClientInterface,
                               file_clerk_client.do_work],
                     "inquisitor" : [inquisitor_client.InquisitorClientInterface,
                                     inquisitor_client.do_work],
                     "library" : [library_manager_client.LibraryManagerClientInterface,
                                  library_manager_client.do_work],
                     "log" : [log_client.LoggerClientInterface,
                              log_client.do_work],
                     "media" : [media_changer_client.MediaChangerClientInterface,
                                media_changer_client.do_work],
                     "mover" : [mover_client.MoverClientInterface,
                                mover_client.do_work],
                     "volume" : [volume_clerk_client.VolumeClerkClientInterface,
                                 volume_clerk_client.do_work],
		     "database" : [dbs.Interface,
			           dbs.do_work]
                         }

# these general functions perform various system functions
def call_function(executable, argv):
    # pull out the arguments from argv and create a string that can be
    # passed to os.system.
    str = executable
    for arg in argv:
        str = "%s %s"%(str, arg)
    # check to see if we need to setup the environment a little
    if not os.environ.has_key("ENSTORE_DIR"):
        str = ". /usr/local/etc/setups.sh; setup enstore; %s"%str
    return os.system(str)

def get_farmlet(default):
    if len(sys.argv) > 2:
        return sys.argv[2]
    else:
        return default

def get_argv3(default=" "):
    if len(sys.argv) > 3:
        return sys.argv[3]
    else:
        return default

def do_rgang_command(fdefault, command):
    farmlet = get_farmlet(fdefault)
    print '/usr/local/bin/rgang %s \"%s\"'%(farmlet, command)
    return os.system('/usr/local/bin/rgang %s \"%s\"'%(farmlet, command))

# keep a list of the commands (specific and generic) accessible when in user
# mode.
class GenericUserOptions:
    
    g_server_options = ["help", "timeout=", "retries="]

    def get_options(self):
        return self.g_server_options

class UserOptions(GenericUserOptions):

    server_options = {
        "file" : ["bfid=", "restore=", "list=", "recursive"],
        "library" : ["priority=", "delete_work=", "get_queue"],
        "volume" : ["add=", "delete=", "new_library=", "no_access=",
                    "read_only=", "restore=", "update=", "vol="]
        }
    server_intfs = {}

    # get a new server interface and store it to use later if needed
    def get_server_intf(self, skey):
        functions = server_functions.get(skey, None)
        if not functions is None:
            self.server_intfs[skey] = self.server_intfs.get(skey,
                                                            functions[0]())
            return self.server_intfs[skey]
        else:
            return None

    # return a list of the allowed options given the command key (server)
    def get_options(self, skey):
        opts = self.server_options.get(skey, [])
        if opts:
            opts = opts + GenericUserOptions.get_options(self)
        return opts

    def get_valid_options(self, skey):
        if self.user_mode:
            opts = self.get_options(skey)
        else:
            intf = self.get_server_intf(skey)
            if not intf is None:
                opts = intf.options()
            else:
                # this was not a valid server key
                opts = []
        return opts

    def get_valid_servers(self):
        if self.user_mode:
            # user mode
            servers = self.server_options.keys()
            servers.sort()
        else:
            # admin mode, all are allowed
            servers = server_functions.keys()
            servers.sort()
        return servers

    # figure out if the passed key is a valid server key
    def is_valid_server(self, skey):
        if self.user_mode:
            return self.server_options.has_key(skey)
        else:
            return 1

    # figure out if the passed option is valid for this key
    def is_valid_option(self, skey, opt):
        if self.user_mode:
            if self.is_valid_server(skey):
                # check if the option is valid
                opts = self.get_valid_options(skey)
                for vopt in opts:
                    if opt == vopt:
                        break
                else:
                    # not a valid opt
                    rtn = 0
                # we found it
                rtn = 1
            else:
                # this is not a valid server key
                rtn = 0
        else:
            # we are in admin mode and all opts are valid
            rtn = 1

class EnstoreInterface(UserOptions):

    def __init__(self):
        # the user can enter the least amount of text that uniquely
        # identifies the desired server. (e.g. - i for inquisitor).  so get
        # the full server name here.
        all_servers = self.get_valid_servers()
        total_matches = self.find_server_match(all_servers)
        if total_matches > 1:
            # not enough info was entered and we matched more than
            # once.  in any case, print help & exit
            self.print_valid_servers()
        elif total_matches == 1:
            # look through the command line and verify that it only consists of
            # allowed options.
            # remove the 'enstore' name from sys.argv
            del sys.argv[0]
            # call the servers' interface, since we pass in a list of valid
            # options, we do not have to validate them, getopts does it
            self.server_intf = server_functions[self.matched_server][0](1,
                                   self.get_valid_options(self.matched_server))
        else:
            # we did not match anything.  if this is user_mode, check if the
            # entered server is a real one, just not a valid one.  if so, print
            # a list of the valid servers, else print full help
            if self.user_mode:
                servers = server_functions.keys()
                total_matches = self.find_server_match(servers)
                if total_matches == 0:
                    # nope, this is a bogus first arg, we don't know what was
                    # meant, print all the help
                    self.print_help()
                else:
                    # this was an existing but invalid server, print the valid
                    # ones
                    self.print_valid_servers()
            else:
                # we were allowed access to all servers but did not match on
                # any of them, so we don't know what was meant, print all help
                self.print_help()
            self.matched_server = ""

    def find_server_match(self, servers):
        total_matches = 0
        pattern = "^%s"%sys.argv[1]
        for server in servers:
            match = re.match(pattern, server, re.I)
            if not match is None:
                total_matches = total_matches + 1
                self.matched_server = server
        return total_matches

    def print_valid_servers(self):
        servers = self.get_valid_servers()
        print "\nERROR: Allowed servers/commands are : "
        for server in servers:
            print "\t%s"%server

    def print_valid_options(self, server):
        opts = self.get_valid_options()
        print "\nERROR: Allowed options for %s are : "
        for opt in opts:
            print "\t%s"%opt

    def print_usage_line(self, server, intf):
        if self.user_mode:
            # only print the options we support
            intf.print_usage_line(self.get_valid_options(server))
        else:
            intf.print_usage_line()

    def print_help(self):
        cmd = "enstore"
        if not self.user_mode:
            call_function("pnfsa", "")
            print "\n%s start   [--just server --ping --asynch --nocheck]"%cmd
            print   "%s stop    [--just server --xterm server]"%cmd
            print   "%s restart [--just server --xterm server]"%cmd
            print   "%s ping    [timeout_seconds]"%cmd
            print   "%s qping   [timeout_seconds]"%cmd
            print   "%s ps                 (list enstore related processes)"%cmd
            print "\n%s Estart   farmlet   (global Enstore start on all farmlet nodes)"%cmd
            print   "%s Estop    farmlet   (global Enstore stop on all farmlet nodes)"%cmd
            print   "%s Erestart farmlet   (global Enstore restart on all farmlet nodes)"%cmd
            print "\n%s EPS      farmlet   (global Enstore-associated ps on all farmlet nodes)"%cmd
            print "\n%s aml2               (lists current mount state & queue list on aml2 robot)"%cmd
        else:
            call_function("pnfs", "")            
        print "\n"
        servers = self.get_valid_servers()
        for server in servers:
            # print the usage line for each server
            print "%s %s "%(cmd, server),
            intf = self.get_server_intf(server)
            if not intf == None:
                self.print_usage_line(server, intf)

class Enstore(EnstoreInterface):

    timeout = 2
    retry = 2
    mc_type = ("type", "AML2_MediaLoader")

    def __init__(self, mode):
        self.user_mode = mode

    # try to get the configuration information from the config server
    def get_config_from_server(self):
        rtn = 0
        port = os.environ.get('ENSTORE_CONFIG_PORT', 0)
        port = string.atoi(port)
        if port:
            # we have a port
            host = os.environ.get('ENSTORE_CONFIG_HOST', 0)
            if host:
                # we have a host
                csc = configuration_client.ConfigurationClient((host, port))
                try:
                    t = csc.get_dict_entry(self.mc_type, self.timeout,
                                           self.retry)
                    servers = t.get("servers", "")
                    if servers:
                        # there may be more than one, we will use the first
                        t = csc.get_uncached(servers[0], self.timeout,
                                             self.retry)
                        # if there is no specified host, use the default
                        self.node = t.get("host", DEFAULT_AML2_NODE)
                    else:
                        # there were no media changers of that type, use the
                        # default node
                        self.node = DEFAULT_AML2_NODE
                    rtn = 1
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
                    rtn = 1
        return rtn

    # get the node name where the aml2 robot media changer is running
    def get_aml2_node(self):
        pass

    # this is where all the work gets done
    def do_work(self):
        if len(sys.argv) > 1:
            arg1 = sys.argv[1]
        else:
            # no parameters were entered
            arg1 = ''
        # check if help was asked for and no server passed in
        if not self.user_mode and \
           arg1 == "start" or arg1 == "enstore-start":
            rtn = call_function("enstore-start", sys.argv[2:])
        elif not self.user_mode and arg1 == "ping":
            rtn = call_function("enstore-ping", sys.argv[2:])
        elif not self.user_mode and arg1 == "qping":
            rtn = call_function("quick-ping", sys.argv[2:])
        elif not self.user_mode and \
             arg1 == "stop" or arg1 == "enstore-stop":
            rtn = call_function("enstore-stop", sys.argv[2:])
        elif not self.user_mode and arg1 == "restart":
            rtn = call_function("enstore-stop", sys.argv[2:])
            rtn = call_function("enstore-start --nocheck", sys.argv[2:])
        elif not self.user_mode and arg1 == "backup":
            rtn = call_function("python $ENSTORE_DIR/src/backup.py", sys.argv[2:])
        elif not self.user_mode and arg1 == "Estart":
            command="%s enstore-start %s%s"%(CMD1, get_argv3("enstore"), dbs.CMD2)
            rtn = do_rgang_command("enstore",command)
        elif not self.user_mode and arg1 == "Estop":
            command="%s enstore-stop %s%s"%(CMD1, get_argv3("enstore-down"), dbs.CMD2)
            rtn = do_rgang_command("enstore-down",command)
        elif not self.user_mode and arg1 == "Erestart":
            command="%s enstore-stop %s%s"%(CMD1, get_argv3("enstore-down"), dbs.CMD2)
            rtn1 = do_rgang_command("enstore-down",command)
            command="%s enstore-start %s%s"%(CMD1, get_argv3("enstore"), dbs.CMD2)
            rtn2 = do_rgang_command("enstore",command)
            rtn = rtn1|rtn2
        elif not self.user_mode and arg1 == "aml2":
            # find the node to rsh to.  this node is the one associated with
            # the media changer of type "AML2_MediaLoader"
            #   if the configuration server is running, get the information
            #   from it.  if not, just read the config file pointed to by
            #   $ENSTORE_CONFIG_FILE.  if neither of these works, assume node
            #   in DEFAULT_AML2_NODE.
            if not self.get_config_from_server() and \
               not self.get_config_from_file():
                self.node = DEFAULT_AML2_NODE
            cmd = 'rsh %s "sh -c \'. /usr/local/etc/setups.sh;setup enstore;dasadmin listd2 | grep rip;dasadmin list rip1\'"'%self.node
            rtn = os.system(cmd)
        elif not self.user_mode and arg1 == "EPS":
            command=". /usr/local/etc/setups.sh; setup enstore; EPS"
            rtn = do_rgang_command("enstore",command)
        elif arg1 == "ps":
            rtn = call_function("EPS", sys.argv[2:])
        elif not self.user_mode and arg1 == "pnfs": 
            rtn = call_function("pnfsa", sys.argv[2:])
        elif arg1 == "pnfs": 
            rtn = call_function("pnfs", sys.argv[2:])
        else:
            if arg1 == "help" or arg1 == "--help" or arg1 == '': 
                rtn = 0
                self.print_help()
            else:
                # it was not one of the above commands.  assume it was a server
                # request.
                EnstoreInterface.__init__(self)
                if self.matched_server == "":
                    rtn = 1
                else:
                    # call the passed in server to do it's stuff
                    rtn = server_functions[self.matched_server][1](self.server_intf)
        sys.exit(rtn)
