###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
#
import sys
import re
import os

# enstore imports
import alarm_client
import configuration_client
import file_clerk_client
import inquisitor_client
import library_manager_client
import log_client
import media_changer_client
import mover_client
import volume_clerk_client

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
                                 volume_clerk_client.do_work]
                         }

# these general functions perform various system functions
def call_function(executable, argv):
    # pull out the arguments from argv and create a string that can be
    # passed to os.system.
    str = executable
    for arg in argv:
        str = "%s %s"%(str, arg)
    return os.system(str)

def get_farmlet(default):
    if len(sys.argv) > 1:
        return sys.argv[1]
    else:
        return default

def do_rgang(fdefault, path1, path2):
    farmlet = get_farmlet(fdefault)
    if os.path.exists(path1):
        path = path1
    else:
        path = path2
    return os.system('/usr/local/bin/rgang %s "%s"'%(farmlet, path))

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
                                                            functions[0](0))
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
            print "\n%s start   [--just server --ping]"%cmd
            print   "%s stop    [--just server --xterm server]"%cmd
            print   "%s restart [--just server --xterm server]"%cmd
            print   "%s ping    [timeout_seconds]"%cmd
            print   "%s qping   [timeout_seconds]"%cmd
            print "\n%s Estart   farmlet   (global Enstore start on all farmlet nodes)"%cmd
            print   "%s Estop    farmlet   (global Enstore stop on all farmlet nodes)"%cmd
            print   "%s Erestart farmlet   (global Enstore restart on all farmlet nodes)"%cmd
            print "\n%s Esys     farmlet   (global Enstore-associated ps on all farmlet nodes)"%cmd
            print "\n%s emass              (lists current mount state & queue list on emass robot)"%cmd
        print "\n"
        servers = self.get_valid_servers()
        for server in servers:
            # print the usage line for each server
            print "%s %s "%(cmd, server),
            intf = self.get_server_intf(server)
            if not intf == None:
                self.print_usage_line(server, intf)

class Enstore(EnstoreInterface):

    def __init__(self, mode):
        self.user_mode = mode

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
            rtn = call_function("$ENSTORE_DIR/bin/enstore-start", sys.argv)
        elif not self.user_mode and arg1 == "ping":
            rtn = call_function("$ENSTORE_DIR/bin/enstore-ping", sys.argv)
        elif not self.user_mode and arg1 == "qping":
            rtn = call_function("$ENSTORE_DIR/bin/quick-ping", sys.argv)
        elif not self.user_mode and \
             arg1 == "stop" or arg1 == "enstore-stop":
            rtn = call_function("$ENSTORE_DIR/bin/enstore-stop", sys.argv)
        elif not self.user_mode and arg1 == "restart":
            rtn = call_function("$ENSTORE_DIR/bin/enstore-stop", sys.argv)
            rtn = call_function("$ENSTORE_DIR/bin/enstore-start --nocheck",
                                sys.argv)
        elif not self.user_mode and arg1 == "backup":
            rtn = call_function("python $ENSTORE_DIR/src/backup.py", sys.argv)
        elif not self.user_mode and arg1 == "Estart":
            # the special check for /export/home/bakken if a kludge for node fntt
            bakken = os.path.expanduser("~bakken")
            rtn = do_rgang("enstore", "%s/enstore/sbin/estart"%bakken,
                           "$ENSTORE_DIR/sbin/estart")
        elif not self.user_mode and arg1 == "Estop":
            # the special check for /export/home/bakken if a kludge for node fntt
            bakken = os.path.expanduser("~bakken")
            rtn = do_rgang("enstore-down",
                           "%s/enstore/sbin/estop"%bakken,
                           "$ENSTORE_DIR/sbin/estop")
        elif not self.user_mode and arg1 == "Erestart":
            # the special check for /export/home/bakken if a kludge for node fntt
            bakken = os.path.expanduser("~bakken")
            rtn = do_rgang("enstore", "%s/enstore/sbin/erestart"%bakken,
                           "$ENSTORE_DIR/sbin/erestart")
        elif not self.user_mode and arg1 == "emass":
            rtn = os.system('rsh rip1 "$ENSTORE_DIR/sbin/egrau"')
        elif not self.user_mode and arg1 == "Esys":
            farmlet = get_farmlet("enstore")
            # get the operating system type (same as shell 'uname' command)
            os_type = os.uname()[0]
            if os_type == "Linux":
                str = "ps auxww"
            else:
                str = "ps -ef"
            rtn = os.system('/usr/local/bin/rgang %s %s| egrep "python|ecmd|encp|reader|writer|dasadmin|mt |db_"'%(farmlet, str))
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
                    server_functions[self.matched_server][1](self.server_intf)
                    rtn = 0
        sys.exit(rtn)
