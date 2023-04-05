#!/usr/bin/env python

###############################################################################
# src/$RCSfile$   $Revision$
#
# start enstore
#
#
#

#Notes: Various functions return 0 for the server is not running an 1 if it is.

# system imports
#
import sys
import os
import string
import errno
import socket
import grp
import pwd
import time
import subprocess
import select

# enstore imports
import setpath
import e_errors
import enstore_constants
import enstore_functions
import enstore_functions2
import generic_client
import option
import hostaddr
import Trace
import Interfaces

import configuration_client
import event_relay_client

#Less hidden side effects to call this?  Also, pychecker perfers it.
### What does this give us?
setpath.set_enstore_paths()

MY_NAME = "ENSTORE_START"
SEND_TO = 3
SEND_TM = 1
#
# These are common to all start/stop functionality. #######################
#

#global cache to avoid looking up the same ip address over and over for the
# machine it is running on when starting or stoping multiple Enstore servers.
# See function this_host().  Starting a servers worth of Enstore server
# processes shouldn't take that long; not long enough to need to worry
# about the hostname and ip address list changing.
host_names_and_ips = None

def get_csc():
    # get a configuration server
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))

    return csc

#Return all IP address and hostnames for this node/host/machine.
def this_host():
    global host_names_and_ips  #global cache variable

    if host_names_and_ips == None:
        try:
            #rtn = socket.gethostbyname_ex(socket.getfqdn())
            hostname = socket.getfqdn()
            rtn = socket.getaddrinfo(hostname, None)
        except (socket.error, socket.herror, socket.gaierror), msg:
            try:
                message = "unable to obtain hostname information: %s\n" \
                          % (str(msg),)
                sys.stderr.write(message)
                sys.stderr.flush()
            except IOError:
                pass
            sys.exit(1)
        #rtn_formated = [rtn[0]] + rtn[1] + rtn[2]
        rtn_formatted = [hostname, hostname.split('.')[0], rtn[0][4][0]]

        interfaces_list = Interfaces.interfacesGet()
        for interface in interfaces_list.keys():
            ip = interfaces_list[interface]['ip']
            if ip == "127.0.0.1":
                continue
            try:
                rc = socket.gethostbyaddr(ip)
            except (socket.error, socket.herror, socket.gaierror), msg:
                try:
                    message = "unable to obtain hostname information: %s\n" \
                              % (str(msg),)
                    sys.stderr.write(message)
                    sys.stderr.flush()
                except IOError:
                    pass
                sys.exit(1)
            rc_formatted = [rc[0]] + rc[1] + rc[2]

            rtn_formatted = rtn_formatted + rc_formatted

        host_names_and_ips = rtn_formatted

    return host_names_and_ips

def is_on_host(host):
    if host in this_host():
        return 1

    return 0

##########################################################################

def output(server_name):
    #Determine where to redirect the output.

    tmp_dir = os.environ.get('ENSTORE_OUT',None)
    if tmp_dir == None:
        tmp_dir = os.environ.get('ENSTORE_HOME',None)
        if tmp_dir == None:
            tmp_dir = os.environ.get('ENSTORE_DIR','')

    try:
        output_dir_base = os.path.join(tmp_dir, "tmp")
    except:
        output_dir_base = "/tmp/enstore/"
        try:
            sys.stderr.write("Unable to determine temp. directory.  Using %s." %
                             output_dir_base)
            sys.stderr.flush()
        except IOError:
            pass
    try:
        username = pwd.getpwuid(os.geteuid())[0]
    except (KeyError, IndexError):
        username = os.geteuid()
    try:
        output_dir = os.path.join(output_dir_base, username)
        try:
            os.makedirs(output_dir)
        except OSError, msg:
            #If the file already exists, this really is not an error.
            if msg.errno != errno.EEXIST:
                raise OSError, msg
    except OSError:
        output_dir = None
        try:
            sys.stderr.write("Unable to create tmp directory %s: %s" %
                             (output_dir, str(msg)))
            sys.stderr.flush()
        except IOError:
            pass
    try:
        output_file = os.path.join(output_dir, "%s.out" % server_name)
    except TypeError:
        output_file = ""

    #By this point output_file should look something like:
    # $ENSTORE_DIR/enstore/ratekeeper.out
    # |--base-----|-name--|--server--|

    return output_file

def save(server_name):
    inf = output(server_name)
    (directory,of) = os.path.split(inf)
    of="%s.sav"%(of,)
    of=os.path.join(directory,of)
    #Determine where to redirect the output.
    try:
        os.system("mv %s %s > /dev/null 2>/dev/null"%(inf,of))
    except:
        pass

def write_pid_file(servername):
    #Get the pid file information.
    try:
        user_name = pwd.getpwuid(os.geteuid())[0]
    except KeyError:
        user_name = os.geteuid()
    pid_dir = os.path.join(enstore_functions.get_enstore_tmp_dir(),
                           user_name)
    pid_file = os.path.join(pid_dir, "%s.pid" % (servername,))

    try:
        #Make the pid dir.
        os.makedirs(pid_dir)
    except OSError, msg:
        if hasattr(msg, 'errno') and msg.errno == errno.EEXIST:
            pass
        else:
            try:
                sys.stderr.write(
                    "Error creating pid directory: %s\n" % str(msg))
                sys.stderr.flush()
            except IOError:
                pass

    #Make the pid file.
    try:
        f = open(pid_file, "wr")
        msg = "%s %s"%(os.getpid(), time.strftime("%b%d %H:%M",time.localtime(time.time())))
        #f.write(str(os.getpid()))
        f.write(msg)
        f.close()
    except OSError, msg:
        try:
            sys.stderr.write(
                "Error writing pid file: %s\n", str(msg))
            sys.stderr.flush()
        except IOError:
            pass

#Return true if the system is in one of the production systems.
def is_in_cluster():

    #If we are on the configuration server host, check the config file
    # directly.
    if os.environ['ENSTORE_CONFIG_HOST'] in this_host():
        conf_dict = enstore_functions.get_config_dict().configdict
        kcs = conf_dict.get('known_config_servers')

    #Any other system we need to check with the configuration server.
    else:
        csc = get_csc()
        kcs = csc.get('known_config_servers', 3, 3)

    #If there wasn't config server information assume it wasn't a production
    # system.
    if not kcs:
        return 0

    #Simple loop to determine if the system is a production system.
    for cluster in kcs.keys():
        if cluster == socket.gethostname()[:len(cluster)]:
            return 1

    return 0

def start_server(cmd, servername):
    cmd_list = cmd.split()

    if(os.fork() == 0):
        #Is the child.

        #Send stdout and strerr to the output file.
        save(servername)
        os.dup(1)
        os.close(1)
        os.open(output(servername), os.O_WRONLY|os.O_CREAT|os.O_TRUNC, 0664)
        os.close(2)
        os.dup(1)

        #Write out the pid file.
        write_pid_file(servername)

        #Expand the paths before executing.
        for i in range(len(cmd_list)):
            cmd_list[i] = os.path.expanduser(cmd_list[i])
            cmd_list[i] = os.path.expandvars(cmd_list[i])

        print "Will execute",cmd_list
        #Execute the new server.
        os.execvp(cmd_list[0], cmd_list)

#If the system is in a production cluster make in run as user enstore
# if possible.
def check_user():

    #If in a cluster system...
    if is_in_cluster():
        #First determine if running as root, if so become enstore and restart.
        if os.geteuid() == 0:
            try:
                enstore_gid = grp.getgrnam("enstore")[2]
                os.setegid(enstore_gid)
            except (OSError, KeyError, IndexError):
                print "Should be running as group enstore, " \
                      "but the enstore group is not found."
                sys.exit(1)
            try:
                enstore_uid = pwd.getpwnam("enstore")[2]
                os.seteuid(enstore_uid)
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

##########################################################################

#def check_db(csc, name, intf, cmd):
#
#    info = csc.get("volume_clerk", 5, 3)
#    if not info.get('host', None) in this_host() and \
#           not info.get('hostip', None) in this_host():
#        return
#
#    # ignore nocheck
#    # if intf.nocheck:
#    #    rtn = 0
#    # else:
#    print "Checking %s." % name
#
#    rtn = os.popen("ps -elf | grep %s | grep -v grep" % name).readlines()
#
#    if not rtn:
#        print "Starting %s." % name
#        os.system(cmd)

#If the event relay responded to alive messages, this would not be necessary.
def check_event_relay(csc, intf, cmd):

    name = "event_relay"

    info = csc.get(name, 5, 3)
    if not info.get('host', None) in this_host() and \
           not info.get('hostip', None) in this_host():
        return

    erc = event_relay_client.EventRelayClient(event_relay_host=info['hostip'],
                                              event_relay_port=info['port'])

    if intf.nocheck:
        rtn = 1
    else:
        print "Checking %s." % name
        rtn = erc.alive()

    if rtn:
        print "Starting %s." % name

        #Start the event relay.
        start_server(cmd, name)

        #Make sure that event_relay is alive.  It sould not be
        # this complicated, but there were situations were TIMEDOUT still
        # occured when there was no good reason for it to occur.  Putting
        # a loop here fixed it.
        for unused in (0, 1, 2):
            try:
                #rtn = 0 implies alive, rtn = 1 implies dead.
                rtn = erc.alive()
            except (socket.error, select.error, e_errors.EnstoreError), msg:
                if msg.errno == errno.ETIMEDOUT:
                    rtn = {'status':(e_errors.TIMEDOUT, enstore_constants.EVENT_RELAY)}
                else:
                    rtn = {'status':(e_errors.NET_ERROR, str(msg))}
            except errno.errorcode[errno.ETIMEDOUT]:
                rtn = {'status':(e_errors.TIMEDOUT,
                                 errno.errorcode[errno.ETIMEDOUT])}
            if rtn == 0:
                break

        if rtn == 1:
            print "Server %s not started." % (name,)
            sys.exit(1)
    else:
        print "Found event_relay."


# lets start fixing thisngs at least from configuration server
def check_config_server(intf, name='configuration_server', start_cmd=None):
    #host = socket.gethostname()
    config_host = os.environ.get('ENSTORE_CONFIG_HOST')
    if not config_host:
        print "ENSTORE_CONFIG_HOST is not set. Exiting"
        sys.exit(1)

    #host_ips = socket.gethostbyname_ex(host)[2]

    config_host_ip = socket.getaddrinfo(config_host, None)[0][4][0]

    #Compare the the ip values.  If a match is found continue with starting
    # the config server.  Otherwise return.
    if not is_on_host(config_host_ip):
        return
    #chip = config_host_ip.split('.')
    #for host_ip in host_ips:
    #    hip = host_ip.split('.')
    #    matched = 0
    #    for i in range(0, len(chip)):
    #        if hip[i] != chip[i]:
    #            break
    #    else:
    #        matched = 1
    #    if matched:
    #        break
    #if not matched:
    #    return

    if intf.nocheck:
        rtn = {'status':("nocheck","nocheck")}
    else:
        print "Checking %s." % name
        # see if EPS returns config_server"
        cmd = 'EPS | egrep "%s" | egrep -v "%s|%s|%s"'%(name, "enstore start", "enstore stop", "enstore restart")
        pipeObj = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, close_fds=True)
        if pipeObj:
            result = pipeObj.communicate()[0]
            if len(result) >= 1:
                # running, don't start
                rtn = {'status':(e_errors.OK,"running")}
            else:
                rtn = {'status':("e_errors.SERVERDIED","not running")}

    if not e_errors.is_ok(rtn):
        print "Starting %s" % (name,)

        #Start the server.
        start_server(start_cmd, name)

        #Check the restarted server.  It sould not be this complicated, but
        # there were situations where TIMEDOUT still occured when there was
        # no good reason for it to occur.  Putting a loop here fixed it.
        for unused in (0, 1, 2, 3, 4, 5):
            time.sleep(2)
            cmd = 'EPS | egrep %s'%(name,)
            pipeObj = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, close_fds=True)
            if pipeObj:
                result = pipeObj.communicate()[0]
                if len(result) >= 1:
                    rtn = {'status':(e_errors.OK,"running")}
                    break

        else:
            rtn = {'status':("e_errors.SERVERDIED","not running")}
            print "Server %s not started." % (name,)
            sys.exit(1)
    else:
        print "Found %s" % (name,)

def check_server(csc, name, intf, cmd):
    #Check if this server is supposed to run on this machine.
    info = csc.get(name, SEND_TO, SEND_TM)
    ##HACK:
    #Do a hack for the monitor server.  Since, it runs on all enstore
    # machines we need to add this information before continuing.
    if e_errors.is_ok(info) and name == enstore_constants.MONITOR_SERVER:
        info['host'] = socket.gethostname()
        #info['hostip'] = socket.gethostbyname(info['host'])
        info['hostip'] = hostaddr.name_to_address(info['host'])
        info['port'] = enstore_constants.MONITOR_PORT
    ##END HACK.
    if not is_on_host(info.get('host', None)) and \
       not is_on_host(info.get('hostip', None)):
        return
    if intf.nocheck:
        rtn = {'status':("nocheck","nocheck")}
    else:
        gc = generic_client.GenericClient(csc, MY_NAME, server_name=name,
                                          rcv_timeout=SEND_TO, rcv_tries=SEND_TM)

        print "Checking %s." % name

        try:
            # Determine if the host is alive.
            rtn = gc.alive(name, SEND_TO, SEND_TM)
        except (socket.error, select.error, e_errors.EnstoreError), msg:
            if hasattr(msg, "errno") and msg.errno == errno.ETIMEDOUT:
                rtn = {'status':(e_errors.TIMEDOUT, name)}
            else:
                rtn = {'status':(e_errors.NET_ERROR, str(msg))}

        except errno.errorcode[errno.ETIMEDOUT]:
            rtn = {'status':(e_errors.TIMEDOUT,
                             errno.errorcode[errno.ETIMEDOUT])}

        if not e_errors.is_ok(rtn):
            # check if python process with this name is still running
            ch_cmd = 'EPS | egrep "%s" | egrep python | egrep -v "%s|%s|%s"'%(name, "enstore start", "enstore stop", "enstore restart")
            pipeObj = subprocess.Popen(ch_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True, close_fds=True)
            if pipeObj:
                rc = pipeObj.communicate()[0]
                if rc:
                    result = rc.split("\n")
                else:
                    result = rc
                if len(result) > 1:
                    # get rid of empty lines
                    while result.count("") > 0:
                        result.remove("")
                if len(result) >= 1:
                    dont_start = True
                    if len(result) == 1:
                        # the command line for the running server looks like:
                        # python <path>server_name server_name.<suffix>
                        # check that the command line returned by ch_cmd
                        # satisfies this rule
                        c_l = result[0].split(' ')
                        if c_l[1].find(name) != -1 and c_l[2].find(name) != -1:
                            pass
                        else:
                           dont_start = False
                    # running, don't start
                    if dont_start:
                        rtn = {'status':(e_errors.OK,"running")}
                        print "Server %s does not respond but is running as \n %s" % (name, result)
                else:
                    rtn = {'status':("e_errors.SERVERDIED","not running")}

    #Process response.
    if not e_errors.is_ok(rtn):
        print "Starting %s: %s:%s" % (name, info['hostip'], info['port'])

        #Start the server.
        start_server(cmd, name)

        #Check the restarted server.  It sould not be this complicated, but
        # there were situations where TIMEDOUT still occured when there was
        # no good reason for it to occur.  Putting a loop here fixed it.
        for unused in (0, 1, 2):
            rtn = csc.alive(name, SEND_TO, SEND_TM)

            if e_errors.is_ok(rtn):
                break
            if rtn['status'][0] == e_errors.TIMEDOUT:
                continue
            if not e_errors.is_ok(rtn):
                print "Server %s not started." % (name,)
                sys.exit(1)
    else:
        print "Found %s: %s:%s" % (name, info.get('hostip', None),
                                   info.get('port', None))


class EnstoreStartInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1, nocheck = None):
        self.name = "START"
        self.just = None
        self.all = 0 #False
        self.nocheck = nocheck  #We need nocheck for enstore_restart.

        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.start_options)

    # parse the options like normal but make sure we have other args
    def parse_options(self):

        generic_client.GenericClientInterface.parse_options(self)

        self.completed_server_name()

    def completed_server_name(self):
        if self.just:
            count = 0
            for name in self.complete_names:
                if self.just == name[:len(self.just)]:
                    count = count + 1
                    hold = name
            if count == 1:
                self.just = hold

    def should_start(self, server_name):

        if self.all:
            return 1
        if self.just == server_name:
            return 1
        if self.just == None and server_name not in self.non_default_names:
            return 1

        return 0

    non_default_names = ["monitor_server","pnfs_agent"]

    complete_names = [
        "accounting_server",
        "drivestat_server",
        "configuration_server",
        "event_relay",
        "log_server",
        "alarm_server",
        "volume_clerk",
        "file_clerk",
        "info_server",
#        "db_checkpoint",
#        "db_deadlock",
        "inquisitor",
        "ratekeeper",
        "library",
        "media",
        "migrator",
        "mover",
        "udp_proxy_server",
        "lm_director",
        "dispatcher",
        "monitor_server",
        "pnfs_agent",
        ]


    start_options = {
        option.JUST:{option.HELP_STRING:"specify single server",
                     option.VALUE_NAME:"just",
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_LABEL:"server name",
		     option.USER_LEVEL:option.ADMIN,
                     },
        option.ALL:{option.HELP_STRING:"specify all servers",
                     option.VALUE_USAGE:option.IGNORED,
                     option.DEFAULT_NAME:"all",
                     option.DEFAULT_TYPE:option.INTEGER,
		     option.USER_LEVEL:option.ADMIN,
                     },
        option.NOCHECK:{option.HELP_STRING:"do not check if server is"
                        " already running.",
                        option.DEFAULT_NAME:"nocheck",
                        option.VALUE_USAGE:option.IGNORED,
                        option.DEFAULT_TYPE:option.STRING,
                        option.USER_LEVEL:option.ADMIN,
                        }
        }
def do_work(intf):
    Trace.init(MY_NAME)

    #If the log server is already running, send log messages there.
    #if e_errors.is_ok(csc.alive("log_server", 2, 2)):
    #    logc = log_client.LoggerClient(csc, MY_NAME, 'log_server')
    #    Trace.set_log_func(logc.log_func)

    #Check if the user is enstore or root on a production node.
    check_user()
    #Get the python binary name.  If necessary, python options could
    # be specified here.
    #python_binary = "python"
    #Start the configuration server.
    if intf.should_start(enstore_constants.CONFIGURATION_SERVER):
        check_config_server(intf, name='configuration_server',
                            start_cmd="$ENSTORE_DIR/sbin/configuration_server "\
                            "--config-file $ENSTORE_CONFIG_FILE")

    csc = get_csc()
    rtn = csc.alive(configuration_client.MY_SERVER, 3, 3)
    if not e_errors.is_ok(rtn):
        #If the configuration server was not specifically specified.
        print "Configuration server not running:", rtn['status']
        sys.exit(1)

    config_dict = csc.dump_and_save()
    # We know the config server is up.  Get the database info.
    #db_dir = csc.get('database', {}).get('db_dir', None)
    db_dir = config_dict.get('database', {}).get('db_dir', None)
    if not db_dir:
        print "Unable to determine database directory."
        sys.exit(1)

    #The movers and migrators need to run as root, check for sudo.
    if os.system("sudo -V > /dev/null 2> /dev/null"): #if true sudo not found.
        sudo = str("")
    else:
        sudo = str("sudo")  #found

    #Start the event relay.
    if intf.should_start(enstore_constants.EVENT_RELAY):
        check_event_relay(csc, intf,
                          "$ENSTORE_DIR/sbin/event_relay")

    #Start the servers.
    #
    # db_checkpoint and db_deadlock should be started *after* any of
    # of the volume_clerk or file_clerk starts
    #
    for server in [ enstore_constants.LOG_SERVER,
                    enstore_constants.ACCOUNTING_SERVER,
                    enstore_constants.DRIVESTAT_SERVER,
                    enstore_constants.ALARM_SERVER,
                    enstore_constants.FILE_CLERK,
                    enstore_constants.VOLUME_CLERK,
                    enstore_constants.INFO_SERVER,
                    enstore_constants.INQUISITOR,
                    enstore_constants.RATEKEEPER,
                    enstore_constants.MONITOR_SERVER,
                    enstore_constants.LM_DIRECTOR,
                    enstore_constants.DISPATCHER]:
        if intf.should_start(server):
            check_server(csc, server, intf,
                         "$ENSTORE_DIR/sbin/%s" % (server,))
    #
    # Added by Dmitry and Michael to start pnfs_agent which needs to be run as user
    # root via sudo.
    #
    for server in [enstore_constants.PNFS_AGENT] :
        if intf.should_start(server):
            check_server(csc, server, intf,
                         "%s $ENSTORE_DIR/sbin/%s" %
                         (sudo, server))

    #Start the Berkley DB dameons.
    #if intf.should_start(enstore_constants.VOLUME_CLERK) or \
    #   intf.should_start(enstore_constants.FILE_CLERK):
    #    check_db(csc, "db_checkpoint", intf,
    #             "db_checkpoint -h %s  -p 5 &" % db_dir)
    #    check_db(csc, "db_deadlock", intf,
    #             "db_deadlock -h %s  -t 1 &" % db_dir)

    #Get the library names.
    libraries = []
    lib_dicts = csc.get_library_managers2(conf_dict=config_dict)
    for lib in lib_dicts:
        lm_name = lib.get('name', None)
        if lm_name:
            libraries.append(lm_name)

    #libraries = map((lambda l: l + ".library_manager"), libraries)

    #Libraries.
    for library_manager in libraries:
        if intf.should_start(enstore_constants.LIBRARY_MANAGER) or \
           intf.should_start(library_manager):
             check_server(csc, library_manager, intf,
                          "$ENSTORE_DIR/sbin/library_manager %s" %
                          (library_manager,))

    #Media changers.
    mc_dicts = csc.get_media_changers2(conf_dict=config_dict)

    for media_changer_info in mc_dicts:
        media_changer_name = media_changer_info['name']
        if intf.should_start(enstore_constants.MEDIA_CHANGER) or \
           intf.should_start(media_changer_name):
            check_server(csc, media_changer_name, intf,
                         "$ENSTORE_DIR/sbin/media_changer %s" %
                         (media_changer_name,))
    """
    for library_manager in libraries:
        media_changer = csc.get_media_changer(library_manager)
        if intf.should_start(enstore_constants.MEDIA_CHANGER) or \
           intf.should_start(media_changer):
            check_server(csc, media_changer, intf,
                         "$ENSTORE_DIR/sbin/media_changer %s" %
                         (media_changer,))
    """

    #Movers.
    movers = csc.get_movers2(None, conf_dict=config_dict)
    #movers = map((lambda l: l + ".mover"), movers)
    for mover_info in movers:
        mover_name = mover_info['mover']+".mover"
        if intf.should_start(enstore_constants.MOVER) or \
           intf.should_start(mover_name):
            check_server(csc, mover_name, intf,
                         "%s $ENSTORE_DIR/sbin/mover %s" %
                         (sudo, mover_name,))
    """
    mover_names = []
    for library_manager in libraries:
        for lm_mover in csc.get_movers(library_manager):
            mover_name = lm_mover[enstore_constants.MOVER]
            if mover_name not in mover_names:
                #Check those already in the list to avoid duplicates.
                mover_names.append(mover_name)
    for mover_name in mover_names:
        if intf.should_start(enstore_constants.MOVER) or \
               intf.should_start(mover_name):
            check_server(csc, mover_name, intf,
                         "%s $ENSTORE_DIR/sbin/mover %s" %
                         (sudo, mover_name))
    """
    # Migrators
    migrators = csc.get_migrators(conf_dict=config_dict)
    for migrator in migrators:
        if intf.should_start(enstore_constants.MIGRATOR) or \
           intf.should_start(migrator):
            check_server(csc, migrator, intf,
                         "%s $ENSTORE_DIR/sbin/migrator %s" %
                         (sudo, migrator,))

    # Proxy servers
    proxy_servers = csc.get_proxy_servers2(conf_dict=config_dict)
    for proxy_server_info in proxy_servers:
        proxy_server_name = proxy_server_info['name']
        if intf.should_start(enstore_constants.UDP_PROXY_SERVER) or \
           intf.should_start(proxy_server_name):
            check_server(csc, proxy_server_name, intf,
                         "$ENSTORE_DIR/sbin/udp_proxy_server %s" %
                         (proxy_server_name,))
    sys.exit(0)

if __name__ == '__main__':

    intf = EnstoreStartInterface(user_mode=0)
    do_work(intf)
