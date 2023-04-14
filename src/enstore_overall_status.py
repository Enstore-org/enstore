#!/usr/bin/env python

# $Id$

import os
import sys
import string
import time
import pwd

import enstore_functions2
import enstore_mail
import enstore_constants
import enstore_html
import configuration_client
import e_errors
import option

#These first few constants are for local files just used by this script.
TMP = ".tmp"
MYNAME = "EN_OVERALL_STAT"
LAST_STATUS_IMPORT = "last_status"
LAST_STATUS_FILE = "%s.py" % (LAST_STATUS_IMPORT,)

DEBUG = 0  #Make true if debugging to avoid sending e-mails to admins.

DOWN_L = [enstore_constants.DOWN,
	  enstore_functions2.format_time(time.time()),
	  enstore_constants.ENONE,
	  enstore_constants.ENONE,
	  enstore_constants.ENONE,
	  enstore_constants.ENONE]

DOWN_L2 = [enstore_constants.SEEN_DOWN,
	  enstore_functions2.format_time(time.time()),
	  "Can not get status from server",
	  enstore_constants.ENONE,
	  enstore_constants.ENONE,
	  enstore_constants.ENONE]

class HtmlStatusOnlyFile:

    # we need to save both the file name passed to us and the one we will
    # write to.  we will create the temp one and then move it to the real
    # one.
    def __init__(self, name):
        self.file_name = name+TMP
        self.openfile = 0
	self.opened = 0
        self.real_file_name = name

    def open(self, mode='w'):
        try:
            self.openfile = open(self.file_name, mode)
	    self.opened = 1
        except IOError:
            self.openfile = 0
	    self.opened = 0

    def install(self):
	# move the file we created to the real file name
        if (not self.real_file_name == self.file_name) and os.path.exists(self.file_name):
	    os.system("mv %s %s"%(self.file_name, self.real_file_name))

    def close(self):
	if self.openfile:
	    self.openfile.close()
	    self.openfile = 0

    def do_write(self, data, filename=None):
	if filename is None:
	    filename = self.file_name
	try:
	    self.openfile.write(data)
	except IOError, detail:
	    print "Error writing %s (%s)"%(filename, detail)

    def write(self, status, nodes_d):
        if self.openfile:
            doc = enstore_html.EnStatusOnlyPage()
            
            doc.body(status, nodes_d)
	    self.do_write(str(doc))

def setup_for_files(last_status_filename):
    destdir = os.path.dirname(last_status_filename)
    if not os.path.exists(destdir):
        #Make the path.
        os.makedirs(destdir)
    if not os.path.exists(last_status_filename):
        #Touch the file.
        f = open(last_status_filename, "w")
        f.close()

    # add this path to our python search directories
    sys.path.insert(0, destdir)
    os.environ['PYTHONPATH'] = string.join(sys.path, ':')

def copy_it(src, dst):
    try:
        fp_r = open(src, "r")
        fp_w = open(dst, "w")
        
        data = fp_r.readlines()
        fp_w.writelines(data)
    
        fp_r.close()
        fp_w.close()
    except (OSError, IOError):
        return 1

    return 0

def mark_enstore_down(status_d, node, last_status_d):
    status_d[node] = DOWN_L2
    # send mail about this, only send mail if we have been down
    # continuously and last mail was sent > 1 hour ago. so, 
    #
    #    if last_status was good, then send mail
    send_mail = 0
    ctr = last_status_d.get(node, 0)
    if ctr == 0:
	# either it was good last time, or it did not exist, in either
        # case send mail
	send_mail = 1
    elif ctr == (ctr/20)*20:
	# only send mail every approx two hours the node is seen
        # down continuously
	send_mail = 1

    #Send e-mail.
    if send_mail == 1 and DEBUG == 0:
	enstore_mail.send_mail(
            MYNAME,
            "%s not reachable to rcp overall status file" % (node,),
            "Overall status page has Enstore ball for %s as red" % (node,)
            )
    
def get_last_status():
    last_s = {}
    exec("import %s\nlast_s = last_status.__dict__.get('status_d', {})\n" \
         % (LAST_STATUS_IMPORT,))
    return last_s

def set_last_status(last_status_filename, status_d):
    fd = open(last_status_filename, 'w')
    fd.write("status_d = %s"%(status_d,))
    fd.close()

def main(intf):
    ##print "in do_work() ..."

    #Get the directory to copy the final output to.
    LCL_HTML_DIR = intf.html_dir
    if LCL_HTML_DIR == None:
        sys.stderr.write("Expected target directory as argument.\n")
        sys.exit(1)

    # where are we running, don't have to rcp to there
    thisNode = enstore_functions2.strip_node(os.uname()[1])

    temp_dir = os.path.join("/tmp/enstore",
                            pwd.getpwuid(os.geteuid())[0])
    last_status_file = os.path.join(temp_dir, LAST_STATUS_FILE)

    # fetch the files from the other nodes.  we will put them
    # in /tmp/enstore_status and import them from there
    # do some setup first
    setup_for_files(last_status_file)

    #Loop through all configuration servers mentioned on the command line
    # looking for one that is up.
    kcs = {}
    config_port = enstore_functions2.default_port()
    for config_host in intf.config_hosts:
        #Hope they use the default port everywhere.
        csc = configuration_client.ConfigurationClient((config_host,
                                                        config_port))
        kcs = csc.get('known_config_servers', timeout = 3, retry = 3)
        if e_errors.is_ok(kcs):
            del kcs['status'] #We need to stop looping over the other elements.
            break
    else:
        # Try the default system, if there is one to find.
        
        config_host =  enstore_functions2.default_host()
        #Hope they use the default port everywhere.
        csc = configuration_client.ConfigurationClient((config_host,
                                                        config_port))
        kcs = csc.get('known_config_servers', timeout = 3, retry = 3)
        if e_errors.is_ok(kcs):
            #Found what we are looking for.
            del kcs['status']
        else:
            sys.stderr.write("No suitable Enstore system found.\n")
            sys.exit(1)

    status_d = {}
    nodes = {}
    # get the last status of the enstore balls
    last_status_d = get_last_status()

    # Loop over the contents of the default systems known_config_servers
    # list of configuration servers.
    for name, value in kcs.items():
        ## Get information from the configuration server.  This
        ## information includes the location of the ENSTORESTATUSFILE
        ## on each Enstore system's web nodes, and the name of each
        ## Enstore system's web node.


        cur_host = enstore_functions2.strip_node(value[0])
        cur_port = value[1]
        cur_csc = configuration_client.ConfigurationClient((value[0],
                                                            cur_port))
        #The dictionary is the format HtmlStatusOnlyFile.write() expects.
        nodes[cur_host] = "%s mass storage" % (name,)
        
        inquisitor = cur_csc.get('inquisitor',  timeout = 3, retry = 3)
        if not e_errors.is_ok(inquisitor):
            # there was an error, mark enstore as down
            mark_enstore_down(status_d, cur_host, last_status_d)
            continue

        html_dir = inquisitor.get('html_file', None)
        status_filename = "%s/%s"%(html_dir,
                                   enstore_constants.ENSTORESTATUSFILE)

        crons = cur_csc.get('crons',  timeout = 3, retry = 3)
        if not e_errors.is_ok(crons):
            # there was an error, mark enstore as down
            mark_enstore_down(status_d, cur_host, last_status_d)
            continue

        web_node = crons.get('web_node', None)

        # make sure node is up before rcping
        if enstore_functions2.ping(web_node) != enstore_constants.IS_ALIVE:
            mark_enstore_down(status_d, cur_host, last_status_d)
            continue

        #Full emote path to the file we want to copy over.
        status_filename = os.path.join(html_dir,
                                       enstore_constants.ENSTORESTATUSFILE)
        #The name of the modules (python file) we need to use to be
        # able to import it.  The [:-3] at the end removes the .py.
        new_import_filename = "%s_%s" % (cur_host,
                                enstore_constants.ENSTORESTATUSFILE[:-3])
        #The full path name of the local file copy once it is copyied over.
        # Note we need to append the .py back here.
        new_status_filename = os.path.join(temp_dir,
                                           "%s%s" % (new_import_filename,
                                                     ".py"))
        #The dictionary is the format HtmlStatusOnlyFile.write() expects.
        #nodes[cur_host] = "%s mass storage" % (name,)

        if web_node == thisNode:
            #Local copy.
            rtn = copy_it(status_filename, new_status_filename)
        else:
            #Remote copy.
            rtn = enstore_functions2.get_remote_file(web_node,
                                                     status_filename,
                                                     new_status_filename)
        #One last check to make sure we really did copy over the file.
        if not os.path.exists(new_status_filename):
            rtn2 = 1
        else:
            rtn2 = 0

        #Read in the copied file to the python interpreter.
        if rtn == 0 and rtn2 == 0:
            exec("import %s\nstatus_d[cur_host] = %s.status\n" % \
                 (new_import_filename, new_import_filename))
            #Cleanup after ourselves.
            #try:
            #    os.remove(new_status_filename)
            #except (OSError, IOError):
            #    pass
        else:
            # there was an error, mark enstore as down
            mark_enstore_down(status_d, cur_host, last_status_d)

        if status_d[cur_host][0] == enstore_constants.DOWN:
            last_status_d[cur_host] = last_status_d.get(cur_host, 0) + 1
        else:
            last_status_d[cur_host] = 0

    #Update persistent information about how long each system has
    # been down (or hopefully not down).
    set_last_status(last_status_file, last_status_d)
            
    #Create the directory if it does not exist.
    if not os.path.exists(LCL_HTML_DIR):
        os.makedirs(LCL_HTML_DIR)
    
    # now create the web page
    filename = "%s/%s"%(LCL_HTML_DIR, enstore_constants.STATUSONLYHTMLFILE)
    only_file = HtmlStatusOnlyFile(filename)
    only_file.open()
    only_file.write(status_d, nodes)
    only_file.close()
    only_file.install()
class EnstoreOverallStatusInterface(option.Interface):
    def __init__(self, args=sys.argv, user_mode=0):

        #This is flag is accessed via a global variable.
        self.html_dir = None
        self.config_hosts = []

        option.Interface.__init__(self, args=args, user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.eos_options)
    
    eos_options = {
        option.CONFIG_HOSTS:{option.HELP_STRING:"The list of configuration"
                             " server hosts to try and find.",
                             option.VALUE_USAGE:option.REQUIRED,
                             option.VALUE_TYPE:option.LIST,
                             option.USER_LEVEL:option.ADMIN,},
        option.HTML_DIR:{option.HELP_STRING:"The directory the final output"
                         " will be copied to.",
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_TYPE:option.STRING,
                         option.USER_LEVEL:option.ADMIN,},
        }

def do_work(intf):

    try:
        exit_status = main(intf)
    except (SystemExit, KeyboardInterrupt):
        exit_status = 1
    except:
        #Get the uncaught exception.
        exc, msg, tb = sys.exc_info()
        sys.stderr.write("%s: %s\n" % (str(exc), str(msg)))
        sys.exit(1)

    sys.exit(exit_status)
    
if __name__ == "__main__":

    intf_of_eos = EnstoreOverallStatusInterface(sys.argv, 0) # zero means admin

    do_work(intf_of_eos)
