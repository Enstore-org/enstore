# system import
import os
import regsub
import re
import string

# enstore import
import interface
import configuration_server
import enstore_status
import e_errors

prefix = "       host             filesystem              Threshold       Action\n#---   ----             ----------              ----------      ----------------------\nO      mailfrom         '\"Patrol\" <patrol@"

prefix2 = ">'\n"

suffix1 = "\n" +\
          "P envars <<EOF\n" +\
          "$ensname = \"$cmdline\";\n" +\
          "$ensname =~ s/\s*\S+\s+(\S+)/$1/;\n" +\
          "$enstime = \"$year-$month-$day $hour:$min\";\n" +\
          "$ensstartfail = \"$host Enstore 3 Problem re-starting $ensname on $host.\"\n" +\
          "EOF\n" +\
          "\n" +\
          "M NO_ENSERVER<<EOF\n" +\
          "Enstore $ensname was not running on $host at $enstime\n" +\
          "                 and was restarted.\n" +\
          "EOF\n" +\
          "\n" +\
          "M START_FAILED<<EOF\n" +\
          "ecmd start failed for $ensname on $host at $enstime.\n" +\
          "EOF\n" +\
          "\n" +\
          "#-----------------------------------------------------------\n" +\
          "#    WWW - Interface\n" +\
          "#-----------------------------------------------------------\n" +\
          "\n" +\
          "\n" +\
          "P www! <<EOF\n" +\
          "\n" +\
          "\n" +\
          "#  give all managers\n" +\
          "@managers=(\"Enstore\");\n" +\
          "\n" +\
          "#  assign hosts to managers\n" +\
          "@hosts=("

suffix2 = "       );\n" +\
          "\n" +\
          "#  specify URL for all status images\n" +\
          "@status_images=(\"patrol_0.gif\",\n" +\
          "                \"patrol_1.gif\",\n" +\
          "                \"patrol_2.gif\",\n" +\
          "                \"patrol_3.gif\",\n" +\
          "                \"patrol_4.gif\"\n" +\
          "        );\n" +\
          "\n" +\
          "#  assign messages to status images\n" +\
          "@status_messages=(\"No problems detected, but history is available.\",\n" +\
          "                  \"No problems detected.\",\n" +\
          "                  \"Problems detected.\",\n" +\
          "                  \"Problems detected. Check the ressources !\",\n" +\
          "                  \"Bad problems detected. Host status is critical !\"\n" +\
          "        );\n" +\
          "\n" +\
          "#  define spaces of time in hours of history window\n" +\
          "@history_block=(0,3,6,24);\n" +\
          "\n" +\
          "#  define interval in minutes of automatic update\n" +\
          "$update_period=7;\n" +\
          " \n" +\
          "#  define ping command for optional RCP job\n" +\
          "$command_ping=\"/usr/etc/ping -c 1 -i 20\";\n" +\
          " \n" +\
          "EOF\n"

enline_prefix = "CC     hppc             ./enpatrol_alive~"

enline_suffix = "       =1        envars()\n"+ \
                "                                                      =2        envars(),write($ensstartfail)\n"

server_start_order = ("config_server", \
                      "logserver", \
                      "volume_clerk", \
                      "file_clerk", \
                      "*library_manager", \
                      "*media_changer", \
                      "*mover", \
                      "inquisitor", \
                      "admin_clerk")

DOMAIN = ".fnal.gov"

class EnpatrolFile(enstore_status.EnFile):

    def __init__(self, name, mnode):
	# add on the default domain name if not included with the mail_node
	if string.count(mnode, ".") == 0:
	    self.mail_node = mnode+DOMAIN
	else:
	    self.mail_node = mnode

	enstore_status.EnFile.__init__(self, name)

    def write(self, cdict):
	self.write_prefix()
        self.write_enlines(cdict)
        self.filedes.write(suffix1)
	self.write_hosts(cdict)
        self.filedes.write(suffix2)

    def write_prefix(self):
	self.filedes.write(prefix)
	self.filedes.write(self.mail_node)
	self.filedes.write(prefix2)

    def write_enlines(self, cdict):
	ckeys = cdict.configdict.keys()
	for srvr in server_start_order:
	    if srvr[0] == "*":
	        # this server marks a type of server of which there might be more
	        # than one in the config file, so look thru the config file to 
	        # get the name of each one that fits this type.
	        for k in ckeys:
	            if string.find(k, srvr[1:]) != -1:
	                self.filedes.write(enline_prefix+k+enline_suffix)
	    else:
	        # only tell patrol to monitor the server if it is in the config
	        # file too. or if it is for the config server which does not
	        # explicitly have an entry in the config server
	        if cdict.configdict.has_key(srvr) or srvr == "config_server":
	            self.filedes.write(enline_prefix+srvr+enline_suffix)

    def write_hosts(self, cdict):
	whost = []
	self.filedes.write("\"")
	for k in cdict.configdict.keys():
	    if cdict.configdict[k].has_key('host'):
	        # first remove the domain name
	        enhost = re.split("\.", cdict.configdict[k]['host'])
	        # keep a copy of what we wrote so we only write each host once
                if enhost[0] not in whost:
	            whost.append(enhost[0])
	            self.filedes.write(enhost[0]+" ")
	else:
	    self.filedes.write("\"\n")

class EnpatrolFileInterface(interface.Interface):

    NAME_DEFAULT = ""

    def __init__(self):
	# fill in the defaults
	self.config_file = self.NAME_DEFAULT
	self.mail_node = "????"
	self.verbose = 0
	interface.Interface.__init__(self)

        if self.config_file == self.NAME_DEFAULT:
	    try:
	        self.config_file = os.environ['ENSTORE_CONFIG_FILE']
	    except:
	        self.print_help
	        sys.exit(0)


    # define the valid command line options
    def options(self):
	return self.help_options()+["verbose=", "config_file=", "mail_node="]


if __name__ == "__main__" :

    # get the interface
    intf = EnpatrolFileInterface()
    
    # read the given config file 
    cdict = configuration_server.ConfigurationDict()
    msg = cdict.read_config(intf.config_file, intf.verbose)
    if msg == (e_errors.OK, None):
        # if the last 5 characters of the name are '.conf', then remove them
        # we will use this shortened name to tack on to the created cf file so
        # the filename is more likely unique. 
        name = regsub.sub(".conf", "", intf.config_file)
    
        # Get an enpatrol file object
        cffile = EnpatrolFile(name+"_enpatrol.cf", intf.mail_node)
        cffile.open()
        cffile.write(cdict)
        cffile.close()

