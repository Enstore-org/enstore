import time
import string
import os

import configuration_server

DEFAULTHTMLDIR = "."

def get_config_dict():
    name = os.environ.get("ENSTORE_CONFIG_FILE", "")
    if name:
	cdict = configuration_server.ConfigurationDict()
        cdict.read_config(name)
    else:
	cdict = {}
    return cdict

def get_from_config_file(server, keyword, default):
    cdict = get_config_dict()
    if cdict:
        server_dict = cdict.configdict.get(server, None)
        if server_dict:
            return server_dict.get(keyword, default)
        else:
            return default
    else:
        return default

# return the location of the html files from the config file
def get_html_dir():
    return get_from_config_file("inquisitor", "html_file", DEFAULTHTMLDIR)

 # translate time.time output to a person readable format.
# strip off the day and reorganize things a little
def format_time(theTime, sep=" "):
    return time.strftime("%Y-%b-%d"+sep+"%H:%M:%S", time.localtime(theTime))

# strip off anything before the '/'
def strip_file_dir(str):
    ind = string.rfind(str, "/")
    if not ind == -1:
	str = str[(ind+1):]

# remove the string .fnal.gov if it is in the input string
def strip_node(str):
    return string.replace(str, ".fnal.gov", "")
