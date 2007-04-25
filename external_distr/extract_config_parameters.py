#!/usr/bin/env python

import os
import sys
import configuration_server
import pprint


def flatten2(prefix, value, flat_dict):
        if type(value) == type({}):
                for i in value.keys():
                        flatten2(prefix+'.'+str(i), value[i], flat_dict)
        elif type(value) == type([]) or type(value) == type(()):
                for i in range(len(value)):
                        flatten2(prefix+'.'+str(i), value[i], flat_dict)
        else:
                flat_dict[prefix] = value
	
def get_by_key(dict, key):
    for i in dict.keys():
        if dict[i].has_key(key):
            print "%s.%s.%s"%(i,key,dict[i][key])

def get_entry(dict, key):

    for i in dict[key].keys():
      flat_dict = {}
      flatten2(i, dict[key][i], flat_dict)
      for j in flat_dict.keys():
        print "%s:%s"%(j, flat_dict[j])
      #pprint.pprint(flat_dict)
    #for i in flat_dict.keys():
    #    print i,flat_dict[i]
	
def server_hosts(dict):
    o_d = []
    for i in dict.keys():
        if (dict[i].has_key("host")) and (not dict[i]["host"] in o_d):
            if ((i.find("library_manager")  != -1) or
                (i.find("media_changer")  != -1) or
                (i.find("clerk") != -1) or
                (i.find("server") != -1)):
               o_d.append(dict[i]["host"])
            
    o_d.sort()
    for i in o_d:
        h = i.split(".")[0]
	print h

def int_part(s):
    ipart = 0
    for ch in s:
        if ch.isdigit():
            ipart = ipart * 10. +(ord(ch)-ord('0'))*1.
    return ipart

    
def _sort(list_to_sort):
    o_d = {}
    for i in list_to_sort:
        o_d[int_part(i)] = i
    keys = o_d.keys()
    keys.sort()
    o_l = []
    for key in keys:
        o_l.append(o_d[key])
    return o_l

def mover_hosts(dict):
    o_d = []
    for i in dict.keys():
        if (dict[i].has_key("host")) and (not dict[i]["host"] in o_d):
            try:
                if i.split(".")[1] == "mover" :
                    o_d.append(dict[i]["host"])
            except IndexError:
                pass

    o_l = _sort(o_d)
    for i in o_l:
        h = i.split(".")[0]
        print h


if __name__ == '__main__':      # testing
        # get configuration file path from ENSTORE_CONFIG_FILE
        config_file = os.environ['ENSTORE_CONFIG_FILE']

        # need a ConfigurationDict to read configuration file
        cd = configuration_server.ConfigurationDict()
        cd.read_config(config_file)

        if sys.argv[1] == "server":
            server_hosts(cd.configdict)
        elif sys.argv[1] == "mover":
            mover_hosts(cd.configdict)
	else:
	    get_entry(cd.configdict, sys.argv[1])
