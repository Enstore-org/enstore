#!/usr/bin/env python

import os
import sys
import configuration_server
import pprint
import argparse
import re


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
                if i.split(".")[1] == "mover":
                    o_d.append(dict[i]["host"])
            except IndexError:
                pass

    o_l = _sort(o_d)
    for i in o_l:
        h = i.split(".")[0]
        print h


def parse_ini(ini_file):
    f = open(ini_file, 'r')
    group_re = r"\[([a-z_]+)\]"
    host_re = r"([a-z0-9]+) s=\"([a-zA-Z0-9_ \.]+)\""
    servers = {}
    current_group=''
    for line in f.readlines():
       if line.strip() == '':
           current_group=''
           continue
       group_match = re.match(group_re, line)
       if group_match:
           current_group = group_match.groups()[0]
           servers[current_group] = {}
           continue
       host_match = re.match(host_re, line)
       if host_match:
           host = host_match.groups()[0]
           host_servers = host_match.groups()[1].split(" ")
           servers[current_group][host] = host_servers
    f.close()
    pp = pprint.PrettyPrinter()
    pp.pprint(servers)
    return servers

def dump_ini(args):
    ini_file, inventory = args
    f = open(ini_file, 'w')
    for group, node in inventory.items():
        if group != '':
            f.write("[%s]\n" % group)
        for host, servers in node.items():
            f.write("%s s=\"%s\"\n" % (host, " ".join(servers)))
        f.write("\n")
    f.close()

def update(args):
    existing_inventory = parse_ini(args.output)
    return (args.output + ".out", existing_inventory)


def make(args):
    ## TODO use approximate functions above to get data from config file
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog='Enstore Ansible Generator',
                    description='Generates Ansible inventory with intended '
                        'state of each server on each host in '
                        'this Enstore deployment')
    parser.add_argument('update', nargs='?', choices=['update'], help='Whether to update existing file or create new')
    parser.add_argument('-o', '--output', required=True, help='Output (ansible inventory) file')
    parser.add_argument('-c', '--config', required=True, help='(Enstore) config file')
    args = parser.parse_args()

    if args.update == "update":
        print "UPDATING: Tombstones will be created for servers which have been removed from Enstore config."
        dump_ini(update(args))
    else:
        dump_ini(make(args))
