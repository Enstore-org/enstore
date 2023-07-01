#!/usr/bin/env python

import configuration_server
import argparse
import re

special_case_servers = {
    'config': ['configuration_server'],
    'server': [
        'dispatcher',
        'lm_director',
        'backup',
        'inquisitor',
        'ratekeeper',
        'event_relay',
        'amqp_broker',
    ],
}

allowed_groups = [
    'mover',
    'media_changer',
    'library_manager',
    'server',
    'migrator',
    'clerk',
]

def get_server_group(server):
    global special_case_servers
    global allowed_groups
    for group, special_servers in special_case_servers.items():
        if server in special_servers:
            return group
    group = server.split('.')[-1]
    if group in allowed_groups:
        return group
    group = server.split('_')[-1]
    if group in allowed_groups:
        return group
    return ''

def add_to_config_servers(config_servers, group, host, server):
    if not config_servers.has_key(group):
       config_servers[group] = {}
    if not config_servers[group].has_key(host):
       config_servers[group][host] = set()
    config_servers[group][host].add(server)

def extract_servers(config_file):
    config_dict = load_config_dict(config_file)
    config_servers = {}
    for server in config_dict.keys():
        if config_dict[server].has_key("host"):
            group = get_server_group(server)
            host = config_dict[server]["host"]
            add_to_config_servers(config_servers, group, host, server)
    return config_servers


def load_config_dict(config_file):
    cd = configuration_server.ConfigurationDict()
    cd.read_config(config_file)
    return cd.configdict


def parse_ini(ini_file):
    f = open(ini_file, 'r')
    group_re = r"\[([a-z_]+)\]"
    host_re = r"([a-z0-9\.]+) s=\"([a-zA-Z0-9_ \.]+)\""
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
    # Regenerate inventory from config and use that,
    # but add servers from old ini to tombstone group
    # if they are not in the newly generated inventory
    existing_inventory = parse_ini(args.output)
    _, config_inventory = make(args)
    for group, host_servers in existing_inventory.items():
        config_host_servers = config_inventory.get(group, {})
        for host, servers in host_servers.items():
            config_servers = config_host_servers.get(host, [])
            for server in servers:
                if not server in config_servers:
                    add_to_config_servers(config_inventory, 'tombstone', host, server)
    return args.output, config_inventory


def make(args):
    config_inventory = extract_servers(args.config)
    return args.output, config_inventory


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
