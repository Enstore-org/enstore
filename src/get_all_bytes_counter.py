#!/usr/bin/env python
######################################################################
# src/$RCSfile$   $Revision$
######################################################################
import string
import os
import time
import signal
import subprocess
import configuration_client
import enstore_constants
import e_errors

# since this is being run from a cron job on hppc, i do not want to import from
# enstore.  we would normally use get_remote_file & ping from enstore_functions and
# VQFORMATED from enstore_constants.  instead define them here
#import enstore_functions
#import enstore_constants
VQFORMATED = "VOLUME_QUOTAS_FORMATED"
DEAD = 0
ALIVE = 1


def ping(node):
    # ping the node to see if it is up.
    times_to_ping = 4
    cmd = "ping -c %s %s" % (times_to_ping, node)
    p = os.popen(cmd, 'r').readlines()
    for line in p:
        if not string.find(line, "transmitted") == -1:
            # this is the statistics line
            stats = string.split(line)
            if stats[0] == stats[3]:
                # transmitted packets = received packets
                return ALIVE
            else:
                return DEAD
    else:
        # we did not find the stat line
        return DEAD


def get_remote_file(node, remote_file, newfile):
    # we have to make sure that the rcp does not hang in case the remote node is goofy
    pid = os.fork()
    if pid == 0:
        # this is the child
        rtn = subprocess.call("enrcp %s:%s %s"%(node, remote_file, newfile), shell=True)
        os._exit(rtn)
    else:
        # this is the parent, allow a total of 30 seconds for the child
        for i in [0, 1, 2, 3, 4, 5]:
            rtn = os.waitpid(pid, os.WNOHANG)
            if rtn[0] == pid:
                # pick out the top 8 bits as the return code
                return rtn[1] >> 8
            time.sleep(5)
        else:
            # the child has not finished, be brutal. it may be hung
            os.kill(pid, signal.SIGKILL)
            return 1


CTR_FILE = "enstore_system_user_data.html2"

NODES = ["d0ensrv2", "cdfensrv2", "stkensrv2"]

TOTAL_FILE = "enstore_all_bytes"
TOTAL_BYTES_FILE = "enstore_all_bytes.bytes"
MB = 1024.0 * 1024.0
GB = MB * 1024.0
TB = GB * 1024.0
PB = TB * 1024.0
UNITS = "TiB"


if __name__ == "__main__":

    # get the 3 counter files and merge them into one
    # since we are not running on an enstore node, we will assume the web
    # directory is /fnal/ups/prd/www_pages/enstore.
    total = 0.0
    total_bytes = 0.0
    active = 0.0
    active_bytes = 0.0
    units = ""
    dead_nodes = []

    cnf_d = configuration_client.get_config_dict()
    servers = cnf_d.get('known_config_servers', [])

    for server in servers:
        if (server == 'status'):
            continue
        server_name, server_port = servers.get(server)
        if ping(server_name) == ALIVE:
            csc = configuration_client.ConfigurationClient(
                (server_name, server_port))
            inq_d = {}
            inq_d = csc.get(enstore_constants.INQUISITOR, 3, 3)
            if not e_errors.is_ok(inq_d):
                dead_nodes.append(server)
                continue

            config_dict = csc.dump_and_save(5, 2)
            if not e_errors.is_ok(config_dict):
                dead_nodes.append(server)
                continue

            inq_d = config_dict.get(enstore_constants.INQUISITOR, {})
            html_dir = inq_d.get(
                "html_file", "/fnal/ups/prd/www_pages/enstore")
            byte_me_file = os.path.join(html_dir, CTR_FILE)

            newfile = "/tmp/%s-%s" % (server, VQFORMATED)
            rtn = get_remote_file(server_name, byte_me_file, newfile)
            if rtn == 0:
                # read it
                file = open(newfile)
                lines = file.readlines()
                for line in lines:
                    # translate total bytes into terabytes
                    parts = line.split()
                    bytes = float(string.strip(parts[0]))
                    total += bytes/TB
                    total_bytes += bytes
                    if len(parts) > 1:
                        bytes = float(string.strip(parts[1]))
                        active += bytes/TB
                        active_bytes += bytes
                    else:
                        active = total
                        active_bytes = total_bytes
                else:
                    file.close()
            else:
                # no info from this node
                dead_nodes.append(server)
        else:
            dead_nodes.append(server)
    else:
        # find out if we have any dead nodes
        if dead_nodes:
            str = "(does not include "
            dead_nodes.sort()
            for server in dead_nodes:
                str = str + server+","
            str = str[0:-1] + ")"
        else:
            str = ""

        # output the total count
        file = open(TOTAL_FILE, 'w')
        file.write("Active: %.3f / Total: %.3f %s %s\n" %
                   (active, total, UNITS, str))
        file.close()

        file = open(TOTAL_BYTES_FILE, 'w')
        file.write("%.0f / %.0f" % (total_bytes, active_bytes))
        file.close()
