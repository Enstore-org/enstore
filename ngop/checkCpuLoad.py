import os
import string

"""
this script will determine whether to alarm on average cpu load.

the logic for determinig whether to alarm or not is highly convoluted
with respect to the node name, and so this script was created as
opposed to letting ngop execute the command below.  nodes being ok
means that no mail or alarm is generated
"""

OK_VAL = 1.0

NODES = {"stkensrv1" : 15.0,
         "d0ensrv1"  : 15.0,
         "cdfensrv1" : 15.0,
         "stkendca5a" : 5.0,
         "stkensrv3" : 5.0,
         "stkensrv2" : 5.0,
         "stkenmvr5a" : 4.0,
         "stkendca4a" : 3.0,
         "stkenmvr6a" : 3.0,
         "stkenmvr4a" : 3.0,
         "stkenmvr2a" : 3.0,
         "stkendca3a" : 3.0,
         }

# all other nodes give the standard result,
# anything above 2 results in an action


def strip_domain(node):
    node_info = string.split(node, ".")
    return node_info[0]


if __name__=="__main__":

    cmd = "uptime|/bin/sed s/.*load.*average://"
    p = os.popen(cmd, 'r')
    cpuLoad = p.readlines()
    p.close()
    cpuLoad = string.split(cpuLoad[0], " ")[-1]
    cpuLoad = float(string.strip(cpuLoad))
    node = os.uname()[1]
    node = strip_domain(node)

    nodes = NODES.keys()
    if node in nodes:
        if cpuLoad > NODES[node]:
            print cpuLoad
        else:
            print OK_VAL
    else:
        print cpuLoad
        
