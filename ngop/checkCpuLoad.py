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

# nodes that are ok if cpuload is < 5
LOAD5 = ["stkensrv1"]
LOAD5_VAL = 5.0

# nodes that are ok if cpuload is < 4
LOAD4 = ["stkensrv3", "stkenmvr5a", "d0ensrv1"]
LOAD4_VAL = 4.0

# nodes that are ok if cpuload is < 3
LOAD3 = ["stkendca4a", "stkendca5a", "stkenmvr6a"]
LOAD3_VAL = 3.0

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

    if node in LOAD5:
        if cpuLoad > LOAD5_VAL:
            print cpuLoad
        else:
            print OK_VAL
    elif node in LOAD4:
        if cpuLoad > LOAD4_VAL:
            print cpuLoad
        else:
            print OK_VAL
    elif node in LOAD3:
        if cpuLoad > LOAD3_VAL:
            print cpuLoad
        else:
            print OK_VAL
    else:
        print cpuLoad
        
