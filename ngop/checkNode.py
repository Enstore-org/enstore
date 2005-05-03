import os
import string

"""
this script will determine whether to alarm on if a node can be pinged.
this is used to ping nodes that are on a private network and not visible
to the main ngop ping facility.

"""

OK_VAL = 0
IS_ALIVE = 1
IS_DEAD = 0
NO_PING_DONE = IS_ALIVE

# nodes to ping.  only certain enstore nodes are connected to
# the private network
NODES_TO_PING = {"d0ensrv4" : ["fntt","adic2"],
                 "cdfensrv4" : ["fntt2.fnal.gov",],
                 "stkensrv4" : ["fntt","adic2"]
                 }
PING_KEYS_L = NODES_TO_PING.keys()

def strip_domain(node):
    node_info = string.split(node, ".")
    return node_info[0]

def ping(node):
    # ping the node to see if it is up.
    times_to_ping = 4
    # the timeout parameter does not work on d0ensrv2.
    #timeout = 5
    #cmd = "ping -c %s -w %s %s"%(times_to_ping, timeout, node)
    cmd = "ping -c %s %s"%(times_to_ping, node)
    p = os.popen(cmd, 'r').readlines()
    for line in p:
        if not string.find(line, "transmitted") == -1:
            # this is the statistics line
            stats = string.split(line)
            if stats[0] == stats[3]:
                # transmitted packets = received packets
                return IS_ALIVE
            else:
                return IS_DEAD
    else:
        # we did not find the stat line
        return IS_DEAD


if __name__=="__main__":

    # determine what node we are running on.  only certain nodes are connected to
    # a private network
    thisNode = strip_domain(os.uname[1])

    pingNode_l = NODES_TO_PING.get(thisNode, None)
    rtn = IS_ALIVE
    if pingNode_l:
        for pingNode in pingnode_l:
            if ping(pingNode) == IS_DEAD:
                # raise an alarm
                msg = "No ping response from %s (pinged from %s)"%(pingNode, thisNode)
                os.system(". /usr/local/etc/setups.sh;setup enstore;enstore alarm --raise --root-error='%s'"%(msg,))
                rtn = IS_DEAD
        else:
            print rtn
    else:
        print NO_PING_DONE
