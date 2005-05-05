import os
import string
import time

"""
this script will determine whether to alarm on if a node can be pinged.
this is used to ping nodes that are on a private network and not visible
to the main ngop ping facility.

"""

OK_VAL = 0
IS_ALIVE = 1
IS_DEAD = 0
NO_PING_DONE = IS_ALIVE
PING_HISTORY_FILE = "pingHistoryFile.py"

# nodes to ping.  only certain enstore nodes are connected to
# the private network
NODES_TO_PING = {"d0ensrv4" : ["fntt","adic2"],
                 "cdfensrv4" : ["fntt2.fnal.gov",],
                 "stkensrv4" : ["fntt","adic2"],
                 "fndapr" : ["pcqcd1","hppc"]
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

class PingHistory:

    def __init__(self, file):
        self.file = file
        self.pingHistory_d = {}
        self.raisedAlarm_d = {}
        if os.path.exists(file):
            importFile = string.replace(file, ".py", "")
            exec("import %s"%(importFile,))
            try:
                exec("self.pingHistory_d = %s.pingHistory_d"%(importFile,))
                exec("self.raisedAlarm_d = %s.raisedAlarm_d"%(importFile,))
            except AttributeError:
                # it is ok if there is no line in the file.
                pass

    def lastTimeAlive(self, node):
        return self.pingHistory_d.get(node, None)

    def setLastTimeAlive(self, node):
        # update the record of the last time the node was seen alive
        now = time.time()
        self.pingHistory_d[node] = now
        # remove any record of alarms that were raised.
        del self.raisedAlarm_d[node]

    def setLastTimeAliveToZero(self, node):
        # the node has not been seen alive.  record this.
        self.pingHistory_d[node] = 0.0

    def raiseAlarm(self, node, thisNode):
        # check if we should raise an alarm.  if we already have, do not raise
        # another one.
        if not self.raisedAlarm_d.has_key(node):
            # we did not raise an alarm yet, record that we will now
            self.raisedAlarm_d[node] = [thisNode, time.time()]
            print "raised alarm"
            msg = "No ping response from %s (pinged from %s)"%(node, thisNode)
            os.system(". /usr/local/etc/setups.sh;setup enstore;enstore alarm --raise --root-error='%s'"%(msg,))

    def write(self):
        fd = open(self.file, 'w')
        fd.write("pingHistory_d = %s\n"%(self.pingHistory_d,))
        fd.write("raisedAlarm_d = %s\n"%(self.raisedAlarm_d,))
        fd.close()

if __name__=="__main__":

    # determine what node we are running on.  only certain nodes are connected to
    # a private network
    thisNode = strip_domain(os.uname()[1])

    pingNode_l = NODES_TO_PING.get(thisNode, None)
    rtn = IS_ALIVE
    if pingNode_l:
        pingHistory = PingHistory(PING_HISTORY_FILE)
        for pingNode in pingNode_l:
            if ping(pingNode) == IS_DEAD:
                # only raise an alarm if the node has been unpingable for longer
                # than 10 minutes
                rtn = IS_DEAD
                now = time.time()
                lastTimeAlive = pingHistory.lastTimeAlive(pingNode)
                if not lastTimeAlive == None:
                    # it was a valid time, see if more than 10 minutes ago
                    if now - lastTimeAlive > 600:
                        # raise an alarm
                        pingHistory.raiseAlarm(pingNode, thisNode)
                else:
                    # there was no known last time alive. and the node is still not alive.
                    pingHistory.setLastTimeAliveToZero(pingNode)
            else:
                # node is still alive
                pingHistory.setLastTimeAlive(pingNode)
        else:
            print rtn
            
        # update the ping history
        pingHistory.write()
    else:
        print NO_PING_DONE

