import os
import string
import sys

"""
this script will determine whether to alarm on how full a disk is.

the logic for determinig whether to alarm or not is highly convoluted
with respect to the node name, and so this script was created as
opposed to letting ngop execute the command below.  nodes being ok
means that no mail or alarm is generated
"""

OK_VAL = 0

# nodes that are ok if disk is > 95% full. if the list of
# file systems is blank (e.g. []), it means all file systems
# on the node.
FULL95 = {"cachen1a" : ["/diska"],
          "cachen2a" : ["/diska"],
          "stkendca3a" : ["/diska", "/diskb"],
          "stkendca4a" : ["/diska", "/diskb"],
          "stkendca5a" : ["/diska", "/diskb"],
          "cdfendca3" : ["/diska"],
          "cdfendca4" : ["/diska"],
          "cdfendca5" : ["/diska"],
          "d0endca3a" : ["/diska"],
          "fcdfdata008" : ["/export"],
          "fcdfdata009" : ["/export"],
          "fcdfdata010" : ["/export"],
          "fcdfdata011" : ["/export"],
          "fcdfdata012" : ["/export"],
          "ketchup" : ["/data1", "/data2"],
          "mustard" : ["/data1", "/data2"],
          "cmsdcdr1" : ["/", "/data"]}
FULL95_VAL = 95
FULL95_KEYS = FULL95.keys()

# nodes that are ok if disk is between 85% and 95% full
FULL85_95 = {}
FULL85_95_VAL = 85
FULL85_95_KEYS = FULL85_95.keys()

# all other nodes give the standard result,

def strip_domain(node):
    node_info = string.split(node, ".")
    return node_info[0]

if __name__=="__main__":

    if len(sys.argv) < 2:
        # we did not get a device to check
        print -1
        sys.exit(0)
    else:
        device = sys.argv[1]

    cmd = "if [ `df %s  2> /dev/null | grep -v Filesystem|wc -l` -eq 1 ];then echo `df %s  2> /dev/null | grep -v Filesystem|awk '{print $5}'`;else echo -1;fi"%(device, device)
    p = os.popen(cmd, 'r')
    device_status = p.readlines()
    p.close()
    device_status = int(string.replace(string.strip(device_status[0]), "%", ""))
    node = os.uname()[1]
    node = strip_domain(node)

    if node in FULL95_KEYS and (device in FULL95[node] or not FULL95[node]):
        if device_status < FULL95_VAL:
            print device_status
        else:
            print OK_VAL
    elif node in FULL85_95_KEYS and (device in FULL85_95[node] or not FULL85_95[node]):
        if device_status < FULL85_95_VAL:
            print device_status
        else:
            print OK_VAL
    else:
        print device_status
        
