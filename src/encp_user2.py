#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#

import encp

def do_work():
    # user2 mode
    mode = 2

    intf = encp.EncpInterface(user_mode=mode)
    if intf:
	encp.do_work(intf)

if __name__ == "__main__" :

    do_work()
