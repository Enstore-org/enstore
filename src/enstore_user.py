#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#

import enstore

def do_work():
    # user mode
    mode = 1

    #en = enstore.Enstore(mode)
    #return en.do_work()
    intf = enstore.EnstoreInterface(mode)
    en = enstore.Enstore(intf)
    return en.do_work()

if __name__ == "__main__" :

    do_work()
