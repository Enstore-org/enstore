#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#

import enstore

def do_work():
    # user mode
    mode = 0

    en = enstore.Enstore(mode)
    return en.do_work()

if __name__ == "__main__" :

    sys.exit(do_work())
