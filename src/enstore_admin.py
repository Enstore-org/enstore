#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#

import enstore

if __name__ == "__main__" :

    # user mode
    mode = 0

    en = enstore.Enstore(mode)
    en.do_work()
