#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#

import enstore

if __name__ == "__main__" :

    # user mode
    mode = 1

    en = enstore.Enstore(mode)
    en.do_work()
