#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#

import ensync


def do_work():
    # user mode
    mode = 1

    intf = ensync.EnsyncInterface(user_mode=mode)
    if intf:
        ensync.do_work(intf)


if __name__ == "__main__":

    do_work()
