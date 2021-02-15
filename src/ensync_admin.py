#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#

import ensync


def do_work():
    # admin mode
    mode = 0

    intf = ensync.EnsyncInterface(user_mode=mode)
    if intf:
        ensync.do_work(intf)


if __name__ == "__main__":

    do_work()
