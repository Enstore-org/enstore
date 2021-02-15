#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#

import enmv


def do_work():
    # user mode
    mode = 1

    intf = enmv.EnmvInterface(user_mode=mode)
    if intf:
        enmv.do_work(intf)


if __name__ == "__main__":

    do_work()
