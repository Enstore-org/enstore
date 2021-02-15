#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#

import encp
import delete_at_exit


if __name__ == "__main__":

    delete_at_exit.quit(encp.start(0))  # 0 means admin
