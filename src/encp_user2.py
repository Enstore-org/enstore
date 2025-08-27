#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#

import encp
import delete_at_exit


if __name__ == "__main__":   # pragma: no cover

    delete_at_exit.delete_and_quit(encp.start(2))  #2 means dcahce
