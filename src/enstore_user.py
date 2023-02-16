#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#

try:
	#Sometime after python 2.2.3 but before 2.4.3 this python library was
	# added.  It causes a problem with freeze if not imported explicitly.
        # The problem being that the executable builds, but when run throws an
        # InputError stating that the module _strptime can not be found.
        # In reality _strptime is a python library, not a module.
	import _strptime
	__pychecker__ = "no-import"
except ImportError:
	pass

import enstore

def do_work():
    # user mode
    mode = 1

    intf = enstore.EnstoreInterface(mode)
    if intf.error is None:
	en = enstore.Enstore(intf)
	en.do_work()

if __name__ == "__main__":   # pragma: no cover

    do_work()
