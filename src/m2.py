#!/usr/bin/env python
"""
m2.py -- collection of mammoth-2 related routines

It contains the following routines:

dump_code(device, path=None, sendto=[], notify=[], comment=None)

	calling m2probe to dump the internal code of a m2 drive

	device: the device that connects to a mammoth-2 drive
	path:	path to the dump file, default to be CWD
	sendto:	a list of email addresses to send the code dump to
	notify:	a list of email addresses to send the notification to
	comment:extra message that goes into notification
		This is useful to pass enstore mover information

	return error message or None

	Most of the errors are handled by m2probe

    example:

	dump_code('/dev/rmt/tps3d0n', '/tmp', ['MartinD@Exabyte.COM'],
		['enstore-admin@fnal.gov'], 'This is mover XXX')

----

This can also be invoked interactively which is useful to dump a local
drive without going through enstore system ...

m2.py dump device [path [snedto notify comment]]

Examples:

m2.py dump /dev/rmt/tps3d0n
m2.py dump /dev/rmt/tps3d0n /tmp
m2.py dump /dev/rmt/tps3d0n /tmp MartinD@Exabyte.COM enstore_admin@fnal.gov 'mover XXX'

Note:

1. To avoid the ambiguity, if one of sendto, notify and comment is specified,
   all of them should be specified.
2. If sendto or notify is a list, all e-mail address should be in a space
   delimited string such as "jon@fnal.gov don@fnal.gov"
"""
from __future__ import print_function

import os
import string
import enmail
import time
import getpass
import sys

# dump_code(device, path=None, sendto=[], notify=[], comment=None)
#	-- calling m2probe to dump the internal code of a m2 drive


def dump_code(device, path=None, sendto=None, notify=None, comment=None):
    """
    dump_code(device, path=None, sendto=[], notify=[], comment=None)

    calling m2probe to dump the internal code of a m2 drive

    device: the device that connects to a mammoth-2 drive
    path:	path to the dump file, default to be CWD
    sendto:	a list of email addresses to send the code dump to
    notify:	a list of email addresses to send the notification to
    comment:extra message that goes into notification
            This is useful to pass enstore mover information

    return error message or None

    Most of the errors are handled by m2probe

    example:

    dump_code('/dev/rmt/tps3d0n', '/tmp', ['MartinD@Exabyte.COM'],
            ['enstore-admin@fnal.gov'], 'This is mover XXX')
    """

    # make sure m2probe exists
    if os.access('m2probe', os.X_OK):
        return 'can not find m2probe'

    # use prefix to fake the path
    if path:
        prefix = '-p ' + os.path.join(path, 'Fermilab')
    else:
        prefix = ''

    cmd = "m2probe -d %s %s" % (prefix, device)

    # parse m2probe's output for file name and status
    l = os.popen(cmd).readlines()
    res = string.split(l[-1], "dumped to")
    status = string.join(l, '')  # use m2probe outout as status
    if len(res) != 2:		# something is wrong
        return "code dumping failed:\n" + status

    # get the file name
    f = string.strip(res[-1])

    # figure out who am I and from where
    from_add = getpass.getuser() + '@' + os.uname()[1]
    subject = "Automatic M2 dump taken at " + time.ctime(time.time())

    error_msg = None

    # send it to some one?
    if sendto:
        mesg = "This is an automatically generated M2 dump by an enstore mover\n\n" + status
        res = enmail.mail_bin(from_add, sendto, subject, f, mesg)
        if res:
            error_msg = 'On sending binary\n' + res

    if notify:
        mesg = "A M2 dump is taken by " + from_add + "\n\n" + status
        if comment:
            mesg = mesg + "\n\n" + comment
        if sendto:
            if isinstance(sendto, type([])):
                to_addresses = string.joinfields(sendto, ', ')
            else:
                to_addresses = sendto
            mesg = mesg + "\n\nThe dump file has been sent to " + to_addresses
        res = enmail.mail(from_add, notify, subject, mesg)

        if res:
            if error_msg:
                error_msg = error_msg + '\n\nOn sending notification:\n' + res
            else:
                error_msg = 'On sending notification:\n' + res

    return error_msg

# usage() -- help for interactive usage


def usage():
    print(
        "usage: %s dump device [path [snedto notifify comment]]" %
        (sys.argv[0]))
    print()
    print("examples:")
    print("\t%s dump /dev/rmt/tps3d0n" % sys.argv[0])
    print("\t%s dump /dev/rmt/tps3d0n /tmp" % sys.argv[0])
    print(
        "\t%s dump /dev/rmt/tps3d0n /tmp MartinD@Exabyte.COM enstore_admin@fnal.gov 'mover XXX'" %
        sys.argv[0])


# interactive invocation:
#
# m2.py dump device [path [snedto notify comment]]
#

if __name__ == "__main__":
    argc = len(sys.argv)
    if argc < 2:
        usage()
        sys.exit(0)
    if sys.argv[1] == 'dump':
        if argc == 3:
            res = dump_code(sys.argv[2])
            if res:
                print(res)
        if argc == 4:
            res = dump_code(sys.argv[2], sys.argv[3])
            if res:
                print(res)
        elif argc == 7:
            res = dump_code(sys.argv[2], sys.argv[3],
                            sys.argv[4], sys.argv[5], sys.argv[6])
            if res:
                print(res)
        else:
            usage()
    else:
        usage()
