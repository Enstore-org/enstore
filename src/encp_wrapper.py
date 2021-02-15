#!/usr/bin/env python
"""
a quick and not so dirty wrapper for encp so that multiple encps can be
instantiated in the same program, probably in different threads.
"""
from __future__ import print_function

from future.utils import raise_
import sys
import thread

import Trace
import file_utils
import e_errors


class Encp:
    def __init__(self, tid=None):
        import encp
        import get
        import put
        self.my_encp = encp
        self.my_get = get
        self.my_put = put
        self.tid = tid
        self.exit_status = -10
        self.err_msg = ""

    def __encp(self, argv, my_encp, my_interface, exe="encp"):
        self.exit_status = -10  # Reset this every time.
        self.err_msg = ""

        # Insert the command if it is not already there.
        CMD = exe
        if argv[0] != CMD:
            argv = [CMD] + argv

        # Grab logname to reset it after encp is done.
        logname = Trace.get_logname()

        try:
            intf = my_interface(argv, 0)
            intf.migration_or_duplication = 1  # Set true for performance.
            if self.tid:
                intf.include_thread_name = self.tid

            res = self.my_encp.do_work(intf, main=my_encp.main)
            if res is None:
                # return -10
                res = -10  # Same as initial value.

            self.err_msg = self.my_encp.err_msg[thread.get_ident()]
        except (KeyboardInterrupt, SystemExit):
            Trace.set_logname(logname)  # Reset the log file name.
            raise_(sys.exc_info()[0], sys.exc_info()[1],
                   sys.exc_info()[2])
        except BaseException:
            self.err_msg = str((str(sys.exc_info()[0]),
                                str(sys.exc_info()[1])))
            sys.stderr.write("%s\n" % self.err_msg)
            res = 1

        if res and not self.err_msg:
            Trace.log(e_errors.INFO,
                      "unexpected combination of values: exit_status[%s]: %s  err_msg[%s]: %s" % (type(res), res, type(self.err_msg), self.err_msg))

        # If we end up with encp being owned not by root at this
        # point, we need to set it back.
        file_utils.acquire_lock_euid_egid()
        try:
            file_utils.set_euid_egid(0, 0)
        except (KeyboardInterrupt, SystemExit):
            raise_(sys.exc_info()[0], sys.exc_info()[1],
                   sys.exc_info()[2])
        except BaseException:
            Trace.set_logname(logname)  # Reset the log file name.
            self.err_msg = str((str(sys.exc_info()[0]),
                                str(sys.exc_info()[1])))
            sys.stderr.write("%s\n" % self.err_msg)
            res = 1
        file_utils.release_lock_euid_egid()

        Trace.set_logname(logname)  # Reset the log file name.
        self.exit_status = res  # Return value if used in a thread.
        return res  # Return value if used directly.

    # encp(argv) -- argv is the same as the command line
    # eg. encp(["encp", "--verbose=4", "/pnfs/.../file", "file"])

    def encp(self, argv):
        return self.__encp(argv, self.my_encp,
                           self.my_encp.EncpInterface)

    # get(argv) -- argv is the same as the command line
    # eg. get(["get", "--verbose=4", "/pnfs/.../file", "file"])

    def get(self, argv):
        return self.__encp(argv, self.my_get,
                           self.my_get.GetInterface, exe="get")

    # put(argv) -- argv is the same as the command line
    # eg. get(["put", "--verbose=4", "/pnfs/.../file", "file"])
    def put(self, argv):
        return self.__encp(argv, self.my_put,
                           self.my_put.PutInterface, exe="put")


if __name__ == '__main__':
    test_encp = Encp()
    for i in sys.argv[1:]:
        print("copying", i, "...", end=' ')
        cmd = "encp --priority 0 --ignore-fair-share %s /dev/null" % (i)
        res = test_encp.encp(cmd.split())
        if res:
            print("FAILED")
        else:
            print("DONE")
