#!/usr/bin/env python

###############################################################################
# src/$RCSfile$
#
# start enstore
#
#
#

# system imports
#
import sys
import re
import os
import string
import errno
import socket
import pwd

import generic_client
import option
import enstore_start
import Trace

MY_NAME = "ENSTORE_RESTART"

class EnstoreRestartInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        self.name = "START"
        self.just = None
        self.all = 0 #False

        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.start_options)
    
    # parse the options like normal but make sure we have other args
    def parse_options(self):

        generic_client.GenericClientInterface.parse_options(self)

        self.completed_server_name()

    def completed_server_name(self):
        if self.just:
            count = 0
            for name in self.complete_names:
                if self.just == name[:len(self.just)]:
                    count = count + 1
                    hold = name
            if count == 1:
                self.just = hold

    def should_start(self, server_name):

        if self.all:
            return 1
        if self.just == server_name:
            return 1
        if self.just == None and server_name not in self.non_default_names:
            return 1

        return 0

    non_default_names = ["accounting_server", "monitor_server"]

    complete_names = [
        "accounting_server",
        "configuration_server",
        "event_relay",
        "log_server",
        "alarm_server",
        "volume_clerk",
        "file_clerk",
        "db_checkpoint",
        "db_deadlock",
        "inquisitor",
        "ratekeeper",
        "library",
        "media",
        "mover",
        "monitor_server",
        ]
        

    start_options = {
        option.JUST:{option.HELP_STRING:"specify single server",
                     option.VALUE_NAME:"just",
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_LABEL:"server name",                     
		     option.USER_LEVEL:option.ADMIN,
                     },
        option.ALL:{option.HELP_STRING:"specify all servers",
                     option.VALUE_USAGE:option.IGNORED,
                     option.DEFAULT_NAME:"all",
                     option.DEFAULT_TYPE:option.INTEGER,
		     option.USER_LEVEL:option.ADMIN,
                     },
        }

def do_work(intf):

    Trace.init(MY_NAME)

    enstore_start.check_user()

    start = "python %s/src/enstore_start.py" % (os.environ['ENSTORE_DIR'],)
    stop = "python %s/src/enstore_stop.py" % (os.environ['ENSTORE_DIR'],)

    if intf.all:
        os.system("%s --all" % (stop,))
        os.system("%s --nocheck --all" % (start,))
    elif intf.just:
        os.system("%s --just %s" % (stop, intf.just))
        os.system("%s --nocheck --just %s" % (start, intf.just))
    else:
        os.system("%s" % (stop,))
        os.system("%s --nocheck" % (start,))


if __name__ == '__main__':

    intf = EnstoreRestartInterface(user_mode=0)

    do_work(intf)
