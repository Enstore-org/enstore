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

class EnstoreRestartInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        self.name = "START"
        self.just = None

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

        if self.just == server_name or self.just == None:
            return 1

        return 0

    complete_names = [
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
        ]
        

    start_options = {
        option.JUST:{option.HELP_STRING:"specify single server",
                     option.VALUE_NAME:"just",
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_LABEL:"server name",                     
		     option.USER_LEVEL:option.ADMIN,
                     },
        }

def do_work(intf):

    enstore_start.check_user()

    if intf.just:
        os.system("enstore stop --just %s" % intf.just)
        os.system("enstore start --nocheck --just %s" % intf.just)
    else:
        os.system("enstore stop")
        os.system("enstore start --nocheck")


if __name__ == '__main__':

    intf = EnstoreRestartInterface(user_mode=0)

    do_work(intf)
