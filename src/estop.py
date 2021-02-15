#!/usr/bin/env python

###############################################################################
# src/$RCSfile$   $Revision$
#
# in order to add support for a new 'client', add a section to the following
# structures -
#             import the appropriate file
#             server_functions

# system import
from __future__ import print_function
import os
import sys

# enstore imports
import generic_client


def get_farmlet(default):
    if len(sys.argv) > 1:
        return sys.argv[1]
    else:
        return default


def do_rgang_command(fdefault, command):
    farmlet = get_farmlet(fdefault)
    print('rgang %s \"%s\"' % (farmlet, command))
    return os.system('rgang %s \"%s\"' % (farmlet, command))


class EstopInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        self.name = "ESTART"
        self.farmlet = None

        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.estop_options)

    parameters = ["<farmlet>"]
    estop_options = {}

    # parse the options like normal but make sure we have other args
    def parse_options(self):

        generic_client.GenericClientInterface.parse_options(self)

        if self.args:
            self.farmlet = self.args[0]
        else:
            print("Farmlet not specified.")
            sys.exit(1)


def do_work(intf):

    cmd = "(source /usr/local/etc/setups.sh; setup enstore; enstore stop) " \
          " >&1- 2>&- <&- &"
    do_rgang_command("enstore", cmd)


if __name__ == '__main__':

    intf = EstopInterface(user_mode=0)

    do_work(intf)
