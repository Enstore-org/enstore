#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import string
import types
import os
import traceback
import pprint

# enstore imports
import dispatching_worker
import generic_server
import interface
import Trace
import e_errors
import hostaddr
import callback

MY_NAME = "Monitor_Server"

class MonitorServer(dispatching_worker.DispatchingWorker):

    def __init__(self):
	self.print_id="CONFIG_DICT"
        self.serverlist = {}


class ConfigurationServer(ConfigurationDict, generic_server.GenericServer):

    def __init__(self, csc, configfile=interface.default_file()):
	self.running = 0
	self.print_id = MY_NAME
        Trace.trace(10,
            "Instantiating Configuration Server at %s %s using config file %s"
            %(csc[0], csc[1], configfile))

        # default socket initialization - ConfigurationDict handles requests
        dispatching_worker.DispatchingWorker.__init__(self, csc)

	self.running = 1

        # always nice to let the user see what she has
        Trace.trace(10, repr(self.__dict__))

class MonitorServerInterface(generic_server.GenericServerInterface):

    def __init__(self):
      
    generic_server.GenericServerInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return generic_server.GenericServerInterface.options(self)


if __name__ == "__main__":
    Trace.init(MY_NAME)
    Trace.trace( 6, "called args="+repr(sys.argv) )
    import sys

    # get the interface
    intf = MonitorServerInterface()

    # get a monitor server
    ms = MonitorServer((intf.config_host, intf.config_port))

    while 1:
        try:
            Trace.trace(6,"Monitor Server (re)starting")
            ms.serve_forever()
	except SystemExit, exit_code:
	    sys.exit(exit_code)
        except:
	    ms.serve_forever_error(MY_NAME)
            continue

    Trace.trace(6,"Monitor Server finished (impossible)")






