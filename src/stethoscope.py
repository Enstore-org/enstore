#!/usr/bin/env python

import select
import time
import sys
import string

import event_relay_client
import event_relay_messages
import enstore_erc_functions
import generic_client
import option

NINETY_SECONDS = 90
    

def do_real_work(intf):

    erc = event_relay_client.EventRelayClient()
    # we will get all of the info from the event relay.
    erc.start([event_relay_messages.ALL,])
    start = time.time()

    # event loop - wait for events for 90 seconds, we should
    # receive at least 1 alive from all entered servers. if
    # not raise an alarm.
    while 1:
        readable, junk, junk = select.select([erc.sock, 0], [], [], 5)
        if not readable:
            continue
        now = time.time()
        for fd in readable:
            if fd == 0:
                # move along, no more to see here
                erc.unsubscribe()
                erc.sock.close()
                return
            else:
                msg = enstore_erc_functions.read_erc(erc)
                if msg and msg.type == event_relay_messages.ALIVE:
                    # get the server
                    alive_server = string.split(msg.extra_info, " ")[2]
                    if alive_server in intf.servers:
                        intf.servers.remove(alive_server)
        if now - start > NINETY_SECONDS or intf.servers == []:
            # exit
            erc.unsubscribe()
            erc.sock.close()
            # see which servers have not sent an alive
            for server in intf.servers:
                print "did not get alive from %s"%(server,)
            return

class StethoscopeInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        self.servers = None
	generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    stethoscope_options = {}
    parameters = ["server[,server]"]

    def parse_options(self):
        generic_client.GenericClientInterface.parse_options(self)

        if (getattr(self, "help", 0) or getattr(self, "usage", 0)):
            pass
        elif len(self.args) < 1:
            self.print_usage("Expected server list parameter")
        else:
            self.servers = string.split(self.args[0], ",")

    def valid_dictionaries(self):
	return (self.help_options, self.stethoscope_options)


if __name__ == "__main__" :

    intf = StethoscopeInterface(user_mode=0)

    do_real_work(intf)
