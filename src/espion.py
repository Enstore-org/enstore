#!/usr/bin/env python

import select
import time
import sys

import event_relay_client
import event_relay_messages
import enstore_erc_functions
import generic_client
import option

TEN_MINUTES = 600

def do_real_work(intf):

    erc = event_relay_client.EventRelayClient()
    if intf.dump:
	# just ask the event relay to dump its brains
	erc.dump()
    elif intf.do_print:
	erc.do_print()
    elif intf.dont_print:
	erc.dont_print()
    else:
	# we will get all of the info from the event relay.
	erc.start([event_relay_messages.ALL,])
	start = time.time()

	# event loop - wait for events
	while 1:
	    readable, junk, junk = select.select([erc.sock, 0], [], [], 15)
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
		    if msg:
			print time.ctime(now), msg.type, msg.extra_info
	    if now - start > TEN_MINUTES:
		# resubscribe
		erc.subscribe()

class EspionInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
	self.dump = 0
	self.do_print = 0
	self.dont_print = 0
	generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    espion_options = {
	option.DUMP:{option.HELP_STRING:
		     "print (stdout) the connected clients and timeouts",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN,
		     },
	option.DO_PRINT:{option.HELP_STRING:
		     "turn on debug printing (stdout) in the event relay",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN,
		     },
	option.DONT_PRINT:{option.HELP_STRING:
		     "turn off debug printing (stdout) in the event relay",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN,
		     },
	}


    def valid_dictionaries(self):
	return (self.help_options, self.espion_options)


if __name__ == "__main__":

    # check if we were asked to send a dump message to the event relay
    intf = EspionInterface(user_mode=0)

    do_real_work(intf)
