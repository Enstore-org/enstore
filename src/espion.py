#!/usr/bin/env python

import select
import time

import event_relay_client
import event_relay_messages
import enstore_functions

TEN_MINUTES = 600

def do_real_work():

    # we will get all of the info from the event relay.
    erc = event_relay_client.EventRelayClient()
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
		msg = enstore_functions.read_erc(erc)
		if msg:
		    print time.ctime(now), msg.type, msg.extra_info
	if now - start > TEN_MINUTES:
	    # resubscribe
	    erc.subscribe()


if __name__ == "__main__" :

    do_real_work()
