###############################################################################
# src/$RCSfile$   $Revision$
#
#system imports
import pprint
import sys

#enstore imports
import Trace
import generic_cs
import e_errors


class GenericClient(generic_cs.GenericCS):

    # check on alive status
    def alive(self, rcv_timeout=0, tries=0):
        Trace.trace(10,'{alive')
        x = self.send({'work':'alive'},rcv_timeout,tries)
        Trace.trace(10,'}alive '+repr(x))
        return x

    # examine the final ticket to check for any errors
    def check_ticket(self, str, ticket):
        if ticket['status'][0] == e_errors.OK:
            if self.verbose:
                pprint.pprint(ticket)
            Trace.trace(1, str+" exit ok")
            sys.exit(0)
        else:
            print "BAD STATUS:",ticket['status']
            pprint.pprint(ticket)
            Trace.trace(0, str+" BAD STATUS - "+repr(ticket['status']))
            sys.exit(1)

    # cover ourselves just in case our sub class does not have a send
    def send(self, work, timeout=0, retry=0):
	pass