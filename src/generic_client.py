###############################################################################
# src/$RCSfile$   $Revision$
#
#system imports
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
    def check_ticket(self, ticket, msg_id, client_id):
        if ticket['status'][0] == e_errors.OK:
	    generic_cs.enprint(ticket, generic_cs.NO_LOGGER, \
	                       generic_cs.PRETTY_PRINT | msg_id, \
	                       client_id, self.verbose)
            Trace.trace(1, client_id+" exit ok")
            sys.exit(0)
        else:
	    generic_cs.enprint("BAD STATUS: "+repr(ticket['status']), \
                               generic_cs.NO_LOGGER, \
	                       generic_cs.PRETTY_PRINT, client_id)
	    generic_cs.enprint(ticket, generic_cs.NO_LOGGER,\
	                       generic_cs.PRETTY_PRINT, client_id)
            Trace.trace(0, client_id+" BAD STATUS - "+repr(ticket['status']))
            sys.exit(1)

    # cover ourselves just in case our sub class does not have a send
    def send(self, work, timeout=0, retry=0):
	pass