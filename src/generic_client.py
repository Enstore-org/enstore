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
    def check_ticket(self, ticket, msg_id):
        if ticket['status'][0] == e_errors.OK:
	    self.enprint(ticket, generic_cs.PRETTY_PRINT|msg_id, self.verbose)
            Trace.trace(1,self.print_id+" exit ok")
            sys.exit(0)
        else:
	    self.enprint("BAD STATUS: "+repr(ticket['status']), \
	                 generic_cs.PRETTY_PRINT)
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            Trace.trace(0, self.print_id+" BAD STATUS - "+\
	                repr(ticket['status']))
            sys.exit(1)

    # cover ourselves just in case our sub class does not have a send
    def send(self, work, timeout=0, retry=0):
	pass

    # reset the verbosity in the server
    def set_verbose(self, verbosity, rcv_timeout=0, tries=0):
        Trace.trace(10,'{set_verbose (client)')
        x = self.send({'work':'set_verbose', 'verbose': verbosity}, \
	              rcv_timeout, tries)
        Trace.trace(10,'}set_verbose (client) '+repr(x))
        return x

    # tell the server to spill it's guts
    def dump(self, rcv_timeout=0, tries=0):
        Trace.trace(10,'{dump')
        x = self.send({'work':'dump'}, rcv_timeout, tries)
        Trace.trace(10,'}dump '+repr(x))
        return x
