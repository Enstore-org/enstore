###############################################################################
# src/$RCSfile$   $Revision$
#
#########################################################################
#                                                                       #
# Mover client.                                                         #
# Mover access methods                                                  #
#                                                                       #
#########################################################################

# system imports

#enstore imports
import configuration_client
import udp_client
import interface
import generic_client
import generic_cs
import Trace
import e_errors

moverid = "MOVERC"

class MoverClient(generic_client.GenericClient):
    def __init__(self, csc=0, verbose=0, name="", \
                 host=interface.default_host(), \
                 port=interface.default_port()):
        self.mover=name
	self.verbose = verbose
        configuration_client.set_csc(self, csc, host, port, verbose)
        self.u = udp_client.UDPClient()

    def send (self, ticket, rcv_timeout=0, tries=0) :
        Trace.trace(12,"{send"+repr(ticket))
        vticket = self.csc.get(self.mover)
        s = self.u.send(ticket, (vticket['hostip'], vticket['port']), \
	                rcv_timeout, tries)
        Trace.trace(12,"}send"+repr(s))
	return s

class MoverClientInterface(interface.Interface):
    def __init__(self):
        self.alive = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.mover = ""
        interface.Interface.__init__(self)

        # parse the options
        self.parse_options()

    # define the command line options that are valid
    def options(self):
        return self.config_options() +\
               ["verbose="] +\
               self.alive_options()+self.help_options()

    #  define our specific help
    def help_line(self):
        return interface.Interface.help_line(self)+" mover"

    # parse the options like normal but make sure we have a mover name
    def parse_options(self):
        interface.Interface.parse_options(self)
        # bomb out if we don't have a mover
        if len(self.args) < 1 :
            self.print_help()
            sys.exit(1)
        else:
            self.mover = self.args[0]


if __name__ == "__main__" :
    import sys
    Trace.init("mover cli")
    Trace.trace(1,"movc called with args "+repr(sys.argv))

    # fill in the interface
    intf = MoverClientInterface()

    # get a mover client
    movc = MoverClient(0, intf.verbose, intf.mover, \
                      intf.config_host, intf.config_port)

    if intf.alive:
        ticket = movc.alive(intf.alive_rcv_timeout,intf.alive_retries)
	msg_id = generic_cs.ALIVE

    del movc.csc.u
    del movc.u		# del now, otherwise get name exception (just for python v1.5???)

    movc.check_ticket(ticket, msg_id, moverid)
