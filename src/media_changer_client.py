###############################################################################
# src/$RCSfile$   $Revision$
#
#########################################################################
#                                                                       #
# Media Changer client.                                                 #
# Media Changer access methods                                          #
#                                                                       #
#########################################################################

#system imports
import sys

#enstore imports
import configuration_client
import udp_client
import interface
import generic_client
import generic_cs
import Trace

class MediaChangerClient(generic_client.GenericClient):
    def __init__(self, csc=0, verbose=0, name="", \
                 host=interface.default_host(), \
                 port=interface.default_port()):
	self.print_id = "MEDCHC"
        self.media_changer=name
	self.verbose = verbose
        configuration_client.set_csc(self, csc, host, port, verbose)
        self.u = udp_client.UDPClient()
        ticket = self.csc.get(name)
	try:
            self.print_id = ticket['logname']
        except:
            pass

    # send the request to the Media Changer server and then send answer to user
    #      rcv_timeout is set to 300sec, the STK mnt/dismnt time is ~35 sec.   This
    #      should really be a function of which media changer we are talking to.
    # If tries is set to 0, then we only try once -- which we should never do
    # with udp.
    def send (self, ticket, rcv_timeout=300, tries=10) :
        vticket = self.csc.get(self.media_changer)
        return  self.u.send(ticket, (vticket['hostip'], vticket['port']), rcv_timeout, tries)

    def loadvol(self, vol_ticket, drive):
        ticket = {'work'           : 'loadvol',
                  'vol_ticket' : vol_ticket,
                  'drive_id'       : drive
                  }
        return self.send(ticket)

    def unloadvol(self, vol_ticket, drive):
        ticket = {'work'           : 'unloadvol',
                  'vol_ticket' : vol_ticket,
                  'drive_id'       : drive
                  }
        return self.send(ticket)

    def MaxWork(self, maxwork):
        ticket = {'work'           : 'maxwork',
                  'maxwork'        : maxwork
                 }
        return self.send(ticket)

    def GetWork(self):
        ticket = {'work'           : 'getwork'
                 }
        return self.send(ticket)

class MediaChangerClientInterface(generic_client.GenericClientInterface):
    def __init__(self):
        self.config_file = ""
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.media_changer = ""
        self.getwork=0
        self.maxwork=-1
        self.volume = 0
        self.drive = 0
        generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return self.client_options()+\
               ["config_file=","maxwork=","getwork"]

    #  define our specific help
    def parameters(self):
        return "media_changer"

    # parse the options like normal but make sure we have other args
    def parse_options(self):
        interface.Interface.parse_options(self)
        if len(self.args) < 1 :
	    self.missing_parameter("media_changer")
            self.print_help()
            sys.exit(1)
        else:
            self.media_changer = self.args[0]
        if (self.alive == 0) and (self.verbose == 0) and (self.maxwork==-1) and (self.getwork==0):
            # bomb out if we number of arguments is wrong
            self.print_help()

    # print out our extended help
    def print_help(self):
        interface.Interface.print_help(self)
        generic_cs.enprint("        --maxwork=N        Max simultaneous operations allowed (may be 0)")
        generic_cs.enprint("        --getwork          List oprations in progress")
        
if __name__ == "__main__" :
    Trace.init("medch cli")
    Trace.trace(1,"mcc called with args "+repr(sys.argv))

    # fill in the interface
    intf = MediaChangerClientInterface()

    # get a media changer client
    mcc = MediaChangerClient(0, intf.verbose, intf.media_changer, \
                            intf.config_host, intf.config_port)

    if intf.alive:
        ticket = mcc.alive(intf.alive_rcv_timeout,intf.alive_retries)
	msg_id = generic_cs.ALIVE
    elif intf.verbose:
        ticket = mcc.set_verbose(intf.verbose, intf.alive_rcv_timeout,\
	                         intf.alive_retries)
	msg_id = generic_cs.CLIENT
    elif intf.maxwork  >= 0:
        ticket=mcc.MaxWork(intf.maxwork)
	msg_id = generic_cs.CLIENT
    elif intf.getwork:
        ticket=mcc.GetWork()
        generic_cs.enprint(ticket['worklist'], generic_cs.PRETTY_PRINT)
	msg_id = generic_cs.CLIENT
    else:
        intf.print_help()
        sys.exit(0)

    del mcc.csc.u
    del mcc.u		# del now, otherwise get name exception (just for python v1.5???)

    mcc.check_ticket(ticket, msg_id)
