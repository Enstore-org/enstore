###############################################################################
# src/$RCSfile$   $Revision$
#
#########################################################################
#                                                                       #
# Media Changer client.                                                 #
# Media Changer access methods                                          #
#                                                                       #
#########################################################################

# system imports
import pdb

#enstore imports
import configuration_client 
import udp_client
import interface
import generic_client
import Trace

class MediaLoaderClient(generic_client.GenericClient):
    def __init__(self, csc=0, list=0, name="", host=interface.default_host(), \
                 port=interface.default_port()):
        self.media_changer=name
        configuration_client.set_csc(self, csc, host, port, list)
        self.u = udp_client.UDPClient()

    # send the request to the Media Loader server and then send answer to user
    def send (self, ticket) :
        vticket = self.csc.get(self.media_changer)
        return  self.u.send(ticket, (vticket['host'], vticket['port']))

    def loadvol(self, external_label, drive):
        ticket = {'work'           : 'loadvol',
                  'external_label' : external_label,
                  'drive_id'       : drive
                  }
        return self.send(ticket)

    def unloadvol(self, volume, drive):
        ticket = {'work'           : 'unloadvol',
                  'external_label' : volume,
                  'drive_id'       : drive
                  }
        return self.send(ticket)

class MediaLoaderClientInterface(interface.Interface):
    def __init__(self):
        self.config_list = 0
        self.config_file = ""
        self.alive = 0
        self.media_changer = ""
        self.volume = 0
        self.drive = 0
        interface.Interface.__init__(self)

        # parse the options
        self.parse_options()

    # define the command line options that are valid
    def options(self):
        return self.config_options() + self.list_options() +\
               ["config_list", "config_file=", "alive"] +\
               self.help_options()

    #  define our specific help
    def help_line(self):
        return interface.Interface.help_line(self)+" media_changer volume drive"

    # parse the options like normal but make sure we have other args
    def parse_options(self):
        interface.Interface.parse_options(self)
        if len(self.args) < 1 :
            self.print_help()
            sys.exit(1)
        else:
            self.media_changer = self.args[0]

        if self.alive == 0:
            # bomb out if we number of arguments is wrong
            if len(self.args) < 3 :
                self.print_help()
                sys.exit(1)
            else:
                self.volume = self.args[1]
                self.drive = self.args[2]


if __name__ == "__main__" :
    Trace.init("medch cli")
    import sys
    import pprint

    # fill in the interface
    intf = MediaLoaderClientInterface()

    # get a media changer client
    mlc = MediaLoaderClient(0, intf.config_list, intf.media_changer, \
                            intf.config_host, intf.config_port)

    if intf.alive:
        ticket = mlc.alive()
    else:
        ticket = mlc.unloadvol(intf.volume, intf.drive)
        print 'unload returned:' + ticket['status']

    if ticket['status'] == 'ok' :
        if intf.list:
            pprint.pprint(ticket)
        sys.exit(0)
    else :
        print "BAD STATUS:",ticket['status']
        pprint.pprint(ticket)
        sys.exit(1)
