#########################################################################
#                                                                       #
# Media Changer client.                                                 #
# Media Changer access methods                                          #
#  $Id$  #
#                                                                       #
#########################################################################
import pdb
from configuration_client import configuration_client, set_csc
from udp_client import UDPClient
from base_defaults import default_host, default_port, BaseDefaults
from client_defaults import ClientDefaults

class MediaLoaderClient(BaseDefaults, ClientDefaults):
    def __init__(self, csc=[], host=default_host(), port=default_port()) :
        self.media_changer = ""
        self.volume = 0
        self.drive = 0
        self.config_file = ""
        self.config_list = 0
        self.dolist = 0
        self.doalive = 0
        set_csc(self, csc, host, port)
        self.u = UDPClient()

    # define the command line options that are valid
    def options(self):
        return BaseDefaults.config_options(self) + \
               BaseDefaults.list_options(self)   + \
               ["config_list", "config_file=", "alive"] +\
               BaseDefaults.options(self)

    #  define our specific help
    def help_line(self):
        print BaseDefaults.help_line(self), "media_changer volume drive",

    def set_mc(self, mc) :
        self.media_changer = mc

    # parse the options like normal but make sure we have a enough args
    def parse_options(self):
        BaseDefaults.parse_options(self)
        # bomb out if the number of arguments is wrong
        if len(self.args) < 1 :
            self.print_help()
            sys.exit(1)
        else:
            self.set_mc(self.args[0])

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

if __name__ == "__main__" :
    import sys
    import pprint

    # fill in defaults
    mlc = MediaLoaderClient()

    # see what the user has specified. bomb out if wrong options specified
    mlc.parse_options()
    mlc.csc.connect()

    if mlc.doalive:
        ticket = mlc.alive()
    else:
        # bomb out if we number of arguments is wron
        if len(mlc.args) < 3 :
            mlc.print_help()
            sys.exit(1)
        else:
            mlc.volume = mlc.args[1]
            mlc.drive = mlc.args[2]

        ticket = mlc.unloadvol(self.volume, self.drive)
        print 'unload returned:' + ticket['status']

    if ticket['status'] == 'ok' :
        if mlc.dolist:
            pprint.pprint(ticket)
        sys.exit(0)
    else :
        print "BAD STATUS:",ticket['status']
        pprint.pprint(ticket)
        sys.exit(1)
