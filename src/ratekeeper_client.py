#system imports
import sys

#enstore imports
import Trace
import generic_client
import configuration_client
import udp_client

MY_NAME = "RATEKEEPER_CLIENT"
MY_SERVER = "ratekeeper"

class RatekeeperClient(generic_client.GenericClient):

    def __init__(self, csc, server_address):
        self.u = udp_client.UDPClient()
        self.timeout = 10
        self.ratekeeper_addr = server_address
        generic_client.GenericClient.__init__(self, csc, MY_NAME)

    # send Active Monitor probe request
    def send_ticket (self, ticket):
        x = self.u.send( ticket, self.ratekeeper_addr, self.timeout, 10 )
        return x


#class RatekeeperClientInterface(generic_client.GenericClientInterface):
#
#    def __init__(self, flag = 1, opts = []):
#        self.name = "ratekeeper"
#        self.alive_rcv_timeout = 10 #Required here
#        self.alive_retries = 3      #Required here
#	generic_client.GenericClientInterface.__init__(self)
#
#    # define the command line options that are valid
#    def options(self):
#        return self.help_options() + self.alive_options()

class RatekeeperClientInterface(generic_client.GenericClientInterface):
    def __init__(self, args=sys.argv, user_mode=1):
        self.name = "ratekeeper"
        self.alive_rcv_timeout = 10 #Required here
        self.alive_retries = 3      #Required here
	generic_client.GenericClientInterface.__init__(self)

    def valid_dictionaries(self):
        return (self.help_options, self.alive_options)

    # parse the options like normal but make sure we have other args
    def parse_options(self):

        generic_client.GenericClientInterface.parse_options(self)

        #if getattr(self, "help", None):
        #    self.print_help()
        #
        #if getattr(self, "usage", None):
        #    self.print_usage()

# we need this in order to be called by the enstore.py code
def do_work(intf):
    csc = configuration_client.ConfigurationClient((intf.config_host,
                                                    intf.config_port))
    ratekeep = csc.get('active_monitor')
    ratekeeper_host = ratekeep.get('host','MISSING')
    ratekeeper_port = ratekeep.get('port','MISSING')
    
    rc = RatekeeperClient((intf.config_host, intf.config_port),
                          (ratekeeper_host, ratekeeper_port))

    reply = rc.handle_generic_commands(intf.name, intf)


if __name__ == "__main__":

    intf = RatekeeperClientInterface()
    
    Trace.init(MY_NAME)
    Trace.trace( 6, 'msc called with args: %s'%(sys.argv,) )

    do_work(intf)
