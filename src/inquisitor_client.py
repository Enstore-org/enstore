# system imports
#
import time
import string

# enstore imports
import configuration_client
import generic_client
import backup_client
import udp_client
import callback
import interface
import Trace
import e_errors

class Inquisitor(generic_client.GenericClient):

    def __init__(self, csc=0, list=0, host=interface.default_host(), \
                 port=interface.default_port()):
        # we always need to be talking to our configuration server
        Trace.trace(10,'{__init__')
        configuration_client.set_csc(self, csc, host, port, list)
        self.u = udp_client.UDPClient()
        Trace.trace(10,'}__init')

    def send (self, ticket):
        Trace.trace(12,"{send"+repr(ticket))
        # who's our inquisitor server that we should send the ticket to?
        vticket = self.csc.get("inquisitor")
        # send user ticket and return answer back
        Trace.trace(12,"send addr="+repr((vticket['hostip'], vticket['port'])))
        s = self.u.send(ticket, (vticket['hostip'], vticket['port']) )
        Trace.trace(12,"}send"+repr(s))
        return s

    def update (self):
	Trace.trace(16,"{update")
	# tell the inquisitor to update the enstore system status info
	s = self.send({"work"       : "update" } )
        Trace.trace(16,"}update")
	return s

    def set_timeout (self, tout):
	Trace.trace(16,"{set_timeout")
	# tell the inquisitor to reset the timeout between gathering stats
	s = self.send({"work"       : "set_timeout" ,\
	               "timeout"    : tout } )
        Trace.trace(16,"}set_timeout")
	return s

    def get_timeout (self):
	Trace.trace(16,"{get_timeout")
	# tell the inquisitor to return the timeout between gathering stats
	s = self.send({"work"       : "get_timeout" } )
        Trace.trace(16,"}get_timeout")
	return s

class InquisitorClientInterface(interface.Interface):

    def __init__(self):
        Trace.trace(10,'{iqc.__init__')
        # fill in the defaults for the possible options
        self.config_list = 0
	self.update = 0
	self.timeout = 0
	self.get_timeout = 0
        self.alive = 0
        interface.Interface.__init__(self)

        # now parse the options
        self.parse_options()
        Trace.trace(10,'}iqc.__init')

    # define the command line options that are valid
    def options(self):
        Trace.trace(16,"{}options")
        return self.config_options()+self.list_options()  +\
               ["config_list","timeout=","get_timeout", "update","alive",""] +\
               self.help_options()



if __name__ == "__main__" :
    import sys
    import pprint
    Trace.init("IQ client")
    Trace.trace(1,"iqc called with args "+repr(sys.argv))

    # fill in interface
    intf = InquisitorClientInterface()

    # now get an inquisitor client
    iqc = Inquisitor(0, intf.config_list, intf.config_host, \
                          intf.config_port)

    if intf.alive:
        ticket = iqc.alive()

    elif intf.update:
        ticket = iqc.update()

    elif intf.timeout:
        ticket = iqc.set_timeout(intf.timeout)

    elif intf.get_timeout:
        ticket = iqc.get_timeout()

    del iqc.csc.u
    del iqc.u           # del now, otherwise get name exception (just for python v1.5???)

    if ticket['status'][0] == e_errors.OK:
        if intf.list:
            pprint.pprint(ticket)
        Trace.trace(1,"iqc exit ok")
        sys.exit(0)
    else:
        print "BAD STATUS:",ticket['status']
        pprint.pprint(ticket)
        Trace.trace(0,"iqc BAD STATUS - "+repr(ticket['status']))
        sys.exit(1)

