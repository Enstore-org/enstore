import time
import errno
import configuration_client
import callback
import udp_client
import base_defaults
import client_defaults
import Trace

class LibraryManagerClient(base_defaults.BaseDefaults,\
                           client_defaults.ClientDefaults) :
    def __init__(self, csc=[],\
                 host=base_defaults.default_host(),\
                 port=base_defaults.default_port()) :
        # we always need to be talking to our configuration server
        self.name = ""
        self.config_list = 0
        self.dolist = 0
        self.dogetwork = 0
        self.doalive = 0
        configuration_client.set_csc(self, csc, host, port)
        self.u = udp_client.UDPClient()

    # define the command line options that are valid
    def options(self):
        return base_defaults.BaseDefaults.config_options(self) + \
               base_defaults.BaseDefaults.list_options(self)   + \
               ["config_list", "getwork", "alive"] +\
               base_defaults.BaseDefaults.options(self)

    #  define our specific help
    def help_line(self):
        return base_defaults.BaseDefaults.help_line(self)+" library"

    # parse the options like normal but make sure we have a library manager
    def parse_options(self):
        base_defaults.BaseDefaults.parse_options(self)
        # bomb out if we don't have a library
        if len(self.args) < 1 :
            self.print_help(),
            sys.exit(1)
        else:
            self.name = self.args[0]

    def send (self, ticket) :
        # who's our library manager that we should send the ticket to?
        lticket = self.csc.get(self.name)
        # send user ticket and return answer back
        return self.u.send(ticket, (lticket['host'], lticket['port']) )


    def write_to_hsm(self, ticket) :
        return self.send(ticket)


    def read_from_hsm(self, ticket) :
        return self.send(ticket)


    def getwork(self,list) :
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"               : "getwork",
                  "user_callback_port" : port,
                  "user_callback_host" : host,
                  "unique_id"          : time.time() }
        # send the work ticket to the library manager
        ticket = self.send(ticket)
        if ticket['status'] != "ok" :
            raise errno.errorcode[errno.EPROTO],"lmc.getwork: sending ticket"\
                  +repr(ticket)

        # We have placed our request in the system and now we have to wait.
        # All we  need to do is wait for the system to call us back,
        # and make sure that is it calling _us_ back, and not some sort of old
        # call-back to this very same port. It is dicey to time out, as it
        # is probably legitimate to wait for hours....
        while 1 :
            control_socket, address = listen_socket.accept()
            new_ticket = callback.read_tcp_socket(control_socket, "library "+\
                                         "manager getwork,  mover call back")
            if ticket["unique_id"] == new_ticket["unique_id"] :
                listen_socket.close()
                break
            else:
                print ("lmc.getwork: imposter called us back, trying again")
                control_socket.close()
        ticket = new_ticket
        if ticket["status"] != "ok" :
            raise errno.errorcode[errno.EPROTO],"lmc.getwork: "\
                  +"1st (pre-work-read) library manager callback on socket "\
                  +repr(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]

        # If the system has called us back with our own  unique id, call back
        # the library manager on the library manager's port and read the
        # work queues on that port.
        data_path_socket = callback.library_manager_callback_socket(ticket)
        worklist = callback.read_tcp_socket(data_path_socket,"library "+\
                                    "manager getwork, reading worklist")
        data_path_socket.close()

        # Work has been read - wait for final dialog with library manager.
        done_ticket = callback.read_tcp_socket(control_socket, "library "+\
                                      "manager getwork, mover final dialog")
        control_socket.close()
        if done_ticket["status"] != "ok" :
            raise errno.errorcode[errno.EPROTO],"lmc.getwork: "\
                  +"2nd (post-work-read) library manger callback on socket "\
                  +repr(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
        return worklist

if __name__ == "__main__" :
    Trace.init("libm cli")
    import sys
    import pprint

    # fill in defaults
    lmc = LibraryManagerClient()

    # see what the user has specified. bomb out if wrong options specified
    lmc.parse_options()
    lmc.csc.connect()

    if lmc.doalive:
        ticket = lmc.alive()
    elif  lmc.dogetwork:
        ticket = lmc.getwork(list)


    if ticket['status'] == 'ok' :
        if lmc.dolist:
            pprint.pprint(ticket)
        sys.exit(0)

    else :
        print "BAD STATUS:",ticket['status']
        pprint.pprint(ticket)
        sys.exit(1)
