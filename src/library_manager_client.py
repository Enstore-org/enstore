import sys
import pprint
from configuration_client import *
from udp_client import UDPClient

class LibraryManagerClient :
    def __init__(self, configuration_client, library_name) :
        # we always need to be talking to our configuration server
        self.u = UDPClient()
        self.csc = configuration_client
        self.name = library_name

    def send (self, ticket) :
        # who's our library manger that we should send the ticket to?
        vticket = self.csc.get(self.name+".library_manager")
        # send user ticket and return answer back
        return self.u.send(ticket, (vticket['host'], vticket['port']) )


    def write_to_hsm(self, ticket) :
        return self.u.send(ticket)


    def read_from_hsm(self, ticket) :
        return self.u.send(ticket)


    def printwork(self):
        return self.send({"work" : "printwork"} )


if __name__ == "__main__" :
    import getopt
    import socket

    # defaults
    config_host = "localhost"
    #(config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_list = 0
    list = 0
    worklist = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port="\
               ,"config_list","printwork","list","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_list" :
            config_list = 1
        elif opt == "--printwork" :
            printwork = 1
        elif opt == "--list" :
            list = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options, "library"
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    # bomb out if we don't have a library
    if len(args) < 1 :
        print "python ",sys.argv[0], options, "library"
        print "   do not forget the '--' in front of each option"
        sys.exit(1)

    if config_list :
        print "Connecting to configuration server at ",config_host,config_port
    csc = configuration_client(config_host,config_port)

    lmc = LibraryManagerClient(csc,args[0])

    if  printwork:
        lmc.printwork()

    if list:
        #pprint.pprint(ticket)
        pass
