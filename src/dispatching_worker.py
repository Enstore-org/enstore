from SocketServer import *

dict = {}

# Generic request response server class, for multiple connections
# This method overrides the process_request function in SocketServer.py
# Note that the UDPServer.get_request actually read the data from the socket

import socket

class DispatchingWorker:

    # Process the  request that was (generally) sent from UDPClient.send
    def process_request(self, request, client_address) :

        # the real info and work is in the ticket - get that
        exec ( "idn, number, ticket = " + request)
        self.reply_address = client_address
        self.client_number = number
        self.current_id = idn

        try :

            # UDPClient resends messages if it doesn't get a response from us
            # see it we've already handled this request earlier. We've
            # handled it if we have a record of it in our dict
            exec ("list = " + repr(dict[idn]))
            if list[0] == number :
                self.reply_with_list(list)
                return

            # if the request number is larger, then this request is new
            # and we need to process it
            elif list[0] < number :
                pass # new request, fall through

            # if the request number is smaller, then there has been a timing
            # race and we've already handled this as much as we are going to.
            else:
                return #old news, timing race....

        # on the very 1st request, we don't have anything to compare to
        except KeyError:
            pass # first request, fall through

        # look in the ticket and figure out what work user wants
        try :
            function = ticket["work"]
        except KeyError:
            ticket = {'status' : "cannot find requested function"}
            self.reply_to_caller(ticket)
            return

        # finally call the user function
        exec ("self." + function + "(ticket)")


    # reply to sender with her number and ticket (which has status)
    # generally, the requested user function will send its response through
    # this function - this keeps the request numbers straight
    def reply_to_caller(self, ticket) :
        reply = (self.client_number, ticket)
        self.reply_with_list(reply)

    # keep a copy of request to check for later udp retries of same
    # request and then send to the user
    def reply_with_list(self, list) :
        dict[self.current_id] = list
        self.socket.sendto(`list`, self.reply_address)
