###############################################################################
# $RCSfile$   $Revision$
#
# system imports
import errno
import time
import os
import traceback
import checksum
import sys
import socket
import signal
import string
if sys.version_info < (2, 2, 0):
    import fcntl, FCNTL
    fcntl.F_SETFD = FCNTL.F_SETFD
    fcntl.FD_CLOEXEC = FCNTL.FD_CLOEXEC
else: #FCNTL is depricated in python 2.2 and later.
    import fcntl
import copy
import types
import rexec

#enstore imports
import hostaddr
import cleanUDP
import udp_common
import Trace
import e_errors
import host_config

# Generic request response server class, for multiple connections
# Note that the get_request actually read the data from the socket

class UDPServer:
    
    def __init__(self, server_address, receive_timeout=60.):
        self.socket_type = socket.SOCK_DGRAM
        self.max_packet_size = 16384
        self.rcv_timeout = receive_timeout   # timeout for get_request in sec.
        self.address_family = socket.AF_INET

        try:
            #If an address was not specified.
            if type(server_address) != type(()) or \
               (type(server_address) != type(()) and len(server_address) != 2):
                ip,port,self.server_socket = udp_common.get_default_callback()
                self.server_address = (ip, port)
	    #If an address was specified.                
            elif (type(server_address[0]) == type("")
                 and type(server_address[1]) == type(0)):
                ip, port, self.server_socket = udp_common.get_callback(
                    server_address[0], server_address[1])
                self.server_address = (ip, port)
            #If an address was not specified.
            else:
                ip,port,self.server_socket = udp_common.get_default_callback()
                self.server_address = (ip, port)
	    
        except socket.error, msg:
            self.server_address = ("", 0)
            self.server_socket = None
	    Trace.log(e_errors.ERROR, str(msg))
            
        try:
            self.node_name, self.aliaslist, self.ipaddrlist = \
                socket.gethostbyname_ex(
                    socket.gethostbyaddr(self.server_address[0])[0])
            cf = host_config.find_config_file()
            if cf:
                cc = host_config.read_config_file(cf)
                for i in cc.get('interface', {}).keys():
                    self.ipaddrlist.append(cc['interface'][i]['ip'])
        except socket.error:
            self.node_name, self.aliaslist, self.ipaddrlist = \
                self.server_address[0], [], [self.server_address[0]]

        # used to recognize UDP retries
        self.request_dict = {}
        # keep requests in request dict for this many seconds
        #self.request_dict_ttl = 1800
        self.request_dict_ttl = 1000
        
        self.rexec = rexec.RExec()
        
        # set this socket to be closed in case of an exec
        fcntl.fcntl(self.server_socket.fileno(), fcntl.F_SETFD,
                    fcntl.FD_CLOEXEC)

    def __del__(self):
        self.server_socket.close()
        
    def r_eval(self, stuff):
        try:
            return self.rexec.r_eval(stuff)
        except:
            return None,None,None
    
    def purge_stale_entries(self):
        stale_time = time.time() - self.request_dict_ttl
        count = 0 
        for key, value in self.request_dict.items():
            if  value[2] < stale_time:
                del self.request_dict[key]
                count = count+1
        Trace.trace(20,"purge_stale_entries count=%d"%(count,))

    #Not used???
    def server_bind(self):
        """Called by constructor to bind the socket.

        May be overridden.

        """
        print "server_bind add %s"%(self.server_address,)
        Trace.trace(16,"server_bind add %s"%(self.server_address,))
        self.server_socket.bind(self.server_address)
    
    def get_message(self):
        # returns  (string, socket address)
        #      string is a stringified ticket, after CRC is removed
        # There are three cases:
        #   read from socket where crc is stripped and return address is valid
        #   read from pipe where there is no crc and no r.a.     
        #   time out where there is no string or r.a.

        request, addr = '',()
        r = [self.server_socket]

        rcv_timeout = self.rcv_timeout
        r, w, x, remaining_time = cleanUDP.Select(r, [], [], rcv_timeout)

        if not r + w:
            return ('',()) #timeout

        for fd in r:
            if fd == self.server_socket:

                req,addr = self.server_socket.recvfrom(self.max_packet_size,
						       self.rcv_timeout)

                request,inCRC = self.r_eval(req)
                if request == None:
                    return (request, addr)
                # calculate CRC
                crc = checksum.adler32(0L, request, len(request))
                if (crc != inCRC) :
                    Trace.log(e_errors.INFO, "BAD CRC request: "+request)
                    Trace.log(e_errors.INFO,
                              "CRC: "+repr(inCRC)+" calculated CRC: "+repr(crc))
                    request=None

        return (request, addr)

    def handle_timeout(self):
        # override this method for specific timeout hadling
        pass

    def fileno(self):
        """Return socket file number.

        Interface required by select().

        """
        return self.server_socket.fileno()

    # Process the  request that was (generally) sent from UDPClient.send
    def process_request(self):
        # ref udp_client.py (i.e. we may wish to have a udp_client method
        # to get this information)

        request, client_address = self.get_message()

        if not request:
            return None
        self.reply_address = client_address
        idn, number, ticket = self.r_eval(request)
        if idn == None or type(ticket) != type({}):
            Trace.log(e_errors.ERROR,"Malformed request from %s %s"%(client_address, request,))
            reply = (0L,{'status': (e_errors.MALFORMED, None)},None)
            self.server_socket.sendto(repr(reply), self.reply_address)
            return None
        self.client_number = number
        self.current_id = idn


        if self.request_dict.has_key(idn):

            # UDPClient resends messages if it doesn't get a response
            # from us, see it we've already handled this request earlier. We've
            # handled it if we have a record of it in our dict
            lst = self.request_dict[idn]
            if lst[0] == number:
                Trace.trace(6,"process_request "+repr(idn)+" already handled")
                self.reply_with_list(lst)
                return None

            # if the request number is smaller, then there has been a timing
            # race and we've already handled this as much as we are going to.
            elif number < lst[0]: 
                Trace.trace(6,"process_request "+repr(idn)+" old news")
                return None#old news, timing race....
        self.purge_stale_entries()
        return ticket


    # reply to sender with her number and ticket (which has status)
    # generally, the requested user function will send its response through
    # this function - this keeps the request numbers straight
    def reply_to_caller(self, ticket):
        reply = (self.client_number, ticket, time.time()) 
        self.reply_with_list(reply)          

    # if a different interface is needed to send the reply on then use it.
    def reply_to_caller_using_interface_ip(self, ticket, interface_ip):
        reply = (self.client_number, ticket, time.time()) 
	self.request_dict[self.current_id] = copy.deepcopy(reply)
	ip, port, send_socket = udp_common.get_callback(interface_ip)
        send_socket.sendto(repr(self.request_dict[self.current_id]),
			   self.reply_address)
	del send_socket

    # keep a copy of request to check for later udp retries of same
    # request and then send to the user
    def reply_with_list(self, list):
        self.request_dict[self.current_id] = copy.deepcopy(list)
        self.server_socket.sendto(repr(self.request_dict[self.current_id]),
				  self.reply_address)
        Trace.trace(6,"udp_server: request_dict %s"%(self.request_dict,))
        
    # for requests that are not handled serially reply_address, current_id,
    # and client_number number must be reset.  In the forking media changer
    # these are in the forked child and passed back to us
    def reply_with_address(self,ticket):
        self.reply_address = ticket["ra"][0] 
        self.client_number = ticket["ra"][1]
        self.current_id    = ticket["ra"][2]
        reply = (self.client_number, ticket, time.time())
        Trace.trace(19,"reply_with_address %s %s %s %s"%( 
            self.reply_address,
            self.current_id,
            self.client_number,
            reply))
        self.reply_to_caller(ticket)

if __name__ == "__main__":
    udpsrv =     UDPServer(("happy",7700), receive_timeout=60.)
    while 1:
        ticket = udpsrv.process_request()
        if ticket:
            print "Message %s"%(ticket,)
            udpsrv.reply_to_caller(ticket)
            break
    del(udpsrv)
    print "finished"
