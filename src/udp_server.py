#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
#import errno
import time
#import os
#import traceback
import checksum
#import sys
import socket
#import signal
#import string
import fcntl
#if sys.version_info < (2, 2, 0):
#    import FCNTL #FCNTL is depricated in python 2.2 and later.
#    fcntl.F_SETFD = FCNTL.F_SETFD
#    fcntl.FD_CLOEXEC = FCNTL.FD_CLOEXEC
import copy
import types
import rexec

# enstore imports
#import hostaddr
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
        except (socket.error, socket.herror, socket.gaierror):
            self.node_name, self.aliaslist, self.ipaddrlist = \
                self.server_address[0], [], [self.server_address[0]]

        # used to recognize UDP retries
        self.request_dict = {}
        # keep requests in request dict for this many seconds
        #self.request_dict_ttl = 1800
        self.request_dict_ttl = 1000
        
        self.rexec = rexec.RExec()
        
        # set this socket to be closed in case of an exec
        if self.server_socket != None:
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

    def handle_timeout(self):
        # override this method for specific timeout hadling
        pass

    def fileno(self):
        """Return socket file number.

        Interface required by select().

        """
        return self.server_socket.fileno()

    def do_request(self):
        # ref udp_client.py (i.e. we may wish to have a udp_client method
        # to get this information)

        request, client_address = self.get_message()

        if not request:
            return None

        return self.process_request(request, client_address)

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
                    Trace.log(e_errors.INFO,
                              "BAD CRC request: %s " % (request,))
                    Trace.log(e_errors.INFO,
                              "CRC: %s calculated CRC: %s" %
                              (repr(inCRC), repr(crc)))
                              
                    request=None

        return (request, addr)

    # Process the  request that was (generally) sent from UDPClient.send
    def process_request(self, request, client_address):

        ### In some cases involving the media_changer, this function will
        ### process messages read from the child processes when
        ### DispatchingWorker.get_request() calls it.  The only major
        ### consecquence of this is that we don't put anything into
        ### ticket['r_a'].  If we do, these fake values of:
        ###    idn = 0
        ###    number = 0
        ###    client_address = ()
        ### will cause reply_with_address() and reply_to_caller() to do the
        ### wrong thing and not send back the reply.  This is becuase
        ### the media changer places these values into ticket['ra'],
        ### and we don't want to have two sets of competing information.
        ### Note: The use of 'r_a' was choosen internally for the
        ### udp_server becuase of the pre-existing use of 'ra' between
        ### the media_changer and udp_server.
       
        idn, number, ticket = self.r_eval(request)
        if idn == None or type(ticket) != type({}):
            Trace.log(e_errors.ERROR,
                      "Malformed request from %s %s" %
                      (client_address, request,))
            reply = (0L,{'status': (e_errors.MALFORMED, None)},None)
            self.server_socket.sendto(repr(reply), client_address)
            return None

        reply_address = client_address
        client_number = number
        current_id = idn
        #The following are not thread safe.
        self.reply_address = client_address
        self.client_number = number
        self.current_id = idn

        #The reason we need to include this information (at least
        # temporarily) is that for a multithreaded server it would
        # be possible for this function to process multiple requests
        # before repy_with_list() could be called from another thread(s).
        # In such a situation reply_to_caller() would reply with the
        # most recent request address and not to the one that made the request.
        if reply_address:
            ticket['r_a'] = (reply_address,
                             client_number,
                             current_id)

        if self.request_dict.has_key(idn):

            # UDPClient resends messages if it doesn't get a response
            # from us, see it we've already handled this request earlier. We've
            # handled it if we have a record of it in our dict
            lst = self.request_dict[idn]
            if lst[0] == number:
                Trace.trace(16,
                            "process_request %s from %s already handled" % \
                            (repr(idn), client_address))
                self.reply_with_list(lst, client_address, idn)
                return None

            # if the request number is smaller, then there has been a timing
            # race and we've already handled this as much as we are going to.
            elif number < lst[0]: 
                Trace.trace(16,
                            "process_request %s from %s old news" % \
                            (repr(idn), client_address))
                return None #old news, timing race....
        self.purge_stale_entries()

        return ticket


    # reply to sender with her number and ticket (which has status)
    # generally, the requested user function will send its response through
    # this function - this keeps the request numbers straight
    def reply_to_caller(self, ticket):
        if type(ticket) == types.DictType and ticket.get("r_a", None):
            reply_address = ticket["r_a"][0] 
            client_number = ticket["r_a"][1]
            current_id    = ticket["r_a"][2]

            del ticket['r_a']
        
        else:
            #Can we ever get here?  If we do it isn't thread safe.
            reply_address = self.reply_address
            client_number = self.client_number
            current_id    = self.current_id

        reply = (client_number, ticket, time.time())
        self.reply_with_list(reply, reply_address, current_id)

    # if a different interface is needed to send the reply on then use it.
    def reply_to_caller_using_interface_ip(self, ticket, interface_ip):
        if type(ticket) == types.DictType and ticket.get("r_a", None):
            reply_address = ticket["r_a"][0] 
            client_number = ticket["r_a"][1]
            current_id    = ticket["r_a"][2]

            del ticket['r_a']
        
        else:
            #Can we ever get here?  If we do it isn't thread safe.
            reply_address = self.reply_address
            client_number = self.client_number
            current_id    = self.current_id

        reply = (client_number, ticket, time.time())
        self.reply_with_list(reply, reply_address, current_id, interface_ip)
        
        #reply = (client_number, ticket, time.time()) 
	#self.request_dict[current_id] = copy.deepcopy(reply)
	#ip, port, send_socket = udp_common.get_callback(interface_ip)
        #send_socket.sendto(repr(self.request_dict[current_id]),
	#		   reply_address)
	#del send_socket

        #Trace.trace(16,
        #      "udp_server (reply with interface %s): to %s: request_dict %s" %
        #      (interface_ip, self.reply_address,
        #       self.request_dict[current_id],))

    # keep a copy of request to check for later udp retries of same
    # request and then send to the user
    def reply_with_list(self, list, reply_address, current_id,
                        interface_ip = None):

        list_copy = copy.deepcopy(list)
        self.request_dict[current_id] = list_copy

        if interface_ip != None:
            ip, port, send_socket = udp_common.get_callback(interface_ip)
            with_interface = " with interface %s" % interface_ip
        else:
            send_socket = self.server_socket
            with_interface = ""  #Give better trace message.
        
        try:
            Trace.trace(16, "udp_server (reply%s): to %s: request_dict %s" %
                        (with_interface, reply_address, current_id))
            send_socket.sendto(repr(list_copy), reply_address)
        except:
            Trace.handle_error()

    # for requests that are not handled serially reply_address, current_id,
    # and client_number number must be reset.  In the forking media changer
    # these are in the forked child and passed back to us
    def reply_with_address(self, ticket):
        self.reply_address = ticket["ra"][0]
        self.client_number = ticket["ra"][1]
        self.current_id    = ticket["ra"][2]
        #reply = (self.client_number, ticket, time.time())
        #Trace.trace(19,"reply_with_address %s %s %s %s"%( 
        #    self.reply_address,
        #    self.current_id,
        #    self.client_number,
        #    reply))
        self.reply_to_caller(ticket)

if __name__ == "__main__":

    #This test program can be run in conjuction with the udp_client.py
    # test program.  This test program will process any message send to
    # the correct port (including other tests than udp_client.py).
    
    udpsrv = UDPServer(('', 7700), receive_timeout = 60.0)
    while 1:
        ticket = udpsrv.do_request()
        if ticket:
            print "Message %s"%(ticket,)
            udpsrv.reply_to_caller(ticket)
            break
    del(udpsrv)
    print "finished"
