#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import string
import time
import errno
import pprint
import socket

# enstore imports
import callback
import interface
import generic_client
import backup_client
import udp_client
import Trace
import e_errors
import configuration_client
import log_client

MY_NAME = "MNTR_CLI"
MY_SERVER = "monitor_server"

class MonitorServerClient:

    def __init__( self, servr_addr ):
        self.u = udp_client.UDPClient()
	self.servr_addr = servr_addr
        Trace.trace(10,'__init__ u='+str(self.u))

    # send the request to the monitor server
    def send (self, ticket,  rcv_timeout=0, tries=0):
        x = self.u.send( ticket, self.servr_addr, rcv_timeout, tries )
        return x

    # ping a server like ENCP would
    def simulate_encp_transfer(self, remote_interface,
                               block_size=65536, block_count=160 ):

        c_hostip, c_port, c_socket = callback.get_callback(
            use_multiple=0,verbose=0)
        
        ticket= { 'work'           : 'simulate_encp_transfer',
                  'callback_addr' :    (c_hostip, c_port),
                  'remote_interface' : remote_interface,
                  'block_count' : block_count,
                  'block_size' : block_size
                  }
        reply=self.send(ticket)
        #simulate the control socket between encp and the mover
        ms_socket, address = c_socket.accept()
        c_socket.close()
        returned_ticket = callback.read_tcp_obj(ms_socket)
        ms_socket.close()
        data_socket=callback.mover_callback_socket(returned_ticket)
        data=data_socket.recv(1)
        if not data:
            raise "Server closed connection"
        bytes_received=len(data)
        t0=time.time()
        while bytes_received<block_size*block_count:
            data = data_socket.recv(block_size)
            if not data: #socket is closed
                raise "Server closed connection"
            bytes_received=bytes_received+len(data)
        ticket['elapsed']=time.time()-t0
        return ticket
    
    def send_measurement(self, measurement_dict):
        block_count = measurement_dict['block_count']
        block_size = measurement_dict['block_size']
        elapsed = measurement_dict['elapsed']
        callback_addr = measurement_dict['callback_addr'][0]
        #be paranoid abou the dress translation. not spending the time
        #to research it.
        try:
            callback_addr = socket.gethostbyaddr(callback_addr)[0]
        except:
            pass
        
        reply = self.send (
            {
            'work' : 'recieve_measurement',
            
            'measurement' : (
            time.asctime(time.localtime(time.time())), 
            callback_addr,
            measurement_dict['remote_interface'],
            block_count,
            block_size,
            elapsed,
            (block_count * block_size) / elapsed / 1000000
            )
            }
            )

    def flush_measurements(self):
        reply = self.send (
            {
            'work' : 'flush_measurements'
            }
            )
    
class MonitorServerClientInterface(generic_client.GenericClientInterface):

    def __init__(self, flag=1, opts=[]):
        generic_client.GenericClientInterface.__init__(self)



def get_all_ips():
    """
    inquire the configuration server, return a list
    of every  IP address involved in Enstore  
    """
    
    x = csc.u.send({"work":"reply_serverlist"},
                   (intf.config_host, intf.config_port))
    if x['status'][0] != 'ok': raise "error from config server"
    server_dict = x['server_list']
    ip_dict = {}
    for k in server_dict.keys():
        
        details = csc.get(k)
        if details.has_key('data_ip'):
            ip = details['data_ip']     #check first if a mover
        elif details.has_key('hostip'):
            ip = details['hostip']      #other server
        else:
            continue
        ip = socket.gethostbyname(ip)  #canonicalize int dot notation
        ip_dict[ip] = 1                #dictionary will strike duplicates
    return ip_dict.keys()              #keys of the dict is a tuple of IPs

csc=None

if __name__ == "__main__":
    global csc

    intf = MonitorServerClientInterface()
    
    Trace.init(MY_NAME)
    Trace.trace( 6, 'msc called with args: %s'%(sys.argv,) )

    csc = configuration_client.ConfigurationClient((intf.config_host,
                                                    intf.config_port))
    

    logc=log_client.LoggerClient(csc, MY_NAME, 'log_server')

    
    ip_list = get_all_ips()

    ##temp cmd line processing - should go through "interface"
    if len(sys.argv)>1:
        ip_list = sys.argv[1:]
    msport = 9999
    for ip in ip_list:
        print "trying", ip
        msc = MonitorServerClient((ip, msport))
        measurement=msc.simulate_encp_transfer(ip)
        pprint.pprint(measurement)
        msc.send_measurement(measurement)
    msc.flush_measurements()
        
    
