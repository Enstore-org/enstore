#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os
import sys
import string
import time
import errno
import pprint
import socket
import select
import threading

# enstore imports
import callback
import interface
import hostaddr
import generic_client
import backup_client
import udp_client
import Trace
import e_errors
import configuration_client
import enstore_constants
import enstore_functions
import log_client

MY_NAME = "MNTR_CLI"
MY_SERVER = "monitor"

SEND_TO_SERVER = "send_to_server"
SEND_FROM_SERVER = "send_from_server"

class MonitorServerClient(generic_client.GenericClient):

    def __init__( self, csc,
                  monitor_server_addr,
                  html_server_addr,
                  html_dir,
                  refresh,
                  timeout,
                  block_size,
                  block_count,
                  summary):
        self.u = udp_client.UDPClient()
        self.monitor_server_addr = monitor_server_addr
        self.html_server_addr = html_server_addr
        self.html_dir = html_dir
        self.refresh = refresh
        self.timeout = timeout
        self.block_size = block_size
        self.block_count = block_count
        self.summary = summary
        self.c_hostip, self.c_port, self.c_socket =\
                       callback.get_callback(verbose=0)
        self.c_socket.listen(4)
        
        generic_client.GenericClient.__init__(self, csc, MY_NAME)

    # send Active Monitor probe request
    def _send_probe (self, ticket):
        x = self.u.send( ticket, self.monitor_server_addr, self.timeout, 10 )
        return x

    # send measurement to the html server
    def _send_measurement (self, ticket):
        try:
            x = self.u.send( ticket, self.html_server_addr, self.timeout, 10 )
        except errno.errorcode[errno.ETIMEDOUT]:
            x = {'status' : (e_errors.TIMEDOUT, None)}
        return x

    # ping a server like ENCP would
    def monitor_one_interface(self, server_address, transfer):
        ticket= { 'work'             : 'simulate_encp_transfer',
                  'transfer'         : transfer,
                  'callback_addr'    : (self.c_hostip, self.c_port),
                  'remote_interface' : server_address[0],
                  'block_count'      : self.block_count,
                  'block_size'       : self.block_size,
                  }

        try:
            #Send the request to the server.
            send_id = self.u.send_deferred(ticket, server_address)

            #Execute the rate test with the server.
            read_measurment = self._simulate_encp_transfer(ticket)

            #Get the rate measurement back from the server.
            write_measurement = self.u.recv_deferred(send_id, timeout = 30)

            #Since the direction data is being sent influences which rate
            # we care about, pick the correct one. (The other is bogus anyway.)
            if transfer == SEND_TO_SERVER:
                reply = write_measurement
            elif transfer == SEND_FROM_SERVER:
                reply = read_measurment
            else:
                reply['status'] = ('INVALIDACTION', "failed to simulate encp")

        except errno.errorcode[errno.ETIMEDOUT]:
            reply = {}
            reply['status'] = ('ETIMEDOUT', "failed to simulate encp")
            reply['elapsed'] = self.timeout*10
            reply['block_count'] = 0

        return reply

    # ping a server like ENCP would
    def _simulate_encp_transfer(self, ticket):
        reply = {'status'     : (None, None),
                 'block_size' : ticket['block_size'],
                 'block_count': ticket['block_count']}
        
        r,w,ex = select.select([self.c_socket], [], [self.c_socket],
                               self.timeout)
        if not r :
            print "passive open did not hear back from monitor server via TCP"
            raise  errno.errorcode[errno.ETIMEDOUT]

        #simulate the control socket between encp and the mover
        ms_socket, address = self.c_socket.accept()
        if not hostaddr.allow(address):
            return reply
            
        returned_ticket = callback.read_tcp_obj(ms_socket)
        ms_socket.close()
        data_socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_socket.connect(returned_ticket['mover']['callback_addr'])

        #Now that all of the socket connections have been opened, let the
        # transfers begin.
        #Since we are recieving the data, recording the time is important.
        if ticket['transfer'] == SEND_FROM_SERVER:
            data=data_socket.recv(1)
            if not data:
                raise "Server closed connection"
            bytes_received=len(data)
            t0=time.time()
            while bytes_received<self.block_size*self.block_count:
                data = data_socket.recv(self.block_size)
                if not data: #socket is closed
                    raise "Server closed connection"
                bytes_received=bytes_received+len(data)
            reply['elapsed']=time.time()-t0
        #When sending, the time isn't important.
        elif ticket['transfer'] == SEND_TO_SERVER:
            sendstr = "S"*ticket['block_size']
            for x in xrange(ticket['block_count']):
                data_socket.send(sendstr)
            reply['elapsed'] = -1

        #If we get here, the status is ok.
        reply['status'] = ('ok', None)

        #Return the reply.
        return reply

    #Take the elapsed time and the amount of data sent/recieved and calculate
    # the rate.
    def calculate_rate(self, measurement_dict):
        if measurement_dict['status'] != ('ok', None):
            measurement = { 'elapsed':0.0, 'rate':0.0}
            return measurement
        
        block_count = measurement_dict['block_count']
        block_size = measurement_dict['block_size']
        elapsed = measurement_dict['elapsed']
        
        rate = (block_count * block_size) / elapsed / (1024*1024)
        measurement = { 'elapsed':elapsed, 'rate':rate,
                        'status':measurement_dict['status'] }
        return measurement

    #
    def send_measurement(self, read_measurement, write_measurement):
        try:
            #Try to acquire the host name from the host ip.
            callback_addr = socket.gethostbyaddr(self.c_hostip)[0]
            remote_addr = socket.gethostbyaddr(self.monitor_server_addr[0])[0]
        except:
            callback_addr = self.c_hostip
            remote_addr = self.monitor_server_addr

        #pack the info.
        ticket = {
            'work' : 'recieve_measurement',
            'refresh' : self.refresh,
            'measurement': (
            enstore_functions.format_time(time.time()),
            enstore_functions.strip_node(callback_addr),
            enstore_functions.strip_node(remote_addr),
            "%.4g" % (read_measurement['rate'],),
            "%.4g" % (write_measurement['rate'],)
            )}

        #Send the information to the web server node.
        self._send_measurement(ticket)
            
    def flush_measurements(self):
        reply = self._send_measurement (
            {
            'work'   : 'flush_measurements',
            'dir'    : self.html_dir,
            'refresh': self.refresh
            }
            )

    #I don't know what a lot of this stuff does, but it does print out
    # the rates when succesfull.
    def update_summary(self, hostname, summary_d, summary,
                       read_rate, write_rate):
        if read_rate['status'] == ('ok', None) and \
           write_rate['status'] == ('ok', None):
            print "  Success."
            print "Network rate measured at %.4g MB/S recieving " \
                  "and %.4g MB/S sending." % \
                  (read_rate['rate'], write_rate['rate'])
            
            summary_d[hostname] = enstore_constants.UP
            if read_rate == 0.0 or write_rate == 0.0:
                summary_d[hostname] = enstore_constants.WARNING
                summary_d[enstore_constants.NETWORK] = enstore_constants.WARNING
        else:
            print "  Error.    Status is (%s,%s)"%(read_rate['status'],
                                                   write_rate['status'])
            summary_d[hostname] = enstore_constants.WARNING
            summary_d[enstore_constants.NETWORK] = enstore_constants.WARNING

class MonitorServerClientInterface(generic_client.GenericClientInterface):

    def __init__(self, flag=1, opts=[]):
        self.do_parse = flag
        self.restricted_opts = opts
        self.summary = 0
        self.html_gen_host = None
        self.name = MY_SERVER
        self.alive_rcv_timeout = 10
        self.alive_retries = 3
        generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.help_options() +  self.alive_options() +\
                   ["summary", "html-gen-host="]

def get_all_ips(config_host, config_port, csc):
    """
    inquire the configuration server, return a list
    of every  IP address involved in Enstore  
    """

    ## What if we cannot get to config server
    x = csc.u.send({"work":"reply_serverlist"},
                   (config_host, config_port))
    if x['status'][0] != 'ok': raise "error from config server"
    server_dict = x['server_list']
    ip_dict = {}
    for k in server_dict.keys():
        #assume we can get to config server since we could above
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



class Vetos:
    """
    A small class to manage the veto dictionary that is provided
    by the configuration server. The administator does not
    want us to probe these nodes.
    """
    def __init__(self, vetos):
        # vetos is a list in confg file format
        # this is a dictionary with keys as
        # possibly (non canonical) IP addresses. Ip or DNS names
        # and the value field being a reason why it is in the veto list

        # don't send to yourself
        vetos[socket.gethostname()] = 'thishost'

        self.veto_item_dict = {}
        for v in vetos.keys():
            try: #If DNS fails, move to the next
                canon = self._canonicalize(v)
                self.veto_item_dict[canon] = (v, vetos[v])
            except socket.error:
                continue

    def is_vetoed_item(self, ip):
        ip_as_canon = self._canonicalize(ip)
        return self.veto_item_dict.has_key(ip_as_canon)

    def veto_info(self, ip):
        ip_as_canon = self._canonicalize(ip)
        #return (ip_text, reason_text)
        return self.veto_item_dict[ip_as_canon]

    def _canonicalize(self, some_ip) :
        return socket.gethostbyname(some_ip)
    
        
# this is called by the enstore saag interface
def do_real_work(summary, config_host, config_port, html_gen_host):
    csc = configuration_client.ConfigurationClient((config_host, config_port))
    config = csc.get('monitor')
    if config['status'] == (e_errors.OK, None):
        logc=log_client.LoggerClient(csc, MY_NAME, 'log_server')

        ip_list = get_all_ips(config_host, config_port, csc)

        host_list = []
        for ip in ip_list:
            host_list.append((socket.gethostbyaddr(ip)[0], ip))
        host_list.sort()
        
        vetos = Vetos(config.get('veto_nodes', {}))


        if html_gen_host:
            config['html_gen_host'] = html_gen_host

        summary_d = {enstore_constants.TIME: enstore_functions.format_time(time.time())}
        summary_d[enstore_constants.BASENODE] = enstore_functions.strip_node(os.uname()[1])
        summary_d[enstore_constants.NETWORK] = enstore_constants.UP  # assumption

        msc = None
        for host, ip in host_list:
            hostname = enstore_functions.strip_node(host)
            if vetos.is_vetoed_item(ip):
                if not summary:
                    print "Skipping %s" % (vetos.veto_info(ip),)
                continue
            if not summary:
                print "Trying", host, 
            msc = MonitorServerClient(
                (config_host, config_port),
                (ip,                      enstore_constants.MONITOR_PORT),
                (config['html_gen_host'], enstore_constants.MONITOR_PORT),
                config['html_dir'],
                config['refresh'],
                config['default_timeout'],
                config['block_size'],
                config['block_count'],
                summary
                )

            #Test rate sending from the server.  The rate info for read time
            # information is stored in msc.measurement.
            read_measurement = msc.monitor_one_interface(
                (ip, enstore_constants.MONITOR_PORT), SEND_FROM_SERVER)
            if read_measurement['status'] == ('ok', None):
                read_rate = msc.calculate_rate(read_measurement)
            else:
                read_rate = {'elapsed':0, 'rate':0,
                             'status':('READ_ERROR', None)}
        
            #Test rate sending to the server.  Since, the time is recorded on
            # the other end use the value returned, and not the one stored
            # in msc.measurement (which is for read info).
            write_measurement = msc.monitor_one_interface(
                (ip, enstore_constants.MONITOR_PORT), SEND_TO_SERVER)
            if write_measurement['status'] == ('ok', None):
                write_rate = msc.calculate_rate(write_measurement)
            else:
                write_rate = {'elapsed':0, 'rate':0,
                             'status':('WRITE_ERROR', None)}

            #Send the information to the html server node.
            msc.send_measurement(read_rate, write_rate)

            #Does some summary stuff.
            msc.update_summary(hostname, summary_d, summary, read_rate,
                               write_rate)
            
        if msc:
            msc.flush_measurements()
            msc.c_socket.close()
            
        # add the name of the html file that will be created
        summary_d[enstore_constants.URL] = "%s"%(enstore_constants.NETWORKFILE,)
    else:
        # there was nothing about the monitor server in the config file
        summary_d = {}

    return summary_d

# we need this in order to be called by the enstore.py code
def do_work(intf):
    #First create an instance that can handle generic commands.
    msc = MonitorServerClient((intf.config_host, intf.config_port),
                              None, None, None, None, None, None, None, None)
    #If there are no generic commands, then do real work.
    if not msc.handle_generic_commands(intf.name, intf):
        do_real_work(intf.summary, intf.config_host, intf.config_port,
                     intf.html_gen_host)

if __name__ == "__main__":
    

    intf = MonitorServerClientInterface()
    
    Trace.init(MY_NAME)
    Trace.trace( 6, 'msc called with args: %s'%(sys.argv,) )

    do_work(intf)
