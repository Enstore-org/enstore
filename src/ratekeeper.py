#!/usr/bin/env python

# $Id$

import os
import sys
import socket
import select
import string
import time
import threading

import dispatching_worker
import generic_server
import interface
import configuration_client
import timeofday
import udp_client
import enstore_functions


MY_NAME = "Ratekeeper"

def endswith(s1,s2):
    return s1[-len(s2):] == s2

def atol(s):
    if s[-1] == 'L':
        s = s[:-1] #chop off any trailing "L"
    return string.atol(s)

def next_minute(t=None):
    if t is None:
        t = time.time()
    Y, M, D, h, m, s, wd, jd, dst = time.localtime(t)
    m = (m+1)%60
    if m==0:
        h=(h+1)%24
        if h==0:
            D=D+1
            wd=wd+1
            jd=jd+1
            ##I'm not going to worry about end-of-month.  Sue me!
    t = time.mktime((Y, M, D, h, m, 0, wd, jd, dst))
    return t
        

class Ratekeeper(dispatching_worker.DispatchingWorker,
                 generic_server.GenericServer):
    interval = 15
    resubscribe_interval = 10*60 
    def __init__(self, event_relay_addr, filename_base, output_dir='/tmp/RATES'):
        self.event_relay_addr = event_relay_addr
        self.filename_base = filename_base
        self.output_dir = output_dir
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.outfile = None
        self.ymd = None #Year, month, date
        self.last_ymd = None
        hostname = os.uname()[1]
        self.sock.bind((hostname, 0))
        self.addr = self.sock.getsockname()
        self.subscribe_time = 0
        self.mover_msg = {} #key is mover, value is last (num, denom)

        dispatching_worker.DispatchingWorker.__init__(self, event_relay_addr)
        
    def subscribe(self):
        self.sock.sendto("notify %s %s" % (self.addr),
                         (self.event_relay_addr))

    def check_outfile(self, now=None):
        if now is None:
            now = time.time()
        tup = time.localtime(now)
        self.ymd = tup[:3]
        if self.ymd != self.last_ymd:
            self.last_ymd = self.ymd
            if self.outfile:
                try:
                    self.outfile.close()
                except:
                    sys.stderr.write("Can't open file\n")

            year, month, day = self.ymd
            outfile_name = os.path.join(self.output_dir, \
                                        "%s.RATES.%04d%02d%02d" %
                                        (self.filename_base, year, month, day))
            self.outfile=open(outfile_name, 'w')

    
    def main(self):
        now = time.time()
        self.start_time = next_minute(now)
        wait = self.start_time - now
        #sys.stderr.write("waiting %.2f seconds\n" % wait)
        print "waiting %.2f seconds" % (wait,)
        time.sleep(wait)
        #sys.stderr.write("starting\n")
        print "starting"
        N = 1L
        bytes_read = 0L
        bytes_written = 0L
        while 1:
            now = time.time()
            self.check_outfile(now)
            if now - self.subscribe_time > self.resubscribe_interval:
                self.subscribe()
                self.subscribe_time = now
                
            end_time = self.start_time + N * self.interval
            remaining = end_time - now
            if remaining <= 0:
                try:
                    self.outfile.write( "%s %d %d\n" %
                                        (time.strftime("%m-%d-%Y %H:%M:%S",
                                                       time.localtime(now)),
                                         bytes_read, bytes_written))
                    self.outfile.flush()
                except:
                    sys.stderr.write("Can't write to output file\n")
                    
                bytes_read = 0L
                bytes_written = 0L
                N = N + 1
                end_time = self.start_time + N * self.interval
                remaining = end_time - now

            r, w, x = select.select([self.sock], [], [], remaining)
            
            if not r:
                continue

            r=r[0]

            try:
                cmd = r.recv(1024)
            except:
                cmd = None
                
            if not cmd:
                continue
            cmd = string.strip(cmd)
            words = string.split(cmd)
            if not words:
                continue
            if words[0] != 'transfer':
                continue
            mover = words[1]
            mover = string.upper(mover)
            if string.find(mover, 'NULL')>=0:
                continue

            num = atol(words[2])  #NB -bytes = read;  +bytes=write
            writing = num>0
            num = abs(num)
            denom = atol(words[3])
            
            prev = self.mover_msg.get(mover)
            self.mover_msg[mover] = (num, denom)
            
            if not prev:
                continue

            num_0, denom_0 = prev
            if num < num_0 or denom != denom_0:
                #consider this the beginning of a new transfer
                continue
            bytes = num - num_0
            if writing:
                bytes_written = bytes_written + bytes
            else:
                bytes_read = bytes_read + bytes


class RatekeeperInterface(generic_server.GenericServerInterface):

    def __init__(self):
        self.host = None
        generic_server.GenericServerInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return generic_server.GenericServerInterface.options(self)+\
               ["host="]



if __name__ == "__main__":
    intf = RatekeeperInterface()
    
    #Get the configuration from the configuration server.
    csc = configuration_client.ConfigurationClient()
    ratekeep = csc.get('ratekeeper', timeout=15, retry=3)
    
    ratekeeper_dir  = ratekeep.get('dir', 'MISSING')
    ratekeeper_host = ratekeep.get('host','MISSING') #Info about local node.
    ratekeeper_port = ratekeep.get('port','MISSING')
    ratekeeper_name = ratekeep.get('name','MISSING') #info about remote node.
    ratekeeper_node = ratekeep.get('name','MISSING')
    ratekeeper_nodes = ratekeep.get('nodes','MISSING')
    
    if ratekeeper_dir  == 'MISSING' or not ratekeeper_dir:
        print "Error: Missing ratekeeper configdict directory.",
        print "  (ratekeeper_dir)"
        sys.exit(1)
    if ratekeeper_port == 'MISSING' or not ratekeeper_port:
        print "Error: Missing ratekeeper configdict directory.",
        print "  (ratekeeper_port)"
        sys.exit(1)
    if ratekeeper_node == 'MISSING':
        ratekeeper_node = ''
    if ratekeeper_name == 'MISSING':
        ratekeeper_name = ratekeeper_node
    if ratekeeper_nodes == 'MISSING':
        ratekeeper_nodes = ''


    #If an option is specified, then use it.  But first check to see if it is
    # listed in the 'nodes' dictionary.  Think of it as the key is the shortest
    # number of characters needed for a match and the value is a tuple
    # containing the full hostname and basename.
    #Example: ratekeeper.py d0
    # Here "d0" is placed into the host and file names.  Then it is compared
    # with the values in the 'nodes' dictionary.  When a match is found, then
    # "d0ensrv2" becomes the host and "d0en" becomes the base name.  If there
    # is not a match, then it is left as simply "d0".
    if intf.host:
        event_relay_host = intf.host
        filename_base = event_relay_host

        if ratekeeper_nodes != 'MISSING':
            for short_name in ratekeeper_nodes.keys():
                if event_relay_host[:len(short_name)] == short_name:
                    event_relay_host = ratekeeper_nodes[short_name][0]
                    filename_base = ratekeeper_nodes[short_name][1]
                    break
    #Parse the command line.
    #If no options specified, obtain the defaults.
    else:
        #Default values from the config dictionary.
        if ratekeeper_node:
            event_relay_host = ratekeeper_node
            filename_base = ratekeeper_name
        #If the configdict doesn't have a 'node' and 'name', use 'host'.
        elif ratekeeper_host:
            event_relay_host = ratekeeper_host
            filename_base = enstore_functions.strip_node(event_relay_host)
        #If the configdict doesn't have anything, check the env. variable
        else:
            event_relay_host = os.environ.get("ENSTORE_CONFIG_HOST")
            filename_base = enstore_functions.strip_node(event_relay_host)
            

    print "Connecting to host %s on port %s." % \
          (event_relay_host, ratekeeper_port)
    rk = Ratekeeper((event_relay_host, ratekeeper_port), filename_base,
                    ratekeeper_dir)

    reply = rk.handle_generic_commands(intf)

    rk_main_thread = threading.Thread(target=rk.main)
    rk_main_thread.start()

    while 1:
        try:
            rk.serve_forever()
        except SystemExit, exit_code:
            sys.exit(exit_code)
        except:
            exc, msg, tb = sys.exc_info()
            format = "%s %s %s %s %s: serve_forever continuing" % \
                     (timeofday.tod(),sys.argv,exc,msg,MY_NAME)
            continue


