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
import configuration_client
import timeofday
import udp_client
import enstore_functions2
import enstore_constants
import monitored_server
import event_relay_client
import event_relay_messages


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
    def __init__(self):
        #Get the configuration from the configuration server.
        csc = configuration_client.ConfigurationClient()
        ratekeep = csc.get('ratekeeper', timeout=15, retry=3)
        
        ratekeeper_dir  = ratekeep.get('dir', 'MISSING')
        ratekeeper_host = ratekeep.get('hostip',ratekeep.get('host','MISSING'))
        ratekeeper_port = ratekeep.get('port','MISSING')
        ratekeeper_nodes = ratekeep.get('nodes','MISSING') #Command line info.
        ratekeeper_addr = (ratekeeper_host, ratekeeper_port)
        
        if ratekeeper_dir  == 'MISSING' or not ratekeeper_dir:
            print "Error: Missing ratekeeper configdict directory.",
            print "  (ratekeeper_dir)"
            sys.exit(1)
        if ratekeeper_host == 'MISSING' or not ratekeeper_host:
            print "Error: Missing ratekeeper configdict directory.",
            print "  (ratekeeper_host)"
            sys.exit(1)
        if ratekeeper_port == 'MISSING' or not ratekeeper_port:
            print "Error: Missing ratekeeper configdict directory.",
            print "  (ratekeeper_port)"
            sys.exit(1)
        if ratekeeper_nodes == 'MISSING':
            ratekeeper_nodes = ''

        self.filename_base = socket.gethostname()  #filename_base
        self.output_dir = ratekeeper_dir
        self.outfile = None
        self.ymd = None #Year, month, date
        self.last_ymd = None
        self.subscribe_time = 0
        self.mover_msg = {} #key is mover, value is last (num, denom)

        generic_server.GenericServer.__init__(self, csc, MY_NAME)
        dispatching_worker.DispatchingWorker.__init__(self, ratekeeper_addr)

        self.alive_interval = monitored_server.get_alive_interval(self.csc,
                                                                  MY_NAME)

        #The generic server __init__ function creates self.erc, but it
        # needs some additional paramaters.
        self.erc.start([event_relay_messages.ALL,])
        self.erc.subscribe()                                         
        self.erc.start_heartbeat(enstore_constants.RATEKEEPER, 
                                 self.alive_interval)
        
    def subscribe(self):
        self.erc.subscribe()

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
            self.outfile=open(outfile_name, 'a')

    def count_bytes(self, words, bytes_read_dict, bytes_written_dict, group):
        mover = words[1]
        mover = string.upper(mover)
        

        #Get the number of bytes moved (words[2]) and total bytes ([3]).
        num = atol(words[2])  #NB -bytes = read;  +bytes=write
        writing = num>0
        num = abs(num)
        denom = atol(words[3])

        #Get the last pair of numbers for each mover.
        prev = self.mover_msg.get(mover)
        self.mover_msg[mover] = (num, denom)

        #When a new file is started, the first transfer occurs, a mover
        # quits, et al, then initialize these parameters.
        if not prev:
            num_0 = denom_0 = 0
        else:
            num_0, denom_0 = prev
        if num_0 >= denom or denom_0 != denom:
            num_0 = denom_0 = 0

        #Caluclate the number of bytes transfered at this time.
        bytes = num - num_0
        if writing:
            bytes_written_dict[group] = bytes_written_dict.get(group,0L)+bytes
        else:
            bytes_read_dict[group] = bytes_read_dict.get(group,0L) + bytes

        #If the file is known to be transfered, reset these to zero.
        if num == denom:
            num_0 = denom_0 = 0

    
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
        bytes_read_dict = {} # = 0L
        bytes_written_dict = {} #0L
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
                    self.outfile.write( "%s %d %d %d %d\n" %
                                        (time.strftime("%m-%d-%Y %H:%M:%S",
                                                       time.localtime(now)),
                                         bytes_read_dict.get("REAL", 0),
                                         bytes_written_dict.get("REAL", 0),
                                         bytes_read_dict.get("NULL", 0),
                                         bytes_written_dict.get("NULL", 0),))
                    self.outfile.flush()
                except:
                    sys.stderr.write("Can't write to output file\n")

                for key in bytes_read_dict.keys():
                    bytes_read_dict[key] = 0L
                    bytes_written_dict[key] = 0L
                
                N = N + 1
                end_time = self.start_time + N * self.interval
                remaining = end_time - now

            if remaining <= 0: #Possible from ntp clock update???
                continue

            r, w, x = select.select([self.erc.sock], [], [], remaining)

            if not r:
                continue

            r=r[0]

            try:
                cmd = r.recv(1024)
            except:
                cmd = None

            #Take the command and seperate the string spliting on whitespace.
            if not cmd:
                continue
            cmd = string.strip(cmd)
            words = string.split(cmd)
            if not words:
                continue

            #If the split strings don't contain the fields we are looking for
            # then ignore them.
            if len(words) < 5: #Don't crash if an old mover is sending.
                continue
            if words[0] != 'transfer' or words[4] != 'network':
                continue

            if string.find(string.upper(words[1]), 'NULL') >= 0:
                self.count_bytes(words, bytes_read_dict,
                                 bytes_written_dict,"NULL")
            else:
                self.count_bytes(words, bytes_read_dict,
                                 bytes_written_dict,"REAL")
            
                
class RatekeeperInterface(generic_server.GenericServerInterface):

    def __init__(self):
        self.host = None
        generic_server.GenericServerInterface.__init__(self)

    def valid_dictionaries(self):
        return (self.help_options,)


if __name__ == "__main__":
    intf = RatekeeperInterface()

    rk = Ratekeeper()
    
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


