#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import os
import sys
import socket
import select
import string
import time
import threading

import dispatching_worker
import generic_server
#import configuration_client
import timeofday
#import udp_client
#import enstore_functions2
import enstore_constants
#import monitored_server
#import event_relay_client
import event_relay_messages
import Trace
import e_errors


MY_NAME = enstore_constants.RATEKEEPER    #"ratekeeper"

rate_lock = threading.Lock()

def endswith(s1,s2):
    return s1[-len(s2):] == s2

#def atol(s):
#    if s[-1] == 'L':
#        s = s[:-1] #chop off any trailing "L"
#    return string.atol(s)

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
    def __init__(self, csc):
        generic_server.GenericServer.__init__(self, csc, MY_NAME,
                                              #function = self.handle_er_msg
                                              )
        Trace.init(self.log_name)

        #Get the configuration from the configuration server.
        ratekeep = self.csc.get('ratekeeper', timeout=15, retry=3)
        
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

        dispatching_worker.DispatchingWorker.__init__(self, ratekeeper_addr)

        self.filename_base = socket.gethostname()  #filename_base
        self.output_dir = ratekeeper_dir
        self.outfile = None
        self.ymd = None #Year, month, date
        self.last_ymd = None
        self.subscribe_time = 0
        self.mover_msg = {} #key is mover, value is last (num, denom)

        #The generic server __init__ function creates self.erc, but it
        # needs some additional paramaters.
        self.event_relay_subscribe([event_relay_messages.TRANSFER,
                                    event_relay_messages.NEWCONFIGFILE])

        #The event relay client start() function sets up the erc socket to be
        # monitored by the dispatching worker layer.  We do not want this
        # in the ratekeeper.  It monitors this socket on its own, so it
        # must be removed from the list.
        self.remove_select_fd(self.erc.sock)

    def reinit(self):
        rate_lock.acquire()

        # stop the communications with the event relay task
        self.event_relay_unsubscribe()

        ###We shouldn't need to stop the rk_main thread here.  It will
        ### pick up any relavent configuration changes every 15 seconds.
        
        self.__init__(self.csc)

        rate_lock.release()
    
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
        num = long(words[2])  #NB -bytes = read;  +bytes=write
        writing = num>0
        num = abs(num)
        denom = long(words[3])

        #Get the last pair of numbers for each mover.
        rate_lock.acquire()
        prev = self.mover_msg.get(mover)
        self.mover_msg[mover] = (num, denom)
        rate_lock.release()

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

            rate_lock.acquire()
            self.check_outfile(now)
            if now - self.subscribe_time > self.resubscribe_interval:
                self.subscribe()
                self.subscribe_time = now
            rate_lock.release()
                
            end_time = self.start_time + N * self.interval
            remaining = end_time - now
            if remaining <= 0:
                try:
                    rate_lock.acquire()
                    self.outfile.write( "%s %d %d %d %d\n" %
                                        (time.strftime("%m-%d-%Y %H:%M:%S",
                                                       time.localtime(now)),
                                         bytes_read_dict.get("REAL", 0),
                                         bytes_written_dict.get("REAL", 0),
                                         bytes_read_dict.get("NULL", 0),
                                         bytes_written_dict.get("NULL", 0),))
                    self.outfile.flush()
                    rate_lock.release()
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

            rate_lock.acquire()
            r, w, x = select.select([self.erc.sock], [], [], remaining)
            rate_lock.release()

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

            if words[0] == event_relay_messages.NEWCONFIGFILE:
                #Hack to get what we want from GenericServer.
                self._reinit2()
                continue
            elif words[0] == event_relay_messages.TRANSFER:
                #If the split strings don't contain the fields we are
                # looking for then ignore them.
                if len(words) < 5: #Don't crash if an old mover is sending.
                    continue
                if words[4] != 'network':
                    #Only transfer messages with network information is used.
                    # Ingore all others and continue onto the next message.
                    continue
            else:
                #Impossible, we don't ask for other types of messages.
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

    rk = Ratekeeper((intf.config_host, intf.config_port))
    
    reply = rk.handle_generic_commands(intf)

    rk_main_thread = threading.Thread(target=rk.main)
    rk_main_thread.start()

    while 1:
        try:
            Trace.log(e_errors.INFO, "Ratekeeper (re)starting")
            rk.serve_forever()
        except SystemExit, exit_code:
            sys.exit(exit_code)
        except:
            Trace.handle_error()
            rk.serve_forever_error("ratekeeper")
            continue

    Trace.log(e_errors.ERROR,"Ratekeeper finished (impossible)")

